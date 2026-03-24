<?php

namespace Utopia\Query\AST;

use Utopia\Query\Exception;
use Utopia\Query\Tokenizer\Token;
use Utopia\Query\Tokenizer\TokenType;

class Parser
{
    /** @var Token[] */
    private array $tokens;
    private int $pos;
    private bool $inColumnList = false;

    /**
     * Parse tokens into a SelectStatement.
     * @param Token[] $tokens filtered tokens (no whitespace/comments), must end with Eof
     */
    public function parse(array $tokens): SelectStatement
    {
        $this->tokens = $tokens;
        $this->pos = 0;
        $this->inColumnList = false;

        return $this->parseSelect();
    }

    private function parseSelect(): SelectStatement
    {
        $ctes = [];
        $recursive = false;

        if ($this->matchKeyword('WITH')) {
            $this->advance();
            if ($this->matchKeyword('RECURSIVE')) {
                $recursive = true;
                $this->advance();
            }
            $ctes = $this->parseCteList($recursive);
        }

        $this->consumeKeyword('SELECT');

        $distinct = false;
        if ($this->matchKeyword('DISTINCT')) {
            $distinct = true;
            $this->advance();
        }

        $columns = $this->parseColumnList();

        $from = null;
        $joins = [];
        $where = null;
        $groupBy = [];
        $having = null;
        $windows = [];
        $orderBy = [];
        $limit = null;
        $offset = null;

        if ($this->matchKeyword('FROM')) {
            $this->advance();
            $from = $this->parseTableSource();
            $joins = $this->parseJoins();
        }

        if ($this->matchKeyword('WHERE')) {
            $this->advance();
            $where = $this->parseExpression();
        }

        if ($this->matchKeyword('GROUP')) {
            $this->advance();
            $this->consumeKeyword('BY');
            $groupBy = $this->parseExpressionList();
        }

        if ($this->matchKeyword('HAVING')) {
            $this->advance();
            $having = $this->parseExpression();
        }

        if ($this->matchKeyword('WINDOW')) {
            $this->advance();
            $windows = $this->parseWindowDefinitions();
        }

        if ($this->matchKeyword('ORDER')) {
            $this->advance();
            $this->consumeKeyword('BY');
            $orderBy = $this->parseOrderByList();
        }

        if ($this->matchKeyword('LIMIT')) {
            $this->advance();
            $limit = $this->parseExpression();
        }

        if ($this->matchKeyword('OFFSET')) {
            $this->advance();
            $offset = $this->parseExpression();
        }

        if ($this->matchKeyword('FETCH')) {
            $this->advance();
            $this->consumeKeyword('FIRST');
            $limit = $this->parseExpression();
            $this->consumeKeyword('ROWS');
            $this->expectIdentifierValue('ONLY');
        }

        return new SelectStatement(
            columns: $columns,
            from: $from,
            joins: $joins,
            where: $where,
            groupBy: $groupBy,
            having: $having,
            orderBy: $orderBy,
            limit: $limit,
            offset: $offset,
            distinct: $distinct,
            ctes: $ctes,
            windows: $windows,
        );
    }

    /**
     * @return CteDefinition[]
     */
    private function parseCteList(bool $recursive): array
    {
        $ctes = [];
        do {
            $ctes[] = $this->parseCteDefinition($recursive);
        } while ($this->matchAndConsume(TokenType::Comma));

        return $ctes;
    }

    private function parseCteDefinition(bool $recursive): CteDefinition
    {
        $name = $this->expectIdentifier();
        $columns = [];

        if ($this->current()->type === TokenType::LeftParen && !$this->matchKeyword('AS')) {
            if ($this->peekIsColumnList()) {
                $this->expect(TokenType::LeftParen);
                $columns[] = $this->expectIdentifier();
                while ($this->matchAndConsume(TokenType::Comma)) {
                    $columns[] = $this->expectIdentifier();
                }
                $this->expect(TokenType::RightParen);
            }
        }

        $this->consumeKeyword('AS');
        $this->expect(TokenType::LeftParen);
        $query = $this->parseSelect();
        $this->expect(TokenType::RightParen);

        return new CteDefinition($name, $query, $columns, $recursive);
    }

    private function peekIsColumnList(): bool
    {
        $depth = 0;

        for ($i = $this->pos; $i < count($this->tokens); $i++) {
            $t = $this->tokens[$i];
            if ($t->type === TokenType::LeftParen) {
                $depth++;
            } elseif ($t->type === TokenType::RightParen) {
                $depth--;
                if ($depth === 0) {
                    $next = $i + 1 < count($this->tokens) ? $this->tokens[$i + 1] : null;
                    return $next !== null
                        && $next->type === TokenType::Keyword
                        && strtoupper($next->value) === 'AS';
                }
            }
        }

        return false;
    }

    /**
     * @return Expr[]
     */
    private function parseColumnList(): array
    {
        $this->inColumnList = true;
        $columns = [];
        $columns[] = $this->parseSelectColumn();

        while ($this->matchAndConsume(TokenType::Comma)) {
            $columns[] = $this->parseSelectColumn();
        }

        $this->inColumnList = false;
        return $columns;
    }

    private function parseSelectColumn(): Expr
    {
        $expr = $this->parseExpression();

        if ($this->matchKeyword('AS')) {
            $this->advance();
            $alias = $this->expectIdentifier();
            return new AliasedExpr($expr, $alias);
        }

        if ($this->inColumnList && $this->isImplicitAlias()) {
            $alias = $this->expectIdentifier();
            return new AliasedExpr($expr, $alias);
        }

        return $expr;
    }

    private function isImplicitAlias(): bool
    {
        $token = $this->current();
        if ($token->type === TokenType::Identifier) {
            return true;
        }
        if ($token->type === TokenType::QuotedIdentifier) {
            return true;
        }
        return false;
    }

    private function parseExpression(): Expr
    {
        return $this->parseOr();
    }

    private function parseOr(): Expr
    {
        $left = $this->parseAnd();

        while ($this->matchKeyword('OR')) {
            $this->advance();
            $right = $this->parseAnd();
            $left = new BinaryExpr($left, 'OR', $right);
        }

        return $left;
    }

    private function parseAnd(): Expr
    {
        $left = $this->parseNot();

        while ($this->matchKeyword('AND')) {
            $this->advance();
            $right = $this->parseNot();
            $left = new BinaryExpr($left, 'AND', $right);
        }

        return $left;
    }

    private function parseNot(): Expr
    {
        if ($this->matchKeyword('NOT')) {
            if ($this->peekKeyword(1, 'EXISTS')) {
                $this->advance(); // consume NOT
                $this->advance(); // consume EXISTS
                $this->expect(TokenType::LeftParen);
                $subquery = $this->parseSelect();
                $this->expect(TokenType::RightParen);
                return new ExistsExpr($subquery, true);
            }

            $this->advance();
            $operand = $this->parseNot();
            return new UnaryExpr('NOT', $operand);
        }

        return $this->parseComparison();
    }

    private function parseComparison(): Expr
    {
        $left = $this->parseAddition();

        $left = $this->parsePostfixModifiers($left);

        return $left;
    }

    private function parsePostfixModifiers(Expr $left): Expr
    {
        // IS [NOT] NULL
        if ($this->matchKeyword('IS')) {
            $this->advance();
            if ($this->matchKeyword('NOT')) {
                $this->advance();
                $this->expectNull();
                return new UnaryExpr('IS NOT NULL', $left, false);
            }
            $this->expectNull();
            return new UnaryExpr('IS NULL', $left, false);
        }

        // NOT IN / NOT BETWEEN / NOT LIKE / NOT ILIKE
        if ($this->matchKeyword('NOT')) {
            if ($this->peekKeyword(1, 'IN')) {
                $this->advance(); // NOT
                $this->advance(); // IN
                return $this->parseInList($left, true);
            }
            if ($this->peekKeyword(1, 'BETWEEN')) {
                $this->advance(); // NOT
                $this->advance(); // BETWEEN
                return $this->parseBetween($left, true);
            }
            if ($this->peekKeyword(1, 'LIKE')) {
                $this->advance(); // NOT
                $this->advance(); // LIKE
                $right = $this->parseAddition();
                return new BinaryExpr($left, 'NOT LIKE', $right);
            }
            if ($this->peekKeyword(1, 'ILIKE')) {
                $this->advance(); // NOT
                $this->advance(); // ILIKE
                $right = $this->parseAddition();
                return new BinaryExpr($left, 'NOT ILIKE', $right);
            }
        }

        // IN
        if ($this->matchKeyword('IN')) {
            $this->advance();
            return $this->parseInList($left, false);
        }

        // BETWEEN
        if ($this->matchKeyword('BETWEEN')) {
            $this->advance();
            return $this->parseBetween($left, false);
        }

        // LIKE / ILIKE
        if ($this->matchKeyword('LIKE')) {
            $this->advance();
            $right = $this->parseAddition();
            return new BinaryExpr($left, 'LIKE', $right);
        }
        if ($this->matchKeyword('ILIKE')) {
            $this->advance();
            $right = $this->parseAddition();
            return new BinaryExpr($left, 'ILIKE', $right);
        }

        // Comparison operators: =, !=, <>, <, >, <=, >=
        if ($this->current()->type === TokenType::Operator) {
            $op = $this->current()->value;
            if (in_array($op, ['=', '!=', '<>', '<', '>', '<=', '>='], true)) {
                $this->advance();
                $right = $this->parseAddition();
                $result = new BinaryExpr($left, $op, $right);
                return $this->parsePostfixModifiers($result);
            }
        }

        return $left;
    }

    private function parseInList(Expr $left, bool $negated): InExpr
    {
        $this->expect(TokenType::LeftParen);

        if ($this->matchKeyword('SELECT') || $this->matchKeyword('WITH')) {
            $subquery = $this->parseSelect();
            $this->expect(TokenType::RightParen);
            return new InExpr($left, $subquery, $negated);
        }

        $list = [];
        $list[] = $this->parseExpression();
        while ($this->matchAndConsume(TokenType::Comma)) {
            $list[] = $this->parseExpression();
        }
        $this->expect(TokenType::RightParen);

        return new InExpr($left, $list, $negated);
    }

    private function parseBetween(Expr $left, bool $negated): BetweenExpr
    {
        $low = $this->parseAddition();
        $this->consumeKeyword('AND');
        $high = $this->parseAddition();

        return new BetweenExpr($left, $low, $high, $negated);
    }

    private function parseAddition(): Expr
    {
        $left = $this->parseMultiplication();

        while (true) {
            $token = $this->current();
            if ($token->type === TokenType::Operator && in_array($token->value, ['+', '-', '||'], true)) {
                $op = $token->value;
                $this->advance();
                $right = $this->parseMultiplication();
                $left = new BinaryExpr($left, $op, $right);
            } else {
                break;
            }
        }

        return $left;
    }

    private function parseMultiplication(): Expr
    {
        $left = $this->parseUnary();

        while (true) {
            $token = $this->current();
            if ($token->type === TokenType::Star) {
                $this->advance();
                $right = $this->parseUnary();
                $left = new BinaryExpr($left, '*', $right);
            } elseif ($token->type === TokenType::Operator && in_array($token->value, ['/', '%'], true)) {
                $op = $token->value;
                $this->advance();
                $right = $this->parseUnary();
                $left = new BinaryExpr($left, $op, $right);
            } else {
                break;
            }
        }

        return $left;
    }

    private function parseUnary(): Expr
    {
        $token = $this->current();

        if ($token->type === TokenType::Operator && ($token->value === '-' || $token->value === '+')) {
            $op = $token->value;
            $this->advance();
            $operand = $this->parseUnary();
            return new UnaryExpr($op, $operand);
        }

        $expr = $this->parsePrimary();

        // Handle PostgreSQL-style :: cast at this level so it works everywhere
        while ($this->current()->type === TokenType::Operator && $this->current()->value === '::') {
            $this->advance();
            $type = $this->expectIdentifier();
            $expr = new CastExpr($expr, $type);
        }

        return $expr;
    }

    private function parsePrimary(): Expr
    {
        $token = $this->current();

        if ($token->type === TokenType::Integer) {
            $this->advance();
            return new Literal((int)$token->value);
        }

        if ($token->type === TokenType::Float) {
            $this->advance();
            return new Literal((float)$token->value);
        }

        if ($token->type === TokenType::String) {
            $this->advance();
            $raw = $token->value;
            $inner = substr($raw, 1, -1);
            $inner = str_replace("''", "'", $inner);
            $inner = str_replace("\\'", "'", $inner);
            return new Literal($inner);
        }

        if ($token->type === TokenType::Boolean) {
            $this->advance();
            return new Literal(strtoupper($token->value) === 'TRUE');
        }

        if ($token->type === TokenType::Null) {
            $this->advance();
            return new Literal(null);
        }

        if ($token->type === TokenType::Placeholder) {
            $this->advance();
            return new Placeholder($token->value);
        }
        if ($token->type === TokenType::NamedPlaceholder) {
            $this->advance();
            return new Placeholder($token->value);
        }
        if ($token->type === TokenType::NumberedPlaceholder) {
            $this->advance();
            return new Placeholder($token->value);
        }

        if ($token->type === TokenType::Star) {
            $this->advance();
            return new Star();
        }

        if ($token->type === TokenType::LeftParen) {
            $this->advance();
            if ($this->matchKeyword('SELECT') || $this->matchKeyword('WITH')) {
                $subquery = $this->parseSelect();
                $this->expect(TokenType::RightParen);
                return new SubqueryExpr($subquery);
            }
            $expr = $this->parseExpression();
            $this->expect(TokenType::RightParen);
            return $expr;
        }

        if ($this->matchKeyword('CASE')) {
            return $this->parseCaseExpr();
        }

        if ($this->matchKeyword('CAST')) {
            return $this->parseCastExpr();
        }

        if ($this->matchKeyword('EXISTS')) {
            $this->advance();
            $this->expect(TokenType::LeftParen);
            $subquery = $this->parseSelect();
            $this->expect(TokenType::RightParen);
            return new ExistsExpr($subquery);
        }

        if ($token->type === TokenType::Identifier || $token->type === TokenType::QuotedIdentifier) {
            return $this->parseIdentifierExpr();
        }

        if ($token->type === TokenType::Keyword) {
            if ($this->peek(1)->type === TokenType::LeftParen) {
                return $this->parseIdentifierExpr();
            }
        }

        throw new Exception(
            "Unexpected token '{$token->value}' ({$token->type->name}) at position {$token->position}"
        );
    }

    private function parseIdentifierExpr(): Expr
    {
        $token = $this->current();
        $name = $this->extractIdentifier($token);
        $this->advance();

        if ($this->current()->type === TokenType::LeftParen) {
            return $this->parseFunctionCallExpr($name);
        }

        if ($this->current()->type === TokenType::Dot) {
            $this->advance();

            if ($this->current()->type === TokenType::Star) {
                $this->advance();
                return new Star($name);
            }

            $second = $this->extractIdentifier($this->current());
            $this->advance();

            if ($this->current()->type === TokenType::Dot) {
                $this->advance();

                if ($this->current()->type === TokenType::Star) {
                    $this->advance();
                    return new Star($second, $name);
                }

                $third = $this->extractIdentifier($this->current());
                $this->advance();
                return new ColumnRef($third, $second, $name);
            }

            return new ColumnRef($second, $name);
        }

        return new ColumnRef($name);
    }

    private function parseFunctionCallExpr(string $name): Expr
    {
        $upperName = strtoupper($name);
        $this->expect(TokenType::LeftParen);

        if ($this->current()->type === TokenType::Star) {
            $this->advance();
            $this->expect(TokenType::RightParen);
            $fn = new FunctionCall($upperName, [new Star()]);
            return $this->parseFunctionPostfix($fn);
        }

        if ($this->current()->type === TokenType::RightParen) {
            $this->advance();
            $fn = new FunctionCall($upperName);
            return $this->parseFunctionPostfix($fn);
        }

        $distinct = false;
        if ($this->matchKeyword('DISTINCT')) {
            $distinct = true;
            $this->advance();
        }

        $args = [];
        $args[] = $this->parseExpression();
        while ($this->matchAndConsume(TokenType::Comma)) {
            $args[] = $this->parseExpression();
        }

        $this->expect(TokenType::RightParen);
        $fn = new FunctionCall($upperName, $args, $distinct);
        return $this->parseFunctionPostfix($fn);
    }

    private function parseFunctionPostfix(FunctionCall $fn): Expr
    {
        if ($this->matchKeyword('FILTER')) {
            $this->advance();
            $this->expect(TokenType::LeftParen);
            $this->consumeKeyword('WHERE');
            $filterExpr = $this->parseExpression();
            $this->expect(TokenType::RightParen);
            $fn = new FunctionCall($fn->name, $fn->arguments, $fn->distinct, $filterExpr);
        }

        if ($this->matchKeyword('OVER')) {
            $this->advance();

            if ($this->current()->type === TokenType::Identifier) {
                $windowName = $this->extractIdentifier($this->current());
                $this->advance();
                return new WindowExpr($fn, windowName: $windowName);
            }

            $this->expect(TokenType::LeftParen);
            $spec = $this->parseWindowSpec();
            $this->expect(TokenType::RightParen);
            return new WindowExpr($fn, spec: $spec);
        }

        return $fn;
    }

    private function parseCaseExpr(): CaseExpr
    {
        $this->consumeKeyword('CASE');

        $operand = null;
        if (!$this->matchKeyword('WHEN')) {
            $operand = $this->parseExpression();
        }

        $whens = [];
        while ($this->matchKeyword('WHEN')) {
            $this->advance();
            $condition = $this->parseExpression();
            $this->consumeKeyword('THEN');
            $result = $this->parseExpression();
            $whens[] = new CaseWhen($condition, $result);
        }

        $else = null;
        if ($this->matchKeyword('ELSE')) {
            $this->advance();
            $else = $this->parseExpression();
        }

        $this->consumeKeyword('END');

        return new CaseExpr($operand, $whens, $else);
    }

    private function parseCastExpr(): CastExpr
    {
        $this->consumeKeyword('CAST');
        $this->expect(TokenType::LeftParen);
        $expr = $this->parseExpression();
        $this->consumeKeyword('AS');
        $type = $this->expectIdentifier();
        $this->expect(TokenType::RightParen);

        return new CastExpr($expr, $type);
    }

    /**
     * @return TableRef|SubquerySource
     */
    private function parseTableSource(): TableRef|SubquerySource
    {
        if ($this->current()->type === TokenType::LeftParen) {
            $this->advance();
            $subquery = $this->parseSelect();
            $this->expect(TokenType::RightParen);

            if ($this->matchKeyword('AS')) {
                $this->advance();
            }
            $alias = $this->expectIdentifier();

            return new SubquerySource($subquery, $alias);
        }

        return $this->parseTableRef();
    }

    private function parseTableRef(): TableRef
    {
        $name = $this->expectIdentifier();
        $schema = null;
        $alias = null;

        if ($this->current()->type === TokenType::Dot) {
            $this->advance();
            $schema = $name;
            $name = $this->expectIdentifier();
        }

        if ($this->matchKeyword('AS')) {
            $this->advance();
            $alias = $this->expectIdentifier();
        } elseif ($this->isTableAlias()) {
            $alias = $this->expectIdentifier();
        }

        return new TableRef($name, $alias, $schema);
    }

    private function isTableAlias(): bool
    {
        $token = $this->current();
        if ($token->type === TokenType::Identifier || $token->type === TokenType::QuotedIdentifier) {
            return true;
        }
        return false;
    }

    /**
     * @return JoinClause[]
     */
    private function parseJoins(): array
    {
        $joins = [];

        while (true) {
            $joinType = $this->tryParseJoinType();
            if ($joinType === null) {
                break;
            }

            $table = $this->parseTableSource();

            $condition = null;
            if ($joinType !== 'CROSS JOIN' && $joinType !== 'NATURAL JOIN') {
                if ($this->matchKeyword('ON')) {
                    $this->advance();
                    $condition = $this->parseExpression();
                }
            }

            $joins[] = new JoinClause($joinType, $table, $condition);
        }

        return $joins;
    }

    private function tryParseJoinType(): ?string
    {
        if ($this->matchKeyword('JOIN')) {
            $this->advance();
            return 'JOIN';
        }

        if ($this->matchKeyword('INNER')) {
            $this->advance();
            $this->consumeKeyword('JOIN');
            return 'INNER JOIN';
        }

        if ($this->matchKeyword('LEFT')) {
            $this->advance();
            if ($this->matchKeyword('OUTER')) {
                $this->advance();
            }
            $this->consumeKeyword('JOIN');
            return 'LEFT JOIN';
        }

        if ($this->matchKeyword('RIGHT')) {
            $this->advance();
            if ($this->matchKeyword('OUTER')) {
                $this->advance();
            }
            $this->consumeKeyword('JOIN');
            return 'RIGHT JOIN';
        }

        if ($this->matchKeyword('FULL')) {
            $this->advance();
            if ($this->matchKeyword('OUTER')) {
                $this->advance();
            }
            $this->consumeKeyword('JOIN');
            return 'FULL OUTER JOIN';
        }

        if ($this->matchKeyword('CROSS')) {
            $this->advance();
            $this->consumeKeyword('JOIN');
            return 'CROSS JOIN';
        }

        if ($this->matchKeyword('NATURAL')) {
            $this->advance();
            $this->consumeKeyword('JOIN');
            return 'NATURAL JOIN';
        }

        return null;
    }

    /**
     * @return Expr[]
     */
    private function parseExpressionList(): array
    {
        $exprs = [];
        $exprs[] = $this->parseExpression();

        while ($this->matchAndConsume(TokenType::Comma)) {
            $exprs[] = $this->parseExpression();
        }

        return $exprs;
    }

    /**
     * @return OrderByItem[]
     */
    private function parseOrderByList(): array
    {
        $items = [];
        $items[] = $this->parseOrderByItem();

        while ($this->matchAndConsume(TokenType::Comma)) {
            $items[] = $this->parseOrderByItem();
        }

        return $items;
    }

    private function parseOrderByItem(): OrderByItem
    {
        $expr = $this->parseExpression();

        $direction = 'ASC';
        if ($this->matchKeyword('ASC')) {
            $this->advance();
            $direction = 'ASC';
        } elseif ($this->matchKeyword('DESC')) {
            $this->advance();
            $direction = 'DESC';
        }

        $nulls = null;
        if ($this->matchKeyword('NULLS')) {
            $this->advance();
            if ($this->matchKeyword('FIRST')) {
                $this->advance();
                $nulls = 'FIRST';
            } elseif ($this->matchKeyword('LAST')) {
                $this->advance();
                $nulls = 'LAST';
            } else {
                throw new Exception(
                    "Expected FIRST or LAST after NULLS at position {$this->current()->position}, got '{$this->current()->value}'"
                );
            }
        }

        return new OrderByItem($expr, $direction, $nulls);
    }

    /**
     * @return WindowDefinition[]
     */
    private function parseWindowDefinitions(): array
    {
        $defs = [];

        do {
            $name = $this->expectIdentifier();
            $this->consumeKeyword('AS');
            $this->expect(TokenType::LeftParen);
            $spec = $this->parseWindowSpec();
            $this->expect(TokenType::RightParen);
            $defs[] = new WindowDefinition($name, $spec);
        } while ($this->matchAndConsume(TokenType::Comma));

        return $defs;
    }

    private function parseWindowSpec(): WindowSpec
    {
        $partitionBy = [];
        $orderBy = [];
        $frameType = null;
        $frameStart = null;
        $frameEnd = null;

        if ($this->matchKeyword('PARTITION')) {
            $this->advance();
            $this->consumeKeyword('BY');
            $partitionBy = $this->parseExpressionList();
        }

        if ($this->matchKeyword('ORDER')) {
            $this->advance();
            $this->consumeKeyword('BY');
            $orderBy = $this->parseOrderByList();
        }

        if ($this->matchKeyword('ROWS') || $this->matchKeyword('RANGE')) {
            $frameType = strtoupper($this->current()->value);
            $this->advance();

            if ($this->matchKeyword('BETWEEN')) {
                $this->advance();
                $frameStart = $this->parseFrameBound();
                $this->consumeKeyword('AND');
                $frameEnd = $this->parseFrameBound();
            } else {
                $frameStart = $this->parseFrameBound();
            }
        }

        return new WindowSpec($partitionBy, $orderBy, $frameType, $frameStart, $frameEnd);
    }

    private function parseFrameBound(): string
    {
        if ($this->matchKeyword('UNBOUNDED')) {
            $this->advance();
            if ($this->matchKeyword('PRECEDING')) {
                $this->advance();
                return 'UNBOUNDED PRECEDING';
            }
            if ($this->matchKeyword('FOLLOWING')) {
                $this->advance();
                return 'UNBOUNDED FOLLOWING';
            }
            throw new Exception(
                "Expected PRECEDING or FOLLOWING after UNBOUNDED at position {$this->current()->position}"
            );
        }

        if ($this->matchKeyword('CURRENT')) {
            $this->advance();
            $this->consumeKeyword('ROW');
            return 'CURRENT ROW';
        }

        if ($this->current()->type === TokenType::Integer) {
            $n = $this->current()->value;
            $this->advance();
            if ($this->matchKeyword('PRECEDING')) {
                $this->advance();
                return $n . ' PRECEDING';
            }
            if ($this->matchKeyword('FOLLOWING')) {
                $this->advance();
                return $n . ' FOLLOWING';
            }
            throw new Exception(
                "Expected PRECEDING or FOLLOWING after number at position {$this->current()->position}"
            );
        }

        throw new Exception(
            "Unexpected frame bound token '{$this->current()->value}' at position {$this->current()->position}"
        );
    }

    private function current(): Token
    {
        return $this->tokens[$this->pos];
    }

    private function peek(int $offset = 1): Token
    {
        $idx = $this->pos + $offset;
        if ($idx < count($this->tokens)) {
            return $this->tokens[$idx];
        }
        return $this->tokens[count($this->tokens) - 1];
    }

    private function advance(): Token
    {
        $token = $this->tokens[$this->pos];
        if ($this->pos < count($this->tokens) - 1) {
            $this->pos++;
        }
        return $token;
    }

    private function expect(TokenType $type, ?string $value = null): Token
    {
        $token = $this->current();
        if ($token->type !== $type) {
            $expected = $value !== null ? "'{$value}'" : $type->name;
            throw new Exception(
                "Expected {$expected} at position {$token->position}, got '{$token->value}'"
            );
        }
        if ($value !== null && strtoupper($token->value) !== strtoupper($value)) {
            throw new Exception(
                "Expected '{$value}' at position {$token->position}, got '{$token->value}'"
            );
        }
        $this->advance();
        return $token;
    }

    private function matchKeyword(string ...$keywords): bool
    {
        $token = $this->current();
        if ($token->type !== TokenType::Keyword) {
            return false;
        }
        $upper = strtoupper($token->value);
        foreach ($keywords as $keyword) {
            if ($upper === strtoupper($keyword)) {
                return true;
            }
        }
        return false;
    }

    private function peekKeyword(int $offset, string $keyword): bool
    {
        $token = $this->peek($offset);
        return $token->type === TokenType::Keyword && strtoupper($token->value) === strtoupper($keyword);
    }

    private function consumeKeyword(string $keyword): Token
    {
        $token = $this->current();
        if ($token->type !== TokenType::Keyword || strtoupper($token->value) !== strtoupper($keyword)) {
            throw new Exception(
                "Expected keyword '{$keyword}' at position {$token->position}, got '{$token->value}'"
            );
        }
        $this->advance();
        return $token;
    }

    private function expectNull(): Token
    {
        $token = $this->current();
        if ($token->type !== TokenType::Null) {
            throw new Exception(
                "Expected NULL at position {$token->position}, got '{$token->value}'"
            );
        }
        $this->advance();
        return $token;
    }

    private function expectIdentifierValue(string $value): Token
    {
        $token = $this->current();
        $upper = strtoupper($value);
        if (
            ($token->type === TokenType::Identifier || $token->type === TokenType::Keyword)
            && strtoupper($token->value) === $upper
        ) {
            $this->advance();
            return $token;
        }
        throw new Exception(
            "Expected '{$value}' at position {$token->position}, got '{$token->value}'"
        );
    }

    private function matchAndConsume(TokenType $type): bool
    {
        if ($this->current()->type === $type) {
            $this->advance();
            return true;
        }
        return false;
    }

    private function expectIdentifier(): string
    {
        $token = $this->current();
        if ($token->type === TokenType::Identifier) {
            $this->advance();
            return $token->value;
        }
        if ($token->type === TokenType::QuotedIdentifier) {
            $this->advance();
            $raw = $token->value;
            return substr($raw, 1, -1);
        }
        if ($token->type === TokenType::Keyword) {
            $this->advance();
            return $token->value;
        }
        throw new Exception(
            "Expected identifier at position {$token->position}, got '{$token->value}' ({$token->type->name})"
        );
    }

    private function extractIdentifier(Token $token): string
    {
        if ($token->type === TokenType::Identifier) {
            return $token->value;
        }
        if ($token->type === TokenType::QuotedIdentifier) {
            return substr($token->value, 1, -1);
        }
        if ($token->type === TokenType::Keyword) {
            return $token->value;
        }
        throw new Exception(
            "Expected identifier at position {$token->position}, got '{$token->value}' ({$token->type->name})"
        );
    }
}
