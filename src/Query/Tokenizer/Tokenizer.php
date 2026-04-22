<?php

namespace Utopia\Query\Tokenizer;

use Utopia\Query\Exception\ValidationException;

class Tokenizer
{
    private const KEYWORD_MAP = [
        'SELECT' => true, 'FROM' => true, 'WHERE' => true, 'AND' => true,
        'OR' => true, 'NOT' => true, 'JOIN' => true, 'LEFT' => true,
        'RIGHT' => true, 'INNER' => true, 'OUTER' => true, 'FULL' => true,
        'CROSS' => true, 'NATURAL' => true, 'ON' => true, 'AS' => true,
        'ORDER' => true, 'BY' => true, 'GROUP' => true, 'HAVING' => true,
        'LIMIT' => true, 'OFFSET' => true, 'ASC' => true, 'DESC' => true,
        'IN' => true, 'BETWEEN' => true, 'LIKE' => true, 'ILIKE' => true,
        'IS' => true, 'CASE' => true, 'WHEN' => true, 'THEN' => true,
        'ELSE' => true, 'END' => true, 'EXISTS' => true, 'DISTINCT' => true,
        'ALL' => true, 'UNION' => true, 'INTERSECT' => true, 'EXCEPT' => true,
        'WITH' => true, 'RECURSIVE' => true, 'SET' => true, 'INSERT' => true,
        'INTO' => true, 'VALUES' => true, 'UPDATE' => true, 'DELETE' => true,
        'CREATE' => true, 'ALTER' => true, 'DROP' => true, 'TABLE' => true,
        'INDEX' => true, 'VIEW' => true, 'OVER' => true, 'PARTITION' => true,
        'WINDOW' => true, 'ROWS' => true, 'RANGE' => true, 'UNBOUNDED' => true,
        'PRECEDING' => true, 'FOLLOWING' => true, 'CURRENT' => true,
        'ROW' => true, 'FETCH' => true, 'NEXT' => true, 'FIRST' => true,
        'LAST' => true, 'NULLS' => true, 'CAST' => true, 'FILTER' => true,
        'WITHIN' => true,
    ];

    private string $sql;

    private int $length;

    private int $pos;

    /**
     * @return Token[]
     */
    public function tokenize(string $sql): array
    {
        $this->sql = $sql;
        $this->length = strlen($sql);
        $this->pos = 0;

        $tokens = [];

        while ($this->pos < $this->length) {
            $start = $this->pos;
            $char = $this->sql[$this->pos];

            if ($char === ' ' || $char === "\t" || $char === "\n" || $char === "\r") {
                $tokens[] = $this->readWhitespace($start);
                continue;
            }

            if ($char === '-' && $this->peek(1) === '-') {
                $tokens[] = $this->readLineComment($start);
                continue;
            }

            if ($char === '/' && $this->peek(1) === '*') {
                $tokens[] = $this->readBlockComment($start);
                continue;
            }

            if ($char === '\'') {
                $tokens[] = $this->readString($start);
                continue;
            }

            $quoteChar = $this->getIdentifierQuoteChar();
            if ($char === $quoteChar) {
                $tokens[] = $this->readQuotedIdentifier($start, $quoteChar);
                continue;
            }

            if ($char === '"' && $quoteChar !== '"') {
                $tokens[] = $this->readQuotedIdentifier($start, '"');
                continue;
            }

            if ($this->isDigit($char)) {
                $tokens[] = $this->readNumber($start);
                continue;
            }

            if ($this->isIdentStart($char)) {
                $tokens[] = $this->readIdentifierOrKeyword($start);
                continue;
            }

            if ($char === '(') {
                $this->pos++;
                $tokens[] = new Token(TokenType::LeftParen, '(', $start);
                continue;
            }

            if ($char === ')') {
                $this->pos++;
                $tokens[] = new Token(TokenType::RightParen, ')', $start);
                continue;
            }

            if ($char === ',') {
                $this->pos++;
                $tokens[] = new Token(TokenType::Comma, ',', $start);
                continue;
            }

            if ($char === ';') {
                $this->pos++;
                $tokens[] = new Token(TokenType::Semicolon, ';', $start);
                continue;
            }

            if ($char === '.') {
                if ($this->peek(1) !== null && $this->isDigit($this->peek(1))) {
                    $tokens[] = $this->readNumber($start);
                    continue;
                }
                $this->pos++;
                $tokens[] = new Token(TokenType::Dot, '.', $start);
                continue;
            }

            if ($char === '*') {
                $this->pos++;
                $tokens[] = new Token(TokenType::Star, '*', $start);
                continue;
            }

            if ($char === '?') {
                $this->pos++;
                $tokens[] = new Token(TokenType::Placeholder, '?', $start);
                continue;
            }

            if ($char === ':') {
                $next = $this->peek(1);
                if ($next === ':') {
                    $this->pos += 2;
                    $tokens[] = new Token(TokenType::Operator, '::', $start);
                    continue;
                }
                if ($next !== null && $this->isIdentStart($next)) {
                    $tokens[] = $this->readNamedPlaceholder($start);
                    continue;
                }
                $this->pos++;
                $tokens[] = new Token(TokenType::Operator, ':', $start);
                continue;
            }

            if ($char === '$') {
                $next = $this->peek(1);
                if ($next !== null && $this->isDigit($next)) {
                    $tokens[] = $this->readNumberedPlaceholder($start);
                    continue;
                }
                $this->pos++;
                $tokens[] = new Token(TokenType::Operator, '$', $start);
                continue;
            }

            $op = $this->tryReadOperator($start);
            if ($op !== null) {
                $tokens[] = $op;
                continue;
            }

            // Emit unknown characters as single-char operator tokens
            $this->pos++;
            $tokens[] = new Token(TokenType::Operator, $char, $start);
        }

        $tokens[] = new Token(TokenType::Eof, '', $this->pos);

        return $tokens;
    }

    /**
     * @param Token[] $tokens
     * @return Token[]
     */
    public static function filter(array $tokens): array
    {
        return array_values(array_filter(
            $tokens,
            fn (Token $t) => $t->type !== TokenType::Whitespace
                && $t->type !== TokenType::LineComment
                && $t->type !== TokenType::BlockComment
        ));
    }

    protected function getIdentifierQuoteChar(): string
    {
        return '`';
    }

    private function peek(int $offset): ?string
    {
        $idx = $this->pos + $offset;
        return $idx < $this->length ? $this->sql[$idx] : null;
    }

    private function isDigit(string $char): bool
    {
        return $char >= '0' && $char <= '9';
    }

    private function isIdentStart(string $char): bool
    {
        return ($char >= 'a' && $char <= 'z')
            || ($char >= 'A' && $char <= 'Z')
            || $char === '_';
    }

    private function isIdentChar(string $char): bool
    {
        return $this->isIdentStart($char) || $this->isDigit($char);
    }

    private function readWhitespace(int $start): Token
    {
        while ($this->pos < $this->length) {
            $c = $this->sql[$this->pos];
            if ($c === ' ' || $c === "\t" || $c === "\n" || $c === "\r") {
                $this->pos++;
            } else {
                break;
            }
        }

        return new Token(TokenType::Whitespace, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readLineComment(int $start): Token
    {
        $this->pos += 2;
        while ($this->pos < $this->length && $this->sql[$this->pos] !== "\n") {
            $this->pos++;
        }

        return new Token(TokenType::LineComment, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readBlockComment(int $start): Token
    {
        $this->pos += 2;
        $terminated = false;
        while ($this->pos < $this->length - 1) {
            if ($this->sql[$this->pos] === '*' && $this->sql[$this->pos + 1] === '/') {
                $this->pos += 2;
                $terminated = true;
                break;
            }
            $this->pos++;
        }

        if (!$terminated) {
            $this->pos = $this->length;
        }

        return new Token(TokenType::BlockComment, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readString(int $start): Token
    {
        $this->pos++;
        while ($this->pos < $this->length) {
            $char = $this->sql[$this->pos];
            if ($char === '\\') {
                // Backslash escape: skip next character. Reject trailing backslash at EOF.
                if ($this->pos + 1 >= $this->length) {
                    throw new ValidationException('Unterminated string literal');
                }
                $this->pos += 2;
                continue;
            }
            if ($char === '\'') {
                $this->pos++;
                // Check for escaped quote ''
                if ($this->pos < $this->length && $this->sql[$this->pos] === '\'') {
                    $this->pos++;
                    continue;
                }
                break;
            }
            $this->pos++;
        }

        return new Token(TokenType::String, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readQuotedIdentifier(int $start, string $quote): Token
    {
        $this->pos++;
        while ($this->pos < $this->length) {
            if ($this->sql[$this->pos] === $quote) {
                $this->pos++;
                // Check for escaped quote (doubled)
                if ($this->pos < $this->length && $this->sql[$this->pos] === $quote) {
                    $this->pos++;
                    continue;
                }
                break;
            }
            $this->pos++;
        }

        return new Token(TokenType::QuotedIdentifier, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readNumber(int $start): Token
    {
        $isFloat = false;

        // Handle dot-prefixed floats like .5
        if ($this->pos < $this->length && $this->sql[$this->pos] === '.') {
            $isFloat = true;
            $this->pos++;
        }

        while ($this->pos < $this->length && $this->isDigit($this->sql[$this->pos])) {
            $this->pos++;
        }

        if (!$isFloat && $this->pos < $this->length && $this->sql[$this->pos] === '.') {
            $next = $this->peek(1);
            if ($next !== null && $this->isDigit($next)) {
                $isFloat = true;
                $this->pos++;
                while ($this->pos < $this->length && $this->isDigit($this->sql[$this->pos])) {
                    $this->pos++;
                }
            }
        }

        // Handle scientific notation (e.g. 1.5e10, 1e-3, 2.5E+8)
        if ($this->pos < $this->length) {
            $c = $this->sql[$this->pos];
            if ($c === 'e' || $c === 'E') {
                $nextIdx = $this->pos + 1;
                if ($nextIdx < $this->length && ($this->sql[$nextIdx] === '+' || $this->sql[$nextIdx] === '-')) {
                    $nextIdx++;
                }
                if ($nextIdx < $this->length && $this->isDigit($this->sql[$nextIdx])) {
                    $isFloat = true;
                    $this->pos = $nextIdx;
                    while ($this->pos < $this->length && $this->isDigit($this->sql[$this->pos])) {
                        $this->pos++;
                    }
                }
            }
        }

        $value = substr($this->sql, $start, $this->pos - $start);
        $type = $isFloat ? TokenType::Float : TokenType::Integer;

        return new Token($type, $value, $start);
    }

    private function readIdentifierOrKeyword(int $start): Token
    {
        while ($this->pos < $this->length && $this->isIdentChar($this->sql[$this->pos])) {
            $this->pos++;
        }

        $value = substr($this->sql, $start, $this->pos - $start);
        $upper = strtoupper($value);

        if ($upper === 'NULL') {
            return new Token(TokenType::Null, $upper, $start);
        }

        if ($upper === 'TRUE' || $upper === 'FALSE') {
            return new Token(TokenType::Boolean, $upper, $start);
        }

        if (isset(self::KEYWORD_MAP[$upper])) {
            return new Token(TokenType::Keyword, $upper, $start);
        }

        return new Token(TokenType::Identifier, $value, $start);
    }

    private function readNamedPlaceholder(int $start): Token
    {
        $this->pos++;
        while ($this->pos < $this->length && $this->isIdentChar($this->sql[$this->pos])) {
            $this->pos++;
        }

        return new Token(TokenType::NamedPlaceholder, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function readNumberedPlaceholder(int $start): Token
    {
        $this->pos++;
        while ($this->pos < $this->length && $this->isDigit($this->sql[$this->pos])) {
            $this->pos++;
        }

        return new Token(TokenType::NumberedPlaceholder, substr($this->sql, $start, $this->pos - $start), $start);
    }

    private function tryReadOperator(int $start): ?Token
    {
        $char = $this->sql[$this->pos];
        $next = $this->peek(1);

        $twoChar = match (true) {
            $char === '<' && $next === '=' => '<=',
            $char === '>' && $next === '=' => '>=',
            $char === '!' && $next === '=' => '!=',
            $char === '<' && $next === '>' => '<>',
            $char === '|' && $next === '|' => '||',
            default => null,
        };

        if ($twoChar !== null) {
            $this->pos += 2;
            return new Token(TokenType::Operator, $twoChar, $start);
        }

        if (in_array($char, ['=', '<', '>', '+', '-', '/', '%'], true)) {
            $this->pos++;
            return new Token(TokenType::Operator, $char, $start);
        }

        return null;
    }
}
