<?php

namespace Utopia\Query\AST;

use Utopia\Query\AST\Expression\Aliased;
use Utopia\Query\AST\Expression\Between;
use Utopia\Query\AST\Expression\Binary;
use Utopia\Query\AST\Expression\CaseWhen;
use Utopia\Query\AST\Expression\Cast;
use Utopia\Query\AST\Expression\Conditional;
use Utopia\Query\AST\Expression\Exists;
use Utopia\Query\AST\Expression\In;
use Utopia\Query\AST\Expression\Subquery;
use Utopia\Query\AST\Expression\Unary;
use Utopia\Query\AST\Expression\Window;
use Utopia\Query\AST\Reference\Table;
use Utopia\Query\AST\Specification\Window as WindowSpecification;
use Utopia\Query\AST\Statement\Select;

class Walker
{
    /**
     * Walk the entire AST, applying the visitor to every node.
     * Returns a new (possibly transformed) Select.
     */
    public function walk(Select $stmt, Visitor $visitor): Select
    {
        $stmt = $this->walkStatement($stmt, $visitor);
        return $visitor->visitSelect($stmt);
    }

    private function walkStatement(Select $stmt, Visitor $visitor): Select
    {
        $columns = $this->walkExpressionArray($stmt->columns, $visitor);

        $from = $stmt->from;
        if ($from instanceof Table) {
            $from = $visitor->visitTableReference($from);
        } elseif ($from instanceof SubquerySource) {
            $from = $this->walkSubquerySource($from, $visitor);
        }

        $joins = [];
        foreach ($stmt->joins as $join) {
            $joins[] = $this->walkJoin($join, $visitor);
        }

        $where = $stmt->where !== null ? $this->walkExpression($stmt->where, $visitor) : null;

        $groupBy = $this->walkExpressionArray($stmt->groupBy, $visitor);

        $having = $stmt->having !== null ? $this->walkExpression($stmt->having, $visitor) : null;

        $orderBy = [];
        foreach ($stmt->orderBy as $item) {
            $orderBy[] = $this->walkOrderByItem($item, $visitor);
        }

        $limit = $stmt->limit !== null ? $this->walkExpression($stmt->limit, $visitor) : null;
        $offset = $stmt->offset !== null ? $this->walkExpression($stmt->offset, $visitor) : null;

        $ctes = [];
        foreach ($stmt->ctes as $cte) {
            $ctes[] = $this->walkCte($cte, $visitor);
        }

        $windows = [];
        foreach ($stmt->windows as $win) {
            $windows[] = $this->walkWindowDefinition($win, $visitor);
        }

        return new Select(
            columns: $columns,
            from: $from,
            joins: $joins,
            where: $where,
            groupBy: $groupBy,
            having: $having,
            orderBy: $orderBy,
            limit: $limit,
            offset: $offset,
            distinct: $stmt->distinct,
            ctes: $ctes,
            windows: $windows,
        );
    }

    private function walkExpression(Expression $expression, Visitor $visitor): Expression
    {
        $walked = match (true) {
            $expression instanceof Binary => new Binary(
                $this->walkExpression($expression->left, $visitor),
                $expression->operator,
                $this->walkExpression($expression->right, $visitor),
            ),
            $expression instanceof Unary => new Unary(
                $expression->operator,
                $this->walkExpression($expression->operand, $visitor),
                $expression->prefix,
            ),
            $expression instanceof FunctionCall => $this->walkFunctionCall($expression, $visitor),
            $expression instanceof Aliased => new Aliased(
                $this->walkExpression($expression->expression, $visitor),
                $expression->alias,
            ),
            $expression instanceof In => $this->walkInExpression($expression, $visitor),
            $expression instanceof Between => new Between(
                $this->walkExpression($expression->expression, $visitor),
                $this->walkExpression($expression->low, $visitor),
                $this->walkExpression($expression->high, $visitor),
                $expression->negated,
            ),
            $expression instanceof Exists => new Exists(
                $this->walkStatement($expression->subquery, $visitor),
                $expression->negated,
            ),
            $expression instanceof Conditional => $this->walkConditionalExpression($expression, $visitor),
            $expression instanceof Cast => new Cast(
                $this->walkExpression($expression->expression, $visitor),
                $expression->type,
            ),
            $expression instanceof Subquery => new Subquery(
                $this->walkStatement($expression->query, $visitor),
            ),
            $expression instanceof Window => $this->walkWindowExpression($expression, $visitor),
            default => $expression,
        };

        return $visitor->visitExpression($walked);
    }

    /**
     * @param Expression[] $expressions
     * @return Expression[]
     */
    private function walkExpressionArray(array $expressions, Visitor $visitor): array
    {
        $result = [];
        foreach ($expressions as $expression) {
            $result[] = $this->walkExpression($expression, $visitor);
        }
        return $result;
    }

    private function walkFunctionCall(FunctionCall $expression, Visitor $visitor): FunctionCall
    {
        $args = $this->walkExpressionArray($expression->arguments, $visitor);
        $filter = $expression->filter !== null ? $this->walkExpression($expression->filter, $visitor) : null;

        return new FunctionCall(
            $expression->name,
            $args,
            $expression->distinct,
            $filter,
        );
    }

    private function walkInExpression(In $expression, Visitor $visitor): In
    {
        $walked = $this->walkExpression($expression->expression, $visitor);

        if ($expression->list instanceof Select) {
            $list = $this->walkStatement($expression->list, $visitor);
        } else {
            $list = $this->walkExpressionArray($expression->list, $visitor);
        }

        return new In($walked, $list, $expression->negated);
    }

    private function walkConditionalExpression(Conditional $expression, Visitor $visitor): Conditional
    {
        $operand = $expression->operand !== null ? $this->walkExpression($expression->operand, $visitor) : null;

        $whens = [];
        foreach ($expression->whens as $when) {
            $whens[] = new CaseWhen(
                $this->walkExpression($when->condition, $visitor),
                $this->walkExpression($when->result, $visitor),
            );
        }

        $else = $expression->else !== null ? $this->walkExpression($expression->else, $visitor) : null;

        return new Conditional($operand, $whens, $else);
    }

    private function walkWindowExpression(Window $expression, Visitor $visitor): Window
    {
        $function = $this->walkExpression($expression->function, $visitor);
        $specification = $expression->specification !== null ? $this->walkWindowSpecification($expression->specification, $visitor) : null;

        return new Window($function, $expression->windowName, $specification);
    }

    private function walkWindowSpecification(WindowSpecification $specification, Visitor $visitor): WindowSpecification
    {
        $partitionBy = $this->walkExpressionArray($specification->partitionBy, $visitor);

        $orderBy = [];
        foreach ($specification->orderBy as $item) {
            $orderBy[] = $this->walkOrderByItem($item, $visitor);
        }

        return new WindowSpecification(
            $partitionBy,
            $orderBy,
            $specification->frameType,
            $specification->frameStart,
            $specification->frameEnd,
        );
    }

    private function walkWindowDefinition(WindowDefinition $win, Visitor $visitor): WindowDefinition
    {
        return new WindowDefinition(
            $win->name,
            $this->walkWindowSpecification($win->specification, $visitor),
        );
    }

    private function walkOrderByItem(OrderByItem $item, Visitor $visitor): OrderByItem
    {
        return new OrderByItem(
            $this->walkExpression($item->expression, $visitor),
            $item->direction,
            $item->nulls,
        );
    }

    private function walkJoin(JoinClause $join, Visitor $visitor): JoinClause
    {
        $table = $join->table;
        if ($table instanceof Table) {
            $table = $visitor->visitTableReference($table);
        } elseif ($table instanceof SubquerySource) {
            $table = $this->walkSubquerySource($table, $visitor);
        }

        $condition = $join->condition !== null ? $this->walkExpression($join->condition, $visitor) : null;

        return new JoinClause($join->type, $table, $condition);
    }

    private function walkSubquerySource(SubquerySource $source, Visitor $visitor): SubquerySource
    {
        return new SubquerySource(
            $this->walk($source->query, $visitor),
            $source->alias,
        );
    }

    private function walkCte(CteDefinition $cte, Visitor $visitor): CteDefinition
    {
        $walkedQuery = $this->walk($cte->query, $visitor);

        return new CteDefinition(
            $cte->name,
            $walkedQuery,
            $cte->columns,
            $cte->recursive,
        );
    }
}
