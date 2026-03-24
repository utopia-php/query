<?php

namespace Utopia\Query\AST;

class Walker
{
    /**
     * Walk the entire AST, applying the visitor to every node.
     * Returns a new (possibly transformed) SelectStatement.
     */
    public function walk(SelectStatement $stmt, Visitor $visitor): SelectStatement
    {
        $stmt = $this->walkStatement($stmt, $visitor);
        return $visitor->visitSelect($stmt);
    }

    private function walkStatement(SelectStatement $stmt, Visitor $visitor): SelectStatement
    {
        $columns = $this->walkExprArray($stmt->columns, $visitor);

        $from = $stmt->from;
        if ($from instanceof TableRef) {
            $from = $visitor->visitTableRef($from);
        } elseif ($from instanceof SubquerySource) {
            $from = $this->walkSubquerySource($from, $visitor);
        }

        $joins = [];
        foreach ($stmt->joins as $join) {
            $joins[] = $this->walkJoin($join, $visitor);
        }

        $where = $stmt->where !== null ? $this->walkExpr($stmt->where, $visitor) : null;

        $groupBy = $this->walkExprArray($stmt->groupBy, $visitor);

        $having = $stmt->having !== null ? $this->walkExpr($stmt->having, $visitor) : null;

        $orderBy = [];
        foreach ($stmt->orderBy as $item) {
            $orderBy[] = $this->walkOrderByItem($item, $visitor);
        }

        $limit = $stmt->limit !== null ? $this->walkExpr($stmt->limit, $visitor) : null;
        $offset = $stmt->offset !== null ? $this->walkExpr($stmt->offset, $visitor) : null;

        $ctes = [];
        foreach ($stmt->ctes as $cte) {
            $ctes[] = $this->walkCte($cte, $visitor);
        }

        $windows = [];
        foreach ($stmt->windows as $win) {
            $windows[] = $this->walkWindowDefinition($win, $visitor);
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
            distinct: $stmt->distinct,
            ctes: $ctes,
            windows: $windows,
        );
    }

    private function walkExpr(Expr $expr, Visitor $visitor): Expr
    {
        $walked = match (true) {
            $expr instanceof BinaryExpr => new BinaryExpr(
                $this->walkExpr($expr->left, $visitor),
                $expr->operator,
                $this->walkExpr($expr->right, $visitor),
            ),
            $expr instanceof UnaryExpr => new UnaryExpr(
                $expr->operator,
                $this->walkExpr($expr->operand, $visitor),
                $expr->prefix,
            ),
            $expr instanceof FunctionCall => $this->walkFunctionCall($expr, $visitor),
            $expr instanceof AliasedExpr => new AliasedExpr(
                $this->walkExpr($expr->expr, $visitor),
                $expr->alias,
            ),
            $expr instanceof InExpr => $this->walkInExpr($expr, $visitor),
            $expr instanceof BetweenExpr => new BetweenExpr(
                $this->walkExpr($expr->expr, $visitor),
                $this->walkExpr($expr->low, $visitor),
                $this->walkExpr($expr->high, $visitor),
                $expr->negated,
            ),
            $expr instanceof ExistsExpr => new ExistsExpr(
                $this->walkStatement($expr->subquery, $visitor),
                $expr->negated,
            ),
            $expr instanceof CaseExpr => $this->walkCaseExpr($expr, $visitor),
            $expr instanceof CastExpr => new CastExpr(
                $this->walkExpr($expr->expr, $visitor),
                $expr->type,
            ),
            $expr instanceof SubqueryExpr => new SubqueryExpr(
                $this->walkStatement($expr->query, $visitor),
            ),
            $expr instanceof WindowExpr => $this->walkWindowExpr($expr, $visitor),
            default => $expr,
        };

        return $visitor->visitExpr($walked);
    }

    /**
     * @param Expr[] $exprs
     * @return Expr[]
     */
    private function walkExprArray(array $exprs, Visitor $visitor): array
    {
        $result = [];
        foreach ($exprs as $expr) {
            $result[] = $this->walkExpr($expr, $visitor);
        }
        return $result;
    }

    private function walkFunctionCall(FunctionCall $expr, Visitor $visitor): FunctionCall
    {
        $args = $this->walkExprArray($expr->arguments, $visitor);
        $filter = $expr->filter !== null ? $this->walkExpr($expr->filter, $visitor) : null;

        return new FunctionCall(
            $expr->name,
            $args,
            $expr->distinct,
            $filter,
        );
    }

    private function walkInExpr(InExpr $expr, Visitor $visitor): InExpr
    {
        $walked = $this->walkExpr($expr->expr, $visitor);

        if ($expr->list instanceof SelectStatement) {
            $list = $this->walkStatement($expr->list, $visitor);
        } else {
            $list = $this->walkExprArray($expr->list, $visitor);
        }

        return new InExpr($walked, $list, $expr->negated);
    }

    private function walkCaseExpr(CaseExpr $expr, Visitor $visitor): CaseExpr
    {
        $operand = $expr->operand !== null ? $this->walkExpr($expr->operand, $visitor) : null;

        $whens = [];
        foreach ($expr->whens as $when) {
            $whens[] = new CaseWhen(
                $this->walkExpr($when->condition, $visitor),
                $this->walkExpr($when->result, $visitor),
            );
        }

        $else = $expr->else !== null ? $this->walkExpr($expr->else, $visitor) : null;

        return new CaseExpr($operand, $whens, $else);
    }

    private function walkWindowExpr(WindowExpr $expr, Visitor $visitor): WindowExpr
    {
        $fn = $this->walkExpr($expr->function, $visitor);
        $spec = $expr->spec !== null ? $this->walkWindowSpec($expr->spec, $visitor) : null;

        return new WindowExpr($fn, $expr->windowName, $spec);
    }

    private function walkWindowSpec(WindowSpec $spec, Visitor $visitor): WindowSpec
    {
        $partitionBy = $this->walkExprArray($spec->partitionBy, $visitor);

        $orderBy = [];
        foreach ($spec->orderBy as $item) {
            $orderBy[] = $this->walkOrderByItem($item, $visitor);
        }

        return new WindowSpec(
            $partitionBy,
            $orderBy,
            $spec->frameType,
            $spec->frameStart,
            $spec->frameEnd,
        );
    }

    private function walkWindowDefinition(WindowDefinition $win, Visitor $visitor): WindowDefinition
    {
        return new WindowDefinition(
            $win->name,
            $this->walkWindowSpec($win->spec, $visitor),
        );
    }

    private function walkOrderByItem(OrderByItem $item, Visitor $visitor): OrderByItem
    {
        return new OrderByItem(
            $this->walkExpr($item->expr, $visitor),
            $item->direction,
            $item->nulls,
        );
    }

    private function walkJoin(JoinClause $join, Visitor $visitor): JoinClause
    {
        $table = $join->table;
        if ($table instanceof TableRef) {
            $table = $visitor->visitTableRef($table);
        } elseif ($table instanceof SubquerySource) {
            $table = $this->walkSubquerySource($table, $visitor);
        }

        $condition = $join->condition !== null ? $this->walkExpr($join->condition, $visitor) : null;

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
