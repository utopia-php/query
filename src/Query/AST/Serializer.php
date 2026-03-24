<?php

namespace Utopia\Query\AST;

class Serializer
{
    public function serialize(SelectStatement $stmt): string
    {
        $parts = [];

        if (!empty($stmt->ctes)) {
            $parts[] = $this->serializeCtes($stmt->ctes);
        }

        $select = 'SELECT';
        if ($stmt->distinct) {
            $select .= ' DISTINCT';
        }

        $columns = [];
        foreach ($stmt->columns as $col) {
            $columns[] = $this->serializeExpr($col);
        }
        $select .= ' ' . implode(', ', $columns);
        $parts[] = $select;

        if ($stmt->from !== null) {
            $parts[] = 'FROM ' . $this->serializeTableSource($stmt->from);
        }

        foreach ($stmt->joins as $join) {
            $parts[] = $this->serializeJoin($join);
        }

        if ($stmt->where !== null) {
            $parts[] = 'WHERE ' . $this->serializeExpr($stmt->where);
        }

        if (!empty($stmt->groupBy)) {
            $exprs = [];
            foreach ($stmt->groupBy as $expr) {
                $exprs[] = $this->serializeExpr($expr);
            }
            $parts[] = 'GROUP BY ' . implode(', ', $exprs);
        }

        if ($stmt->having !== null) {
            $parts[] = 'HAVING ' . $this->serializeExpr($stmt->having);
        }

        if (!empty($stmt->windows)) {
            $defs = [];
            foreach ($stmt->windows as $win) {
                $defs[] = $this->quoteIdentifier($win->name) . ' AS (' . $this->serializeWindowSpec($win->spec) . ')';
            }
            $parts[] = 'WINDOW ' . implode(', ', $defs);
        }

        if (!empty($stmt->orderBy)) {
            $items = [];
            foreach ($stmt->orderBy as $item) {
                $items[] = $this->serializeOrderByItem($item);
            }
            $parts[] = 'ORDER BY ' . implode(', ', $items);
        }

        if ($stmt->limit !== null) {
            $parts[] = 'LIMIT ' . $this->serializeExpr($stmt->limit);
        }

        if ($stmt->offset !== null) {
            $parts[] = 'OFFSET ' . $this->serializeExpr($stmt->offset);
        }

        return implode(' ', $parts);
    }

    public function serializeExpr(Expr $expr): string
    {
        return match (true) {
            $expr instanceof AliasedExpr => $this->serializeExpr($expr->expr) . ' AS ' . $this->quoteIdentifier($expr->alias),
            $expr instanceof WindowExpr => $this->serializeWindowExpr($expr),
            $expr instanceof BinaryExpr => $this->serializeBinary($expr, null),
            $expr instanceof UnaryExpr => $this->serializeUnary($expr),
            $expr instanceof ColumnRef => $this->serializeColumnRef($expr),
            $expr instanceof Literal => $this->serializeLiteral($expr),
            $expr instanceof Star => $this->serializeStar($expr),
            $expr instanceof Placeholder => $expr->value,
            $expr instanceof Raw => $expr->sql,
            $expr instanceof FunctionCall => $this->serializeFunctionCall($expr),
            $expr instanceof InExpr => $this->serializeIn($expr),
            $expr instanceof BetweenExpr => $this->serializeBetween($expr),
            $expr instanceof ExistsExpr => $this->serializeExists($expr),
            $expr instanceof CaseExpr => $this->serializeCase($expr),
            $expr instanceof CastExpr => $this->serializeCast($expr),
            $expr instanceof SubqueryExpr => '(' . $this->serialize($expr->query) . ')',
            default => throw new \Utopia\Query\Exception('Unsupported expression type: ' . get_class($expr)),
        };
    }

    protected function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function operatorPrecedence(string $op): int
    {
        return match (strtoupper($op)) {
            'OR' => 1,
            'AND' => 2,
            '=', '!=', '<>', '<', '>', '<=', '>=' => 3,
            'LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE' => 3,
            '+', '-', '||' => 4,
            '*', '/', '%' => 5,
            default => 0,
        };
    }

    private function serializeBinary(BinaryExpr $expr, ?int $parentPrecedence): string
    {
        $prec = $this->operatorPrecedence($expr->operator);

        $left = $this->serializeBinaryChild($expr->left, $prec);
        $right = $this->serializeBinaryChild($expr->right, $prec);

        $sql = $left . ' ' . $expr->operator . ' ' . $right;

        if ($parentPrecedence !== null && $prec < $parentPrecedence) {
            return '(' . $sql . ')';
        }

        return $sql;
    }

    private function serializeBinaryChild(Expr $child, int $parentPrecedence): string
    {
        if ($child instanceof BinaryExpr) {
            return $this->serializeBinary($child, $parentPrecedence);
        }

        return $this->serializeExpr($child);
    }

    private function serializeUnary(UnaryExpr $expr): string
    {
        if ($expr->prefix) {
            $op = $expr->operator;
            $operand = $this->serializeExpr($expr->operand);
            if (strlen($op) === 1) {
                return $op . '(' . $operand . ')';
            }
            return $op . ' (' . $operand . ')';
        }

        $operand = $this->serializeExpr($expr->operand);
        return $operand . ' ' . $expr->operator;
    }

    private function serializeColumnRef(ColumnRef $expr): string
    {
        $parts = [];
        if ($expr->schema !== null) {
            $parts[] = $this->quoteIdentifier($expr->schema);
        }
        if ($expr->table !== null) {
            $parts[] = $this->quoteIdentifier($expr->table);
        }
        $parts[] = $this->quoteIdentifier($expr->name);
        return implode('.', $parts);
    }

    private function serializeLiteral(Literal $expr): string
    {
        if ($expr->value === null) {
            return 'NULL';
        }
        if (is_bool($expr->value)) {
            return $expr->value ? 'TRUE' : 'FALSE';
        }
        if (is_int($expr->value)) {
            return (string) $expr->value;
        }
        if (is_float($expr->value)) {
            return (string) $expr->value;
        }
        return "'" . str_replace("'", "''", $expr->value) . "'";
    }

    private function serializeStar(Star $expr): string
    {
        if ($expr->schema !== null && $expr->table !== null) {
            return $this->quoteIdentifier($expr->schema) . '.' . $this->quoteIdentifier($expr->table) . '.*';
        }
        if ($expr->table !== null) {
            return $this->quoteIdentifier($expr->table) . '.*';
        }
        return '*';
    }

    private function serializeFunctionCall(FunctionCall $expr): string
    {
        if (count($expr->arguments) === 1 && $expr->arguments[0] instanceof Star) {
            return $expr->name . '(*)';
        }

        if (empty($expr->arguments)) {
            return $expr->name . '()';
        }

        $args = [];
        foreach ($expr->arguments as $arg) {
            $args[] = $this->serializeExpr($arg);
        }

        $prefix = $expr->distinct ? 'DISTINCT ' : '';
        $sql = $expr->name . '(' . $prefix . implode(', ', $args) . ')';

        if ($expr->filter !== null) {
            $sql .= ' FILTER (WHERE ' . $this->serializeExpr($expr->filter) . ')';
        }

        return $sql;
    }

    private function serializeIn(InExpr $expr): string
    {
        $left = $this->serializeExpr($expr->expr);
        $keyword = $expr->negated ? 'NOT IN' : 'IN';

        if ($expr->list instanceof SelectStatement) {
            return $left . ' ' . $keyword . ' (' . $this->serialize($expr->list) . ')';
        }

        $items = [];
        foreach ($expr->list as $item) {
            $items[] = $this->serializeExpr($item);
        }
        return $left . ' ' . $keyword . ' (' . implode(', ', $items) . ')';
    }

    private function serializeBetween(BetweenExpr $expr): string
    {
        $left = $this->serializeExpr($expr->expr);
        $keyword = $expr->negated ? 'NOT BETWEEN' : 'BETWEEN';
        $low = $this->serializeExpr($expr->low);
        $high = $this->serializeExpr($expr->high);
        return $left . ' ' . $keyword . ' ' . $low . ' AND ' . $high;
    }

    private function serializeExists(ExistsExpr $expr): string
    {
        $keyword = $expr->negated ? 'NOT EXISTS' : 'EXISTS';
        return $keyword . ' (' . $this->serialize($expr->subquery) . ')';
    }

    private function serializeCase(CaseExpr $expr): string
    {
        $sql = 'CASE';
        if ($expr->operand !== null) {
            $sql .= ' ' . $this->serializeExpr($expr->operand);
        }

        foreach ($expr->whens as $when) {
            $sql .= ' WHEN ' . $this->serializeExpr($when->condition);
            $sql .= ' THEN ' . $this->serializeExpr($when->result);
        }

        if ($expr->else !== null) {
            $sql .= ' ELSE ' . $this->serializeExpr($expr->else);
        }

        $sql .= ' END';
        return $sql;
    }

    private function serializeCast(CastExpr $expr): string
    {
        return 'CAST(' . $this->serializeExpr($expr->expr) . ' AS ' . $expr->type . ')';
    }

    private function serializeWindowExpr(WindowExpr $expr): string
    {
        $fn = $this->serializeExpr($expr->function);

        if ($expr->windowName !== null) {
            return $fn . ' OVER ' . $this->quoteIdentifier($expr->windowName);
        }

        if ($expr->spec !== null) {
            return $fn . ' OVER (' . $this->serializeWindowSpec($expr->spec) . ')';
        }

        return $fn . ' OVER ()';
    }

    private function serializeWindowSpec(WindowSpec $spec): string
    {
        $parts = [];

        if (!empty($spec->partitionBy)) {
            $exprs = [];
            foreach ($spec->partitionBy as $expr) {
                $exprs[] = $this->serializeExpr($expr);
            }
            $parts[] = 'PARTITION BY ' . implode(', ', $exprs);
        }

        if (!empty($spec->orderBy)) {
            $items = [];
            foreach ($spec->orderBy as $item) {
                $items[] = $this->serializeOrderByItem($item);
            }
            $parts[] = 'ORDER BY ' . implode(', ', $items);
        }

        if ($spec->frameType !== null) {
            $frame = $spec->frameType;
            if ($spec->frameEnd !== null) {
                $frame .= ' BETWEEN ' . $spec->frameStart . ' AND ' . $spec->frameEnd;
            } else {
                $frame .= ' ' . $spec->frameStart;
            }
            $parts[] = $frame;
        }

        return implode(' ', $parts);
    }

    private function serializeOrderByItem(OrderByItem $item): string
    {
        $sql = $this->serializeExpr($item->expr) . ' ' . $item->direction;
        if ($item->nulls !== null) {
            $sql .= ' NULLS ' . $item->nulls;
        }
        return $sql;
    }

    private function serializeTableSource(TableRef|SubquerySource $source): string
    {
        if ($source instanceof SubquerySource) {
            return '(' . $this->serialize($source->query) . ') AS ' . $this->quoteIdentifier($source->alias);
        }

        return $this->serializeTableRef($source);
    }

    private function serializeTableRef(TableRef $ref): string
    {
        $sql = '';
        if ($ref->schema !== null) {
            $sql .= $this->quoteIdentifier($ref->schema) . '.';
        }
        $sql .= $this->quoteIdentifier($ref->name);
        if ($ref->alias !== null) {
            $sql .= ' AS ' . $this->quoteIdentifier($ref->alias);
        }
        return $sql;
    }

    private function serializeJoin(JoinClause $join): string
    {
        $sql = $join->type . ' ' . $this->serializeTableSource($join->table);
        if ($join->condition !== null) {
            $sql .= ' ON ' . $this->serializeExpr($join->condition);
        }
        return $sql;
    }

    /**
     * @param CteDefinition[] $ctes
     */
    private function serializeCtes(array $ctes): string
    {
        $recursive = false;
        foreach ($ctes as $cte) {
            if ($cte->recursive) {
                $recursive = true;
                break;
            }
        }

        $defs = [];
        foreach ($ctes as $cte) {
            $def = $this->quoteIdentifier($cte->name);
            if (!empty($cte->columns)) {
                $cols = [];
                foreach ($cte->columns as $col) {
                    $cols[] = $this->quoteIdentifier($col);
                }
                $def .= ' (' . implode(', ', $cols) . ')';
            }
            $def .= ' AS (' . $this->serialize($cte->query) . ')';
            $defs[] = $def;
        }

        $keyword = $recursive ? 'WITH RECURSIVE' : 'WITH';
        return $keyword . ' ' . implode(', ', $defs);
    }
}
