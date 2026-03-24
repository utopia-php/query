<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\Expr;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\Visitor;

class TableRenamer implements Visitor
{
    /** @param array<string, string> $renames map of old name to new name */
    public function __construct(private readonly array $renames) {}

    public function visitExpr(Expr $expr): Expr
    {
        if ($expr instanceof ColumnRef && $expr->table !== null) {
            $newTable = $this->renames[$expr->table] ?? null;
            if ($newTable !== null) {
                return new ColumnRef($expr->name, $newTable, $expr->schema);
            }
        }

        if ($expr instanceof Star && $expr->table !== null) {
            $newTable = $this->renames[$expr->table] ?? null;
            if ($newTable !== null) {
                return new Star($newTable, $expr->schema);
            }
        }

        return $expr;
    }

    public function visitTableRef(TableRef $ref): TableRef
    {
        $newName = $this->renames[$ref->name] ?? null;
        $newAlias = $ref->alias !== null ? ($this->renames[$ref->alias] ?? null) : null;

        if ($newName !== null || $newAlias !== null) {
            return new TableRef(
                $newName ?? $ref->name,
                $newAlias ?? $ref->alias,
                $ref->schema,
            );
        }

        return $ref;
    }

    public function visitSelect(SelectStatement $stmt): SelectStatement
    {
        return $stmt;
    }
}
