<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\Expr;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\Visitor;
use Utopia\Query\Exception;

class ColumnValidator implements Visitor
{
    /** @param string[] $allowedColumns */
    public function __construct(private readonly array $allowedColumns)
    {
    }

    public function visitExpr(Expr $expr): Expr
    {
        if ($expr instanceof ColumnRef) {
            if (!in_array($expr->name, $this->allowedColumns, true)) {
                throw new Exception("Column '{$expr->name}' is not in the allowed list");
            }
        }
        return $expr;
    }

    public function visitTableRef(TableRef $ref): TableRef
    {
        return $ref;
    }

    public function visitSelect(SelectStatement $stmt): SelectStatement
    {
        return $stmt;
    }
}
