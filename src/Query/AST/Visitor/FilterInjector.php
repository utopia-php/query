<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\BinaryExpr;
use Utopia\Query\AST\Expr;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\Visitor;

class FilterInjector implements Visitor
{
    public function __construct(private readonly Expr $condition) {}

    public function visitExpr(Expr $expr): Expr
    {
        return $expr;
    }

    public function visitTableRef(TableRef $ref): TableRef
    {
        return $ref;
    }

    public function visitSelect(SelectStatement $stmt): SelectStatement
    {
        if ($stmt->where === null) {
            return $stmt->with(where: $this->condition);
        }

        $combined = new BinaryExpr($stmt->where, 'AND', $this->condition);
        return $stmt->with(where: $combined);
    }
}
