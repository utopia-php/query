<?php

namespace Utopia\Query\AST;

interface Visitor
{
    // Visit an expression node. Return the same node to keep it, or a new node to replace it.
    public function visitExpr(Expr $expr): Expr;

    // Visit a table reference. Return the same or replacement.
    public function visitTableRef(TableRef $ref): TableRef;

    // Visit a SelectStatement. Return the same or replacement.
    public function visitSelect(SelectStatement $stmt): SelectStatement;
}
