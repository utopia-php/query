<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\Expression\Binary;
use Utopia\Query\AST\Reference\Table;
use Utopia\Query\AST\Statement\Select;
use Utopia\Query\AST\Visitor;

class FilterInjector implements Visitor
{
    public function __construct(private readonly Expression $condition)
    {
    }

    public function visitExpression(Expression $expression): Expression
    {
        return $expression;
    }

    public function visitTableReference(Table $reference): Table
    {
        return $reference;
    }

    public function visitSelect(Select $stmt): Select
    {
        if ($stmt->where === null) {
            return $stmt->with(where: $this->condition);
        }

        $combined = new Binary($stmt->where, 'AND', $this->condition);
        return $stmt->with(where: $combined);
    }
}
