<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\Reference\Column;
use Utopia\Query\AST\Reference\Table;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Visitor;
use Utopia\Query\Exception;

class ColumnValidator implements Visitor
{
    /** @param string[] $allowedColumns */
    public function __construct(private readonly array $allowedColumns)
    {
    }

    public function visitExpression(Expression $expression): Expression
    {
        if ($expression instanceof Column) {
            if (!in_array($expression->name, $this->allowedColumns, true)) {
                throw new Exception("Column '{$expression->name}' is not in the allowed list");
            }
        }
        return $expression;
    }

    public function visitTableReference(Table $reference): Table
    {
        return $reference;
    }

    public function visitSelect(SelectStatement $stmt): SelectStatement
    {
        return $stmt;
    }
}
