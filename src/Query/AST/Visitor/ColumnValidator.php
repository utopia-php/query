<?php

namespace Utopia\Query\AST\Visitor;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\Reference\Column;
use Utopia\Query\AST\Reference\Table;
use Utopia\Query\AST\Statement\Select;
use Utopia\Query\AST\Visitor;
use Utopia\Query\Exception;

class ColumnValidator implements Visitor
{
    /** @param string[] $allowedColumns */
    public function __construct(private readonly array $allowedColumns)
    {
    }

    #[\Override]
    public function visitExpression(Expression $expression): Expression
    {
        if ($expression instanceof Column) {
            if (!in_array($expression->name, $this->allowedColumns, true)) {
                throw new Exception("Column '{$expression->name}' is not in the allowed list");
            }
        }
        return $expression;
    }

    #[\Override]
    public function visitTableReference(Table $reference): Table
    {
        return $reference;
    }

    #[\Override]
    public function visitSelect(Select $stmt): Select
    {
        return $stmt;
    }
}
