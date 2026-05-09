<?php

namespace Utopia\Query\Schema\Column;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\Forwarder;
use Utopia\Query\Schema\Table;

/**
 * @property Table\MySQL $table
 */
class MySQL extends Column
{
    use Forwarder\MySQL;

    /**
     * Mark this column as primary, or declare a composite primary key on the
     * parent table when called with an array of column names.
     *
     * @param  list<string>  $columns
     *
     * @phpstan-return ($columns is array{} ? static : Table\MySQL)
     */
    public function primary(array $columns = []): static|Table
    {
        if ($columns === []) {
            $this->isPrimary = true;

            return $this;
        }

        return $this->table->primary($columns);
    }

    /**
     * Single-arg form sets a column-level CHECK; two-arg form declares a
     * named table-level CHECK on the parent table.
     *
     * @phpstan-return ($expression is null ? static : Table\MySQL)
     */
    public function check(string $expressionOrName, ?string $expression = null): static|Table
    {
        if ($expression === null) {
            $this->checkExpression = $expressionOrName;

            return $this;
        }

        return $this->table->check($expressionOrName, $expression);
    }
}
