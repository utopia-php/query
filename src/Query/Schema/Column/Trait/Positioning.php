<?php

namespace Utopia\Query\Schema\Column\Trait;

/**
 * Positioning a column relative to another via AFTER. PostgreSQL cannot
 * control column order, and MongoDB documents have no column order.
 */
trait Positioning
{
    public function after(string $column): static
    {
        $this->after = $column;

        return $this;
    }
}
