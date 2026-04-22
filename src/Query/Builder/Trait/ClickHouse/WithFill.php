<?php

namespace Utopia\Query\Builder\Trait\ClickHouse;

use Utopia\Query\Builder\Condition;

trait WithFill
{
    #[\Override]
    public function orderWithFill(string $column, string $direction = 'ASC', mixed $from = null, mixed $to = null, mixed $step = null): static
    {
        $expr = $this->resolveAndWrap($column) . ' ' . \strtoupper($direction) . ' WITH FILL';
        $bindings = [];

        if ($from !== null) {
            $expr .= ' FROM ?';
            $bindings[] = $from;
        }
        if ($to !== null) {
            $expr .= ' TO ?';
            $bindings[] = $to;
        }
        if ($step !== null) {
            $expr .= ' STEP ?';
            $bindings[] = $step;
        }

        $this->rawOrders[] = new Condition($expr, $bindings);

        return $this;
    }
}
