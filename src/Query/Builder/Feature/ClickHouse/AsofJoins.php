<?php

namespace Utopia\Query\Builder\Feature\ClickHouse;

interface AsofJoins
{
    /**
     * ClickHouse ASOF JOIN — joins on the closest matching row by the right column.
     */
    public function asofJoin(string $table, string $left, string $right, string $alias = ''): static;

    /**
     * ClickHouse ASOF LEFT JOIN — left join variant of ASOF JOIN.
     */
    public function asofLeftJoin(string $table, string $left, string $right, string $alias = ''): static;
}
