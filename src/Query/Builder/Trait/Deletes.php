<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Builder\Plan;
use Utopia\Query\Query;

trait Deletes
{
    #[\Override]
    public function delete(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = ['DELETE FROM ' . $this->quote($this->table)];

        $this->compileWhereClauses($parts, $grouped);

        $this->compileOrderAndLimit($parts, $grouped);

        return new Plan(\implode(' ', $parts), $this->bindings, executor: $this->executor);
    }
}
