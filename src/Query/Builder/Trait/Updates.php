<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Builder\Statement;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;

trait Updates
{
    /**
     * @param  list<mixed>  $bindings
     */
    #[\Override]
    public function setRaw(string $column, string $expression, array $bindings = []): static
    {
        $this->rawSets[$column] = $expression;
        $this->rawSetBindings[$column] = $bindings;

        return $this;
    }

    #[\Override]
    public function update(): Statement
    {
        $this->bindings = [];
        $this->validateTable();

        $assignments = $this->compileAssignments();

        if (empty($assignments)) {
            throw new ValidationException('No assignments for UPDATE. Call set() or setRaw() before update().');
        }

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = ['UPDATE ' . $this->quote($this->table) . ' SET ' . \implode(', ', $assignments)];

        $this->compileWhereClauses($parts, $grouped);

        $this->compileOrderAndLimit($parts, $grouped);

        return new Statement(\implode(' ', $parts), $this->bindings, executor: $this->executor);
    }
}
