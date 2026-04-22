<?php

namespace Utopia\Query\Builder\Trait\PostgreSQL;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\MergeClause;
use Utopia\Query\Builder\Plan;
use Utopia\Query\Exception\ValidationException;

trait Merge
{
    #[\Override]
    public function mergeInto(string $target): static
    {
        $this->mergeTarget = $target;

        return $this;
    }

    #[\Override]
    public function using(BaseBuilder $source, string $alias): static
    {
        $this->mergeSource = $source;
        $this->mergeSourceAlias = $alias;

        return $this;
    }

    #[\Override]
    public function on(string $condition, mixed ...$bindings): static
    {
        $this->mergeCondition = $condition;
        $this->mergeConditionBindings = \array_values($bindings);

        return $this;
    }

    #[\Override]
    public function whenMatched(string $action, mixed ...$bindings): static
    {
        $this->mergeClauses[] = new MergeClause($action, true, \array_values($bindings));

        return $this;
    }

    #[\Override]
    public function whenNotMatched(string $action, mixed ...$bindings): static
    {
        $this->mergeClauses[] = new MergeClause($action, false, \array_values($bindings));

        return $this;
    }

    #[\Override]
    public function executeMerge(): Plan
    {
        if ($this->mergeTarget === '') {
            throw new ValidationException('No merge target specified. Call mergeInto() before executeMerge().');
        }
        if ($this->mergeSource === null) {
            throw new ValidationException('No merge source specified. Call using() before executeMerge().');
        }
        if ($this->mergeCondition === '') {
            throw new ValidationException('No merge condition specified. Call on() before executeMerge().');
        }

        $this->bindings = [];

        $sourceResult = $this->mergeSource->build();
        $this->addBindings($sourceResult->bindings);

        $sql = 'MERGE INTO ' . $this->quote($this->mergeTarget)
            . ' USING (' . $sourceResult->query . ') AS ' . $this->quote($this->mergeSourceAlias)
            . ' ON ' . $this->mergeCondition;

        foreach ($this->mergeConditionBindings as $binding) {
            $this->addBinding($binding);
        }

        foreach ($this->mergeClauses as $clause) {
            $keyword = $clause->matched ? 'WHEN MATCHED THEN' : 'WHEN NOT MATCHED THEN';
            $sql .= ' ' . $keyword . ' ' . $clause->action;
            $this->addBindings($clause->bindings);
        }

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }
}
