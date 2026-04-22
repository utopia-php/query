<?php

namespace Utopia\Query\Builder\Trait;

trait ConditionalAggregates
{
    #[\Override]
    public function countWhen(string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'COUNT(CASE WHEN ' . $condition . ' THEN 1 END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function sumWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'SUM(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function avgWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'AVG(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function minWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MIN(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function maxWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MAX(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }
}
