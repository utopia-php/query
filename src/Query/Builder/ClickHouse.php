<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Exception;
use Utopia\Query\Query;

class ClickHouse extends BaseBuilder
{
    /**
     * @var array<Query>
     */
    protected array $prewhereQueries = [];

    protected bool $useFinal = false;

    protected ?float $sampleFraction = null;

    // ── ClickHouse-specific fluent API ──

    /**
     * Add PREWHERE filters (evaluated before reading all columns — major ClickHouse optimization)
     *
     * @param  array<Query>  $queries
     */
    public function prewhere(array $queries): static
    {
        foreach ($queries as $query) {
            $this->prewhereQueries[] = $query;
        }

        return $this;
    }

    /**
     * Add FINAL keyword after table name (forces merging of data parts)
     */
    public function final(): static
    {
        $this->useFinal = true;

        return $this;
    }

    /**
     * Add SAMPLE clause after table name (approximate query processing)
     */
    public function sample(float $fraction): static
    {
        $this->sampleFraction = $fraction;

        return $this;
    }

    public function reset(): static
    {
        parent::reset();
        $this->prewhereQueries = [];
        $this->useFinal = false;
        $this->sampleFraction = null;

        return $this;
    }

    // ── Dialect-specific compilation ──

    protected function wrapIdentifier(string $identifier): string
    {
        return '`' . $identifier . '`';
    }

    protected function compileRandom(): string
    {
        return 'rand()';
    }

    /**
     * ClickHouse uses the match(column, pattern) function instead of REGEXP
     *
     * @param  array<mixed>  $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return 'match(' . $attribute . ', ?)';
    }

    /**
     * ClickHouse does not support MATCH() AGAINST() full-text search
     *
     * @param  array<mixed>  $values
     *
     * @throws Exception
     */
    protected function compileSearch(string $attribute, array $values, bool $not): string
    {
        throw new Exception('Full-text search (MATCH AGAINST) is not supported in ClickHouse. Use contains() or a custom full-text index instead.');
    }

    // ── Hooks ──

    protected function buildTableClause(): string
    {
        $sql = 'FROM ' . $this->wrapIdentifier($this->table);

        if ($this->useFinal) {
            $sql .= ' FINAL';
        }

        if ($this->sampleFraction !== null) {
            $sql .= ' SAMPLE ' . $this->sampleFraction;
        }

        return $sql;
    }

    /**
     * @param  array<string>  $parts
     * @param  array<string, mixed>  $grouped
     */
    protected function buildAfterJoins(array &$parts, array $grouped): void
    {
        if (! empty($this->prewhereQueries)) {
            $clauses = [];
            foreach ($this->prewhereQueries as $query) {
                $clauses[] = $this->compileFilter($query);
            }
            $parts[] = 'PREWHERE ' . \implode(' AND ', $clauses);
        }
    }
}
