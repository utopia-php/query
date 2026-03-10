<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Join\Placement;
use Utopia\Query\Query;
use Utopia\Query\QuotesIdentifiers;

class ClickHouse extends BaseBuilder implements Hints
{
    use QuotesIdentifiers;
    /**
     * @var array<Query>
     */
    protected array $prewhereQueries = [];

    protected bool $useFinal = false;

    protected ?float $sampleFraction = null;

    /** @var list<string> */
    protected array $hints = [];

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
        if ($fraction <= 0.0 || $fraction >= 1.0) {
            throw new ValidationException('Sample fraction must be between 0 and 1 exclusive');
        }

        $this->sampleFraction = $fraction;

        return $this;
    }

    public function hint(string $hint): static
    {
        if (!\preg_match('/^[A-Za-z0-9_=., ]+$/', $hint)) {
            throw new ValidationException('Invalid hint: ' . $hint);
        }

        $this->hints[] = $hint;

        return $this;
    }

    /**
     * @param  array<string, string>  $settings
     */
    public function settings(array $settings): static
    {
        foreach ($settings as $key => $value) {
            if (!\preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $key)) {
                throw new ValidationException('Invalid ClickHouse setting key: ' . $key);
            }

            $value = (string) $value;

            if (!\preg_match('/^[a-zA-Z0-9_.]+$/', $value)) {
                throw new ValidationException('Invalid ClickHouse setting value: ' . $value);
            }

            $this->hints[] = $key . '=' . $value;
        }

        return $this;
    }

    public function reset(): static
    {
        parent::reset();
        $this->prewhereQueries = [];
        $this->useFinal = false;
        $this->sampleFraction = null;
        $this->hints = [];

        return $this;
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
     * @throws UnsupportedException
     */
    protected function compileSearch(string $attribute, array $values, bool $not): string
    {
        throw new UnsupportedException('Full-text search (MATCH AGAINST) is not supported in ClickHouse. Use contains() or a custom full-text index instead.');
    }

    /**
     * ClickHouse uses startsWith()/endsWith() functions instead of LIKE with wildcards.
     *
     * @param  array<mixed>  $values
     */
    protected function compileLike(string $attribute, array $values, string $prefix, string $suffix, bool $not): string
    {
        /** @var string $rawVal */
        $rawVal = $values[0];

        // startsWith: prefix='', suffix='%'
        if ($prefix === '' && $suffix === '%') {
            $func = $not ? 'NOT startsWith' : 'startsWith';
            $this->addBinding($rawVal);

            return $func . '(' . $attribute . ', ?)';
        }

        // endsWith: prefix='%', suffix=''
        if ($prefix === '%' && $suffix === '') {
            $func = $not ? 'NOT endsWith' : 'endsWith';
            $this->addBinding($rawVal);

            return $func . '(' . $attribute . ', ?)';
        }

        // Fallback for any other LIKE pattern (should not occur in practice)
        $val = $this->escapeLikeValue($rawVal);
        $this->addBinding($prefix . $val . $suffix);
        $keyword = $not ? 'NOT LIKE' : 'LIKE';

        return $attribute . ' ' . $keyword . ' ?';
    }

    /**
     * ClickHouse uses position() instead of LIKE '%val%' for substring matching.
     *
     * @param  array<mixed>  $values
     */
    protected function compileContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding($values[0]);

            return 'position(' . $attribute . ', ?) > 0';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding($value);
            $parts[] = 'position(' . $attribute . ', ?) > 0';
        }

        return '(' . \implode(' OR ', $parts) . ')';
    }

    /**
     * ClickHouse uses position() instead of LIKE '%val%' for substring matching (all values).
     *
     * @param  array<mixed>  $values
     */
    protected function compileContainsAll(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        $parts = [];
        foreach ($values as $value) {
            $this->addBinding($value);
            $parts[] = 'position(' . $attribute . ', ?) > 0';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * ClickHouse uses position() = 0 instead of NOT LIKE '%val%'.
     *
     * @param  array<mixed>  $values
     */
    protected function compileNotContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding($values[0]);

            return 'position(' . $attribute . ', ?) = 0';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding($value);
            $parts[] = 'position(' . $attribute . ', ?) = 0';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    public function update(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $assignments = [];

        if (! empty($this->pendingRows)) {
            foreach ($this->pendingRows[0] as $col => $value) {
                $assignments[] = $this->resolveAndWrap($col) . ' = ?';
                $this->addBinding($value);
            }
        }

        foreach ($this->rawSets as $col => $expression) {
            $assignments[] = $this->resolveAndWrap($col) . ' = ' . $expression;
            if (isset($this->rawSetBindings[$col])) {
                foreach ($this->rawSetBindings[$col] as $binding) {
                    $this->addBinding($binding);
                }
            }
        }

        foreach ($this->caseSets as $col => $caseData) {
            $assignments[] = $this->resolveAndWrap($col) . ' = ' . $caseData['sql'];
            foreach ($caseData['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        if (empty($assignments)) {
            throw new ValidationException('No assignments for UPDATE. Call set() or setRaw() before update().');
        }

        $parts = [];

        $this->compileWhereClauses($parts);

        if (empty($parts)) {
            throw new ValidationException('ClickHouse UPDATE requires a WHERE clause.');
        }

        $sql = 'ALTER TABLE ' . $this->quote($this->table)
            . ' UPDATE ' . \implode(', ', $assignments)
            . ' ' . \implode(' ', $parts);

        return new BuildResult($sql, $this->bindings);
    }

    public function delete(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $parts = [];

        $this->compileWhereClauses($parts);

        if (empty($parts)) {
            throw new ValidationException('ClickHouse DELETE requires a WHERE clause.');
        }

        $sql = 'ALTER TABLE ' . $this->quote($this->table)
            . ' DELETE ' . \implode(' ', $parts);

        return new BuildResult($sql, $this->bindings);
    }

    /**
     * ClickHouse does not support subqueries in JOIN ON conditions.
     * Force all join filter conditions to WHERE placement.
     */
    protected function resolveJoinFilterPlacement(Placement $requested, bool $isCrossJoin): Placement
    {
        return Placement::Where;
    }

    public function build(): BuildResult
    {
        $result = parent::build();

        if (! empty($this->hints)) {
            $settingsStr = \implode(', ', $this->hints);

            return new BuildResult($result->query . ' SETTINGS ' . $settingsStr, $result->bindings);
        }

        return $result;
    }

    protected function buildTableClause(): string
    {
        $fromSub = $this->fromSubquery;
        if ($fromSub !== null) {
            $subResult = $fromSub['subquery']->build();
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }

            return 'FROM (' . $subResult->query . ') AS ' . $this->quote($fromSub['alias']);
        }

        $sql = 'FROM ' . $this->quote($this->table);

        if ($this->useFinal) {
            $sql .= ' FINAL';
        }

        if ($this->sampleFraction !== null) {
            $sql .= ' SAMPLE ' . \sprintf('%.10g', $this->sampleFraction);
        }

        if ($this->tableAlias !== '') {
            $sql .= ' AS ' . $this->quote($this->tableAlias);
        }

        return $sql;
    }

    /**
     * @param  array<string>  $parts
     */
    protected function buildAfterJoins(array &$parts, GroupedQueries $grouped): void
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
