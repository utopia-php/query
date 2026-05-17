<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\ClickHouse\FormattedInsertStatement;
use Utopia\Query\Builder\Feature\BitwiseAggregates;
use Utopia\Query\Builder\Feature\ClickHouse\ApproximateAggregates;
use Utopia\Query\Builder\Feature\ClickHouse\ArrayJoins;
use Utopia\Query\Builder\Feature\ClickHouse\AsofJoins;
use Utopia\Query\Builder\Feature\ClickHouse\LimitBy;
use Utopia\Query\Builder\Feature\ClickHouse\WithFill;
use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Builder\Feature\GroupByModifiers;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\StatisticalAggregates;
use Utopia\Query\Builder\Feature\StringAggregates;
use Utopia\Query\Builder\Feature\TableSampling;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Join\Placement;
use Utopia\Query\Query;
use Utopia\Query\QuotesIdentifiers;

class ClickHouse extends BaseBuilder implements Hints, ConditionalAggregates, TableSampling, FullOuterJoins, StringAggregates, StatisticalAggregates, BitwiseAggregates, LimitBy, ArrayJoins, AsofJoins, WithFill, GroupByModifiers, ApproximateAggregates
{
    use QuotesIdentifiers;
    use Trait\BitwiseAggregates;
    use Trait\ClickHouse\ApproximateAggregates;
    use Trait\ClickHouse\ArrayJoins;
    use Trait\ClickHouse\AsofJoins;
    use Trait\ClickHouse\LimitBy;
    use Trait\ClickHouse\WithFill;
    use Trait\FullOuterJoins;
    use Trait\GroupByModifiers;
    use Trait\StatisticalAggregates;
    use Trait\StringAggregates;

    /**
     * @var array<Query>
     */
    protected array $prewhereQueries = [];

    protected bool $useFinal = false;

    protected ?float $sampleFraction = null;

    /** @var list<string> */
    protected array $hints = [];

    /** @var ?array{count: int, columns: list<string>} */
    protected ?array $limitByClause = null;

    /** @var list<array{type: string, column: string, alias: string}> */
    protected array $arrayJoins = [];

    /** @var list<string> */
    protected array $rawJoinClauses = [];

    protected ?string $insertFormat = null;

    /** @var list<string> */
    protected array $insertFormatColumns = [];

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

    #[\Override]
    public function hint(string $hint): static
    {
        if (!\preg_match('/^[A-Za-z0-9_=., ]+$/', $hint)) {
            throw new ValidationException('Invalid hint: ' . $hint);
        }

        $this->hints[] = $hint;

        return $this;
    }

    /**
     * Declare a ClickHouse FORMAT pragma for the next INSERT.
     *
     * When a format is set, `insert()` emits
     * `INSERT INTO \`t\` (\`col1\`, \`col2\`) FORMAT <name>` with no VALUES.
     * The row payload must be streamed into the HTTP body by the caller.
     * Column names are derived from the most recent `set()` call (values are
     * ignored). Pass `$columns` to declare them explicitly when no `set()`
     * call has been made.
     *
     * @param  list<string>  $columns
     */
    public function insertFormat(string $format, array $columns = []): static
    {
        if (!\preg_match('/^[A-Za-z][A-Za-z0-9]*$/', $format)) {
            throw new ValidationException('Invalid ClickHouse INSERT format: ' . $format);
        }

        $this->insertFormat = $format;
        $this->insertFormatColumns = $columns;

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

    #[\Override]
    public function tablesample(float $percent, string $method = 'BERNOULLI'): static
    {
        return $this->sample($percent / 100);
    }

    #[\Override]
    public function countWhen(string $condition, string $alias = '', mixed ...$bindings): static
    {
        return $this->aggregateFilter('count', null, $condition, $alias, \array_values($bindings));
    }

    #[\Override]
    public function sumWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        return $this->aggregateFilter('sum', $column, $condition, $alias, \array_values($bindings));
    }

    #[\Override]
    public function avgWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        return $this->aggregateFilter('avg', $column, $condition, $alias, \array_values($bindings));
    }

    #[\Override]
    public function minWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        return $this->aggregateFilter('min', $column, $condition, $alias, \array_values($bindings));
    }

    #[\Override]
    public function maxWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        return $this->aggregateFilter('max', $column, $condition, $alias, \array_values($bindings));
    }

    /**
     * Emit a conditional aggregate using ClickHouse's `-If` combinator.
     *
     * @param  list<mixed>  $bindings
     */
    private function aggregateFilter(string $aggregate, ?string $column, string $condition, string $alias, array $bindings): static
    {
        $arguments = $column === null
            ? $condition
            : $this->resolveAndWrap($column) . ', ' . $condition;
        $expr = $aggregate . 'If(' . $arguments . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, $bindings);
    }

    /**
     * ClickHouse has no bare STDDEV function. Emit stddevPop (population
     * standard deviation) which matches the ISO SQL standard semantics.
     */
    #[\Override]
    public function stddev(string $attribute, string $alias = ''): static
    {
        $expr = 'stddevPop(' . $this->resolveAndWrap($attribute) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    /**
     * ClickHouse has no bare VARIANCE function. Emit varPop (population
     * variance) which matches the ISO SQL standard semantics.
     */
    #[\Override]
    public function variance(string $attribute, string $alias = ''): static
    {
        $expr = 'varPop(' . $this->resolveAndWrap($attribute) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    protected function groupConcatExpr(string $column, string $orderBy): string
    {
        return 'arrayStringConcat(groupArray(' . $column . '), ?)';
    }

    #[\Override]
    protected function jsonArrayAggExpr(string $column): string
    {
        return 'toJSONString(groupArray(' . $column . '))';
    }

    #[\Override]
    protected function jsonObjectAggExpr(string $keyColumn, string $valueColumn): string
    {
        return 'toJSONString(CAST((groupArray(' . $keyColumn . '), groupArray(' . $valueColumn . ')) AS Map(String, String)))';
    }

    #[\Override]
    public function withTotals(): static
    {
        $this->groupByModifier = 'WITH TOTALS';

        return $this;
    }

    #[\Override]
    public function withRollup(): static
    {
        $this->groupByModifier = 'WITH ROLLUP';

        return $this;
    }

    #[\Override]
    public function withCube(): static
    {
        $this->groupByModifier = 'WITH CUBE';

        return $this;
    }

    #[\Override]
    public function reset(): static
    {
        parent::reset();
        $this->prewhereQueries = [];
        $this->useFinal = false;
        $this->sampleFraction = null;
        $this->hints = [];
        $this->limitByClause = null;
        $this->arrayJoins = [];
        $this->rawJoinClauses = [];
        $this->insertFormat = null;
        $this->insertFormatColumns = [];
        $this->resetGroupByModifier();

        return $this;
    }

    #[\Override]
    protected function compileRandom(): string
    {
        return 'rand()';
    }

    /**
     * ClickHouse uses the match(column, pattern) function instead of REGEXP
     *
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return 'match(' . $attribute . ', ?)';
    }

    /**
     * ClickHouse uses startsWith()/endsWith() functions instead of LIKE with wildcards.
     *
     * @param  array<mixed>  $values
     */
    #[\Override]
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
    #[\Override]
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
    #[\Override]
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
    #[\Override]
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

    #[\Override]
    public function insert(): Statement
    {
        $format = $this->insertFormat;
        if ($format === null) {
            return parent::insert();
        }

        $this->bindings = [];
        $this->validateTable();

        $columns = !empty($this->insertFormatColumns)
            ? $this->insertFormatColumns
            : (!empty($this->rows) ? \array_keys($this->rows[0]) : []);

        if (empty($columns)) {
            throw new ValidationException('No columns specified for FORMAT INSERT. Pass columns to insertFormat() or call set() before insert().');
        }

        foreach ($columns as $col) {
            if ($col === '') {
                throw new ValidationException('Column names for FORMAT INSERT must be non-empty strings.');
            }
        }

        $wrappedColumns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $columns
        );

        $sql = 'INSERT INTO ' . $this->quote($this->table)
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' FORMAT ' . $format;

        return new FormattedInsertStatement(
            $sql,
            [],
            $columns,
            $format,
            executor: $this->executor,
        );
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

        $parts = [];

        $this->compileWhereClauses($parts);

        if (empty($parts)) {
            throw new ValidationException('ClickHouse UPDATE requires a WHERE clause.');
        }

        $sql = 'ALTER TABLE ' . $this->quote($this->table)
            . ' UPDATE ' . \implode(', ', $assignments)
            . ' ' . \implode(' ', $parts);

        return new Statement($sql, $this->bindings, executor: $this->executor);
    }

    #[\Override]
    public function delete(): Statement
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

        $settings = $this->buildSettingsClause();
        if ($settings !== '') {
            $sql .= ' ' . $settings;
        }

        return new Statement($sql, $this->bindings, executor: $this->executor);
    }

    /**
     * ClickHouse does not support subqueries in JOIN ON conditions.
     * Force all join filter conditions to WHERE placement.
     */
    #[\Override]
    protected function resolveJoinFilterPlacement(Placement $requested, bool $isCrossJoin): Placement
    {
        return Placement::Where;
    }

    #[\Override]
    protected function buildTableClause(): string
    {
        $fromSub = $this->fromSubquery;
        if ($fromSub !== null) {
            $subResult = $fromSub->subquery->build();
            $this->addBindings($subResult->bindings);

            return 'FROM (' . $subResult->query . ') AS ' . $this->quote($fromSub->alias);
        }

        $sql = 'FROM ' . $this->quote($this->table);

        if ($this->useFinal) {
            $sql .= ' FINAL';
        }

        if ($this->sampleFraction !== null) {
            $sql .= ' SAMPLE ' . \sprintf('%.10g', $this->sampleFraction);
        }

        if ($this->alias !== '') {
            $sql .= ' AS ' . $this->quote($this->alias);
        }

        return $sql;
    }

    /**
     * Emit PREWHERE (before reading all columns), ARRAY JOIN, and raw ASOF
     * joins between the JOIN section and WHERE. These are structural
     * ClickHouse clauses that do not carry bindings.
     */
    #[\Override]
    protected function buildAfterJoinsClause(ParsedQuery $grouped): string
    {
        $parts = [];

        if (! empty($this->arrayJoins)) {
            $arrayJoinParts = [];
            foreach ($this->arrayJoins as $aj) {
                $clause = $aj['type'] . ' ' . $this->resolveAndWrap($aj['column']);
                if ($aj['alias'] !== '') {
                    $clause .= ' AS ' . $this->quote($aj['alias']);
                }
                $arrayJoinParts[] = $clause;
            }
            $parts[] = \implode(' ', $arrayJoinParts);
        }

        if (! empty($this->rawJoinClauses)) {
            $parts[] = \implode(' ', $this->rawJoinClauses);
        }

        if (! empty($this->prewhereQueries)) {
            $clauses = [];
            foreach ($this->prewhereQueries as $query) {
                $clauses[] = $this->compileFilter($query);
            }
            $parts[] = 'PREWHERE ' . \implode(' AND ', $clauses);
        }

        return \implode(' ', $parts);
    }

    /**
     * Emit the ClickHouse GROUP BY modifier (WITH TOTALS / WITH ROLLUP /
     * WITH CUBE) between GROUP BY and HAVING.
     */
    #[\Override]
    protected function buildAfterGroupByClause(): string
    {
        return $this->groupByModifier ?? '';
    }

    /**
     * Emit LIMIT BY between ORDER BY and LIMIT. The count binding is added
     * here so ordering is naturally correct: LIMIT BY binding precedes the
     * outer LIMIT binding emitted by the parent.
     */
    #[\Override]
    protected function buildAfterOrderByClause(): string
    {
        if ($this->limitByClause === null) {
            return '';
        }

        $cols = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $this->limitByClause['columns']
        );

        $this->addBinding($this->limitByClause['count']);

        return 'LIMIT ? BY ' . \implode(', ', $cols);
    }

    /**
     * Emit the trailing SETTINGS fragment from registered hints.
     */
    #[\Override]
    protected function buildSettingsClause(): string
    {
        if (empty($this->hints)) {
            return '';
        }

        return 'SETTINGS ' . \implode(', ', $this->hints);
    }
}
