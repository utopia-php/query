<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\ClickHouse\AsofOperator;
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

    protected ?string $groupByModifier = null;

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
        $expr = 'countIf(' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function sumWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'sumIf(' . $this->resolveAndWrap($column) . ', ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function avgWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'avgIf(' . $this->resolveAndWrap($column) . ', ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function minWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'minIf(' . $this->resolveAndWrap($column) . ', ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function maxWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'maxIf(' . $this->resolveAndWrap($column) . ', ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function fullOuterJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::fullOuterJoin($table, $left, $right, $operator, $alias);

        return $this;
    }

    #[\Override]
    public function groupConcat(string $column, string $separator = ',', string $alias = '', ?array $orderBy = null): static
    {
        $col = $this->resolveAndWrap($column);
        $expr = 'arrayStringConcat(groupArray(' . $col . '), ?)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, [$separator]);
    }

    #[\Override]
    public function jsonArrayAgg(string $column, string $alias = ''): static
    {
        $expr = 'toJSONString(groupArray(' . $this->resolveAndWrap($column) . '))';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function jsonObjectAgg(string $keyColumn, string $valueColumn, string $alias = ''): static
    {
        $expr = 'toJSONString(CAST((groupArray(' . $this->resolveAndWrap($keyColumn) . '), groupArray(' . $this->resolveAndWrap($valueColumn) . ')) AS Map(String, String)))';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function stddev(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::stddev($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function stddevPop(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::stddevPop($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function stddevSamp(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::stddevSamp($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function variance(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::variance($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function varPop(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::varPop($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function varSamp(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::varSamp($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function bitAnd(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::bitAnd($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function bitOr(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::bitOr($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function bitXor(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::bitXor($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function limitBy(int $count, array $columns): static
    {
        $this->limitByClause = ['count' => $count, 'columns' => $columns];

        return $this;
    }

    #[\Override]
    public function arrayJoin(string $column, string $alias = ''): static
    {
        $this->arrayJoins[] = ['type' => 'ARRAY JOIN', 'column' => $column, 'alias' => $alias];

        return $this;
    }

    #[\Override]
    public function leftArrayJoin(string $column, string $alias = ''): static
    {
        $this->arrayJoins[] = ['type' => 'LEFT ARRAY JOIN', 'column' => $column, 'alias' => $alias];

        return $this;
    }

    /**
     * @param  array<string, string>  $equiPairs
     */
    #[\Override]
    public function asofJoin(
        string $table,
        array $equiPairs,
        string $leftInequality,
        AsofOperator $operator,
        string $rightInequality,
        string $alias = '',
    ): static {
        $this->rawJoinClauses[] = $this->buildAsofJoin(
            keyword: 'ASOF JOIN',
            table: $table,
            equiPairs: $equiPairs,
            leftInequality: $leftInequality,
            operator: $operator,
            rightInequality: $rightInequality,
            alias: $alias,
        );

        return $this;
    }

    /**
     * @param  array<string, string>  $equiPairs
     */
    #[\Override]
    public function asofLeftJoin(
        string $table,
        array $equiPairs,
        string $leftInequality,
        AsofOperator $operator,
        string $rightInequality,
        string $alias = '',
    ): static {
        $this->rawJoinClauses[] = $this->buildAsofJoin(
            keyword: 'ASOF LEFT JOIN',
            table: $table,
            equiPairs: $equiPairs,
            leftInequality: $leftInequality,
            operator: $operator,
            rightInequality: $rightInequality,
            alias: $alias,
        );

        return $this;
    }

    /**
     * @param  array<string, string>  $equiPairs
     */
    private function buildAsofJoin(
        string $keyword,
        string $table,
        array $equiPairs,
        string $leftInequality,
        AsofOperator $operator,
        string $rightInequality,
        string $alias,
    ): string {
        if ($equiPairs === []) {
            throw new ValidationException('ASOF JOIN requires at least one equi-join column pair.');
        }

        $tableExpr = $this->quote($table);
        if ($alias !== '') {
            $tableExpr .= ' AS ' . $this->quote($alias);
        }

        $conditions = [];
        foreach ($equiPairs as $left => $right) {
            $conditions[] = $this->resolveAndWrap($left) . ' = ' . $this->resolveAndWrap($right);
        }
        $conditions[] = $this->resolveAndWrap($leftInequality) . ' ' . $operator->value . ' ' . $this->resolveAndWrap($rightInequality);

        return $keyword . ' ' . $tableExpr . ' ON ' . \implode(' AND ', $conditions);
    }

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
    public function quantile(float $level, string $column, string $alias = ''): static
    {
        $expr = 'quantile(' . $level . ')(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function quantiles(array $levels, string $column, string $alias = ''): static
    {
        if ($levels === []) {
            throw new ValidationException('quantiles() requires at least one level.');
        }

        foreach ($levels as $level) {
            if ($level < 0.0 || $level > 1.0) {
                throw new ValidationException('quantiles() levels must be in the range [0, 1].');
            }
        }

        $expr = 'quantiles(' . \implode(', ', $levels) . ')(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function quantileExact(float $level, string $column, string $alias = ''): static
    {
        $expr = 'quantileExact(' . $level . ')(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function median(string $column, string $alias = ''): static
    {
        $expr = 'median(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function uniq(string $column, string $alias = ''): static
    {
        $expr = 'uniq(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function uniqExact(string $column, string $alias = ''): static
    {
        $expr = 'uniqExact(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function uniqCombined(string $column, string $alias = ''): static
    {
        $expr = 'uniqCombined(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function argMin(string $valueColumn, string $argColumn, string $alias = ''): static
    {
        $expr = 'argMin(' . $this->resolveAndWrap($valueColumn) . ', ' . $this->resolveAndWrap($argColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function argMax(string $valueColumn, string $argColumn, string $alias = ''): static
    {
        $expr = 'argMax(' . $this->resolveAndWrap($valueColumn) . ', ' . $this->resolveAndWrap($argColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function topK(int $k, string $column, string $alias = ''): static
    {
        $expr = 'topK(' . $k . ')(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function topKWeighted(int $k, string $column, string $weightColumn, string $alias = ''): static
    {
        $expr = 'topKWeighted(' . $k . ')(' . $this->resolveAndWrap($column) . ', ' . $this->resolveAndWrap($weightColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function anyValue(string $column, string $alias = ''): static
    {
        $expr = 'any(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function anyLastValue(string $column, string $alias = ''): static
    {
        $expr = 'anyLast(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function groupUniqArray(string $column, string $alias = ''): static
    {
        $expr = 'groupUniqArray(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function groupArrayMovingAvg(string $column, string $alias = ''): static
    {
        $expr = 'groupArrayMovingAvg(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function groupArrayMovingSum(string $column, string $alias = ''): static
    {
        $expr = 'groupArrayMovingSum(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
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
        $this->groupByModifier = null;

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
    public function update(): Plan
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

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }

    #[\Override]
    public function delete(): Plan
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

        return new Plan($sql, $this->bindings, executor: $this->executor);
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
    protected function buildAfterJoinsClause(GroupedQueries $grouped): string
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
