<?php

namespace Utopia\Query;

use Closure;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\Case\Expression as CaseExpression;
use Utopia\Query\Builder\Feature;
use Utopia\Query\Builder\GroupedQueries;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\UnionClause;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Attribute;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

abstract class Builder implements
    Compiler,
    Feature\Selects,
    Feature\Aggregates,
    Feature\Joins,
    Feature\Unions,
    Feature\CTEs,
    Feature\Inserts,
    Feature\Updates,
    Feature\Deletes,
    Feature\Hooks,
    Feature\Windows
{
    protected string $table = '';

    protected string $tableAlias = '';

    /**
     * @var array<Query>
     */
    protected array $pendingQueries = [];

    /**
     * @var list<mixed>
     */
    protected array $bindings = [];

    /**
     * @var list<UnionClause>
     */
    protected array $unions = [];

    /** @var list<Filter> */
    protected array $filterHooks = [];

    /** @var list<Attribute> */
    protected array $attributeHooks = [];

    /** @var list<JoinFilter> */
    protected array $joinFilterHooks = [];

    /** @var list<array<string, mixed>> */
    protected array $pendingRows = [];

    /** @var array<string, string> */
    protected array $rawSets = [];

    /** @var array<string, list<mixed>> */
    protected array $rawSetBindings = [];

    protected ?string $lockMode = null;

    protected ?Builder $insertSelectSource = null;

    /** @var list<string> */
    protected array $insertSelectColumns = [];

    /** @var list<array{name: string, query: string, bindings: list<mixed>, recursive: bool}> */
    protected array $ctes = [];

    /** @var list<array{expression: string, bindings: list<mixed>}> */
    protected array $rawSelects = [];

    /** @var list<array{function: string, alias: string, partitionBy: ?list<string>, orderBy: ?list<string>}> */
    protected array $windowSelects = [];

    /** @var list<array{sql: string, bindings: list<mixed>}> */
    protected array $caseSelects = [];

    /** @var array<string, array{sql: string, bindings: list<mixed>}> */
    protected array $caseSets = [];

    /** @var string[] */
    protected array $conflictKeys = [];

    /** @var string[] */
    protected array $conflictUpdateColumns = [];

    /** @var array<string, string> */
    protected array $conflictRawSets = [];

    /** @var array<string, list<mixed>> */
    protected array $conflictRawSetBindings = [];

    /** @var list<array{column: string, subquery: Builder, not: bool}> */
    protected array $whereInSubqueries = [];

    /** @var list<array{subquery: Builder, alias: string}> */
    protected array $subSelects = [];

    /** @var ?array{subquery: Builder, alias: string} */
    protected ?array $fromSubquery = null;

    /** @var list<array{expression: string, bindings: list<mixed>}> */
    protected array $rawOrders = [];

    /** @var list<array{expression: string, bindings: list<mixed>}> */
    protected array $rawGroups = [];

    /** @var list<array{expression: string, bindings: list<mixed>}> */
    protected array $rawHavings = [];

    /** @var array<int, JoinBuilder> */
    protected array $joinBuilders = [];

    /** @var list<array{subquery: Builder, not: bool}> */
    protected array $existsSubqueries = [];

    abstract protected function quote(string $identifier): string;

    /**
     * Compile a random ordering expression (e.g. RAND() or rand())
     */
    abstract protected function compileRandom(): string;

    /**
     * Compile a regex filter
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileRegex(string $attribute, array $values): string;

    /**
     * Compile a full-text search filter
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileSearch(string $attribute, array $values, bool $not): string;

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

        if ($this->tableAlias !== '') {
            $sql .= ' AS ' . $this->quote($this->tableAlias);
        }

        return $sql;
    }

    /**
     * Hook called after JOIN clauses, before WHERE. Override to inject e.g. PREWHERE.
     *
     * @param  array<string>  $parts
     */
    protected function buildAfterJoins(array &$parts, GroupedQueries $grouped): void
    {
        // no-op by default
    }

    public function from(string $table, string $alias = ''): static
    {
        $this->table = $table;
        $this->tableAlias = $alias;
        $this->fromSubquery = null;

        return $this;
    }

    public function into(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    /**
     * @param  array<string, mixed>  $row
     */
    public function set(array $row): static
    {
        $this->pendingRows[] = $row;

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function setRaw(string $column, string $expression, array $bindings = []): static
    {
        $this->rawSets[$column] = $expression;
        $this->rawSetBindings[$column] = $bindings;

        return $this;
    }

    /**
     * @param  string[]  $keys
     * @param  string[]  $updateColumns
     */
    public function onConflict(array $keys, array $updateColumns): static
    {
        $this->conflictKeys = $keys;
        $this->conflictUpdateColumns = $updateColumns;

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function conflictSetRaw(string $column, string $expression, array $bindings = []): static
    {
        $this->conflictRawSets[$column] = $expression;
        $this->conflictRawSetBindings[$column] = $bindings;

        return $this;
    }

    public function filterWhereIn(string $column, Builder $subquery): static
    {
        $this->whereInSubqueries[] = ['column' => $column, 'subquery' => $subquery, 'not' => false];

        return $this;
    }

    public function filterWhereNotIn(string $column, Builder $subquery): static
    {
        $this->whereInSubqueries[] = ['column' => $column, 'subquery' => $subquery, 'not' => true];

        return $this;
    }

    public function selectSub(Builder $subquery, string $alias): static
    {
        $this->subSelects[] = ['subquery' => $subquery, 'alias' => $alias];

        return $this;
    }

    public function fromSub(Builder $subquery, string $alias): static
    {
        $this->fromSubquery = ['subquery' => $subquery, 'alias' => $alias];
        $this->table = '';

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function orderByRaw(string $expression, array $bindings = []): static
    {
        $this->rawOrders[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function groupByRaw(string $expression, array $bindings = []): static
    {
        $this->rawGroups[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function havingRaw(string $expression, array $bindings = []): static
    {
        $this->rawHavings[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    public function countDistinct(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::countDistinct($attribute, $alias);

        return $this;
    }

    /**
     * @param  \Closure(JoinBuilder): void  $callback
     */
    public function joinWhere(string $table, Closure $callback, string $type = 'JOIN', string $alias = ''): static
    {
        $joinBuilder = new JoinBuilder();
        $callback($joinBuilder);

        $method = match ($type) {
            'LEFT JOIN' => Method::LeftJoin,
            'RIGHT JOIN' => Method::RightJoin,
            'CROSS JOIN' => Method::CrossJoin,
            default => Method::Join,
        };

        if ($method === Method::CrossJoin) {
            $this->pendingQueries[] = new Query($method, $table, $alias !== '' ? [$alias] : []);
        } else {
            // Use placeholder values; the JoinBuilder will handle the ON clause
            $values = ['', '=', ''];
            if ($alias !== '') {
                $values[] = $alias;
            }
            $this->pendingQueries[] = new Query($method, $table, $values);
        }

        $index = \count($this->pendingQueries) - 1;
        $this->joinBuilders[$index] = $joinBuilder;

        return $this;
    }

    public function filterExists(Builder $subquery): static
    {
        $this->existsSubqueries[] = ['subquery' => $subquery, 'not' => false];

        return $this;
    }

    public function filterNotExists(Builder $subquery): static
    {
        $this->existsSubqueries[] = ['subquery' => $subquery, 'not' => true];

        return $this;
    }

    public function explain(bool $analyze = false): BuildResult
    {
        $result = $this->build();
        $prefix = $analyze ? 'EXPLAIN ANALYZE ' : 'EXPLAIN ';

        return new BuildResult($prefix . $result->query, $result->bindings);
    }

    /**
     * @param  array<string>  $columns
     */
    public function select(array $columns): static
    {
        $this->pendingQueries[] = Query::select($columns);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function filter(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    public function sortAsc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderAsc($attribute);

        return $this;
    }

    public function sortDesc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderDesc($attribute);

        return $this;
    }

    public function sortRandom(): static
    {
        $this->pendingQueries[] = Query::orderRandom();

        return $this;
    }

    public function limit(int $value): static
    {
        $this->pendingQueries[] = Query::limit($value);

        return $this;
    }

    public function offset(int $value): static
    {
        $this->pendingQueries[] = Query::offset($value);

        return $this;
    }

    public function cursorAfter(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorAfter($value);

        return $this;
    }

    public function cursorBefore(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorBefore($value);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function queries(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    public function addHook(Hook $hook): static
    {
        if ($hook instanceof Filter) {
            $this->filterHooks[] = $hook;
        }
        if ($hook instanceof Attribute) {
            $this->attributeHooks[] = $hook;
        }
        if ($hook instanceof JoinFilter) {
            $this->joinFilterHooks[] = $hook;
        }

        return $this;
    }

    public function count(string $attribute = '*', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::count($attribute, $alias);

        return $this;
    }

    public function sum(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::sum($attribute, $alias);

        return $this;
    }

    public function avg(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::avg($attribute, $alias);

        return $this;
    }

    public function min(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::min($attribute, $alias);

        return $this;
    }

    public function max(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::max($attribute, $alias);

        return $this;
    }

    /**
     * @param  array<string>  $columns
     */
    public function groupBy(array $columns): static
    {
        $this->pendingQueries[] = Query::groupBy($columns);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function having(array $queries): static
    {
        $this->pendingQueries[] = Query::having($queries);

        return $this;
    }

    public function distinct(): static
    {
        $this->pendingQueries[] = Query::distinct();

        return $this;
    }

    public function join(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::join($table, $left, $right, $operator, $alias);

        return $this;
    }

    public function leftJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::leftJoin($table, $left, $right, $operator, $alias);

        return $this;
    }

    public function rightJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::rightJoin($table, $left, $right, $operator, $alias);

        return $this;
    }

    public function crossJoin(string $table, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::crossJoin($table, $alias);

        return $this;
    }

    public function union(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('UNION', $result->query, $result->bindings);

        return $this;
    }

    public function unionAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('UNION ALL', $result->query, $result->bindings);

        return $this;
    }

    public function intersect(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('INTERSECT', $result->query, $result->bindings);

        return $this;
    }

    public function intersectAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('INTERSECT ALL', $result->query, $result->bindings);

        return $this;
    }

    public function except(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('EXCEPT', $result->query, $result->bindings);

        return $this;
    }

    public function exceptAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause('EXCEPT ALL', $result->query, $result->bindings);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    public function fromSelect(array $columns, self $source): static
    {
        $this->insertSelectColumns = $columns;
        $this->insertSelectSource = $source;

        return $this;
    }

    public function insertSelect(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        if ($this->insertSelectSource === null) {
            throw new ValidationException('No SELECT source specified. Call fromSelect() before insertSelect().');
        }

        if (empty($this->insertSelectColumns)) {
            throw new ValidationException('No columns specified. Call fromSelect() with columns before insertSelect().');
        }

        $wrappedColumns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $this->insertSelectColumns
        );

        $sourceResult = $this->insertSelectSource->build();

        $sql = 'INSERT INTO ' . $this->quote($this->table)
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' ' . $sourceResult->query;

        foreach ($sourceResult->bindings as $binding) {
            $this->addBinding($binding);
        }

        return new BuildResult($sql, $this->bindings);
    }

    public function with(string $name, self $query): static
    {
        $result = $query->build();
        $this->ctes[] = ['name' => $name, 'query' => $result->query, 'bindings' => $result->bindings, 'recursive' => false];

        return $this;
    }

    public function withRecursive(string $name, self $query): static
    {
        $result = $query->build();
        $this->ctes[] = ['name' => $name, 'query' => $result->query, 'bindings' => $result->bindings, 'recursive' => true];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function selectRaw(string $expression, array $bindings = []): static
    {
        $this->rawSelects[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    public function selectWindow(string $function, string $alias, ?array $partitionBy = null, ?array $orderBy = null): static
    {
        $this->windowSelects[] = [
            'function' => $function,
            'alias' => $alias,
            'partitionBy' => $partitionBy,
            'orderBy' => $orderBy,
        ];

        return $this;
    }

    public function selectCase(CaseExpression $case): static
    {
        $this->caseSelects[] = ['sql' => $case->sql, 'bindings' => $case->bindings];

        return $this;
    }

    public function setCase(string $column, CaseExpression $case): static
    {
        $this->caseSets[$column] = ['sql' => $case->sql, 'bindings' => $case->bindings];

        return $this;
    }

    public function when(bool $condition, Closure $callback): static
    {
        if ($condition) {
            $callback($this);
        }

        return $this;
    }

    public function page(int $page, int $perPage = 25): static
    {
        if ($page < 1) {
            throw new ValidationException('Page must be >= 1, got ' . $page);
        }
        if ($perPage < 1) {
            throw new ValidationException('Per page must be >= 1, got ' . $perPage);
        }

        $this->pendingQueries[] = Query::limit($perPage);
        $this->pendingQueries[] = Query::offset(($page - 1) * $perPage);

        return $this;
    }

    public function toRawSql(): string
    {
        $result = $this->build();
        $sql = $result->query;
        $offset = 0;

        foreach ($result->bindings as $binding) {
            if (\is_string($binding)) {
                $value = "'" . str_replace("'", "''", $binding) . "'";
            } elseif (\is_int($binding) || \is_float($binding)) {
                $value = (string) $binding;
            } elseif (\is_bool($binding)) {
                $value = $binding ? '1' : '0';
            } else {
                $value = 'NULL';
            }

            $pos = \strpos($sql, '?', $offset);
            if ($pos !== false) {
                $sql = \substr_replace($sql, $value, $pos, 1);
                $offset = $pos + \strlen($value);
            }
        }

        return $sql;
    }

    public function build(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        // CTE prefix
        $ctePrefix = '';
        if (! empty($this->ctes)) {
            $hasRecursive = false;
            $cteParts = [];
            foreach ($this->ctes as $cte) {
                if ($cte['recursive']) {
                    $hasRecursive = true;
                }
                foreach ($cte['bindings'] as $binding) {
                    $this->addBinding($binding);
                }
                $cteParts[] = $this->quote($cte['name']) . ' AS (' . $cte['query'] . ')';
            }
            $keyword = $hasRecursive ? 'WITH RECURSIVE' : 'WITH';
            $ctePrefix = $keyword . ' ' . \implode(', ', $cteParts) . ' ';
        }

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = [];

        // SELECT
        $selectParts = [];

        if (! empty($grouped->aggregations)) {
            foreach ($grouped->aggregations as $agg) {
                $selectParts[] = $this->compileAggregate($agg);
            }
        }

        if (! empty($grouped->selections)) {
            $selectParts[] = $this->compileSelect($grouped->selections[0]);
        }

        // Sub-selects
        foreach ($this->subSelects as $subSelect) {
            $subResult = $subSelect['subquery']->build();
            $selectParts[] = '(' . $subResult->query . ') AS ' . $this->quote($subSelect['alias']);
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // Raw selects
        foreach ($this->rawSelects as $rawSelect) {
            $selectParts[] = $rawSelect['expression'];
            foreach ($rawSelect['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        // Window function selects
        foreach ($this->windowSelects as $win) {
            $overParts = [];

            if ($win['partitionBy'] !== null && $win['partitionBy'] !== []) {
                $partCols = \array_map(
                    fn (string $col): string => $this->resolveAndWrap($col),
                    $win['partitionBy']
                );
                $overParts[] = 'PARTITION BY ' . \implode(', ', $partCols);
            }

            if ($win['orderBy'] !== null && $win['orderBy'] !== []) {
                $orderCols = [];
                foreach ($win['orderBy'] as $col) {
                    if (\str_starts_with($col, '-')) {
                        $orderCols[] = $this->resolveAndWrap(\substr($col, 1)) . ' DESC';
                    } else {
                        $orderCols[] = $this->resolveAndWrap($col) . ' ASC';
                    }
                }
                $overParts[] = 'ORDER BY ' . \implode(', ', $orderCols);
            }

            $overClause = \implode(' ', $overParts);
            $selectParts[] = $win['function'] . ' OVER (' . $overClause . ') AS ' . $this->quote($win['alias']);
        }

        // CASE selects
        foreach ($this->caseSelects as $caseSelect) {
            $selectParts[] = $caseSelect['sql'];
            foreach ($caseSelect['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        $selectSQL = ! empty($selectParts) ? \implode(', ', $selectParts) : '*';

        $selectKeyword = $grouped->distinct ? 'SELECT DISTINCT' : 'SELECT';
        $parts[] = $selectKeyword . ' ' . $selectSQL;

        // FROM
        $parts[] = $this->buildTableClause();

        // JOINS
        $joinFilterWhereClauses = [];
        if (! empty($grouped->joins)) {
            // Build a map from pending query index to join index for JoinBuilder lookup
            $joinQueryIndices = [];
            foreach ($this->pendingQueries as $idx => $pq) {
                if ($pq->getMethod()->isJoin()) {
                    $joinQueryIndices[] = $idx;
                }
            }

            foreach ($grouped->joins as $joinIdx => $joinQuery) {
                $pendingIdx = $joinQueryIndices[$joinIdx] ?? -1;
                $joinBuilder = $this->joinBuilders[$pendingIdx] ?? null;

                if ($joinBuilder !== null) {
                    $joinSQL = $this->compileJoinWithBuilder($joinQuery, $joinBuilder);
                } else {
                    $joinSQL = $this->compileJoin($joinQuery);
                }

                $joinTable = $joinQuery->getAttribute();
                $joinType = match ($joinQuery->getMethod()) {
                    Method::Join => 'JOIN',
                    Method::LeftJoin => 'LEFT JOIN',
                    Method::RightJoin => 'RIGHT JOIN',
                    Method::CrossJoin => 'CROSS JOIN',
                    default => 'JOIN',
                };
                $isCrossJoin = $joinQuery->getMethod() === Method::CrossJoin;

                foreach ($this->joinFilterHooks as $hook) {
                    $result = $hook->filterJoin($joinTable, $joinType);
                    if ($result === null) {
                        continue;
                    }

                    $placement = $this->resolveJoinFilterPlacement($result->placement, $isCrossJoin);

                    if ($placement === Placement::On) {
                        $joinSQL .= ' AND ' . $result->condition->getExpression();
                        foreach ($result->condition->getBindings() as $binding) {
                            $this->addBinding($binding);
                        }
                    } else {
                        $joinFilterWhereClauses[] = $result->condition;
                    }
                }

                $parts[] = $joinSQL;
            }
        }

        // Hook: after joins (e.g. ClickHouse PREWHERE)
        $this->buildAfterJoins($parts, $grouped);

        // WHERE
        $whereClauses = [];

        foreach ($grouped->filters as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->table);
            $whereClauses[] = $condition->getExpression();
            foreach ($condition->getBindings() as $binding) {
                $this->addBinding($binding);
            }
        }

        foreach ($joinFilterWhereClauses as $condition) {
            $whereClauses[] = $condition->getExpression();
            foreach ($condition->getBindings() as $binding) {
                $this->addBinding($binding);
            }
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub['subquery']->build();
            $prefix = $sub['not'] ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub['column']) . ' ' . $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub['subquery']->build();
            $prefix = $sub['not'] ? 'NOT EXISTS' : 'EXISTS';
            $whereClauses[] = $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $cursorSQL = '';
        if ($grouped->cursor !== null && $grouped->cursorDirection !== null) {
            $cursorQueries = Query::getCursorQueries($this->pendingQueries, false);
            if (! empty($cursorQueries)) {
                $cursorSQL = $this->compileCursor($cursorQueries[0]);
            }
        }
        if ($cursorSQL !== '') {
            $whereClauses[] = $cursorSQL;
        }

        if (! empty($whereClauses)) {
            $parts[] = 'WHERE ' . \implode(' AND ', $whereClauses);
        }

        // GROUP BY
        $groupByParts = [];
        if (! empty($grouped->groupBy)) {
            $groupByCols = \array_map(
                fn (string $col): string => $this->resolveAndWrap($col),
                $grouped->groupBy
            );
            $groupByParts = $groupByCols;
        }
        foreach ($this->rawGroups as $rawGroup) {
            $groupByParts[] = $rawGroup['expression'];
            foreach ($rawGroup['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($groupByParts)) {
            $parts[] = 'GROUP BY ' . \implode(', ', $groupByParts);
        }

        // HAVING
        $havingClauses = [];
        if (! empty($grouped->having)) {
            foreach ($grouped->having as $havingQuery) {
                foreach ($havingQuery->getValues() as $subQuery) {
                    /** @var Query $subQuery */
                    $havingClauses[] = $this->compileFilter($subQuery);
                }
            }
        }
        foreach ($this->rawHavings as $rawHaving) {
            $havingClauses[] = $rawHaving['expression'];
            foreach ($rawHaving['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($havingClauses)) {
            $parts[] = 'HAVING ' . \implode(' AND ', $havingClauses);
        }

        // ORDER BY
        $orderClauses = [];

        $vectorOrderExpr = $this->compileVectorOrderExpr();
        if ($vectorOrderExpr !== null) {
            $orderClauses[] = $vectorOrderExpr['expression'];
            foreach ($vectorOrderExpr['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        $orderQueries = Query::getByType($this->pendingQueries, [
            Method::OrderAsc,
            Method::OrderDesc,
            Method::OrderRandom,
        ], false);
        foreach ($orderQueries as $orderQuery) {
            $orderClauses[] = $this->compileOrder($orderQuery);
        }
        foreach ($this->rawOrders as $rawOrder) {
            $orderClauses[] = $rawOrder['expression'];
            foreach ($rawOrder['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($orderClauses)) {
            $parts[] = 'ORDER BY ' . \implode(', ', $orderClauses);
        }

        // LIMIT
        if ($grouped->limit !== null) {
            $parts[] = 'LIMIT ?';
            $this->addBinding($grouped->limit);
        }

        // OFFSET
        if ($this->shouldEmitOffset($grouped->offset, $grouped->limit)) {
            $parts[] = 'OFFSET ?';
            $this->addBinding($grouped->offset);
        }

        // LOCKING
        if ($this->lockMode !== null) {
            $parts[] = $this->lockMode;
        }

        $sql = \implode(' ', $parts);

        // UNION
        if (!empty($this->unions)) {
            $sql = '(' . $sql . ')';
        }
        foreach ($this->unions as $union) {
            $sql .= ' ' . $union->type . ' (' . $union->query . ')';
            foreach ($union->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $sql = $ctePrefix . $sql;

        return new BuildResult($sql, $this->bindings);
    }

    /**
     * Compile the INSERT INTO ... VALUES portion.
     *
     * @return array{0: string, 1: list<mixed>}
     */
    protected function compileInsertBody(): array
    {
        $this->validateTable();
        $this->validateRows('insert');
        $columns = $this->validateAndGetColumns();

        $wrappedColumns = \array_map(fn (string $col): string => $this->resolveAndWrap($col), $columns);

        $bindings = [];
        $rowPlaceholders = [];
        foreach ($this->pendingRows as $row) {
            $placeholders = [];
            foreach ($columns as $col) {
                $bindings[] = $row[$col] ?? null;
                $placeholders[] = '?';
            }
            $rowPlaceholders[] = '(' . \implode(', ', $placeholders) . ')';
        }

        $sql = 'INSERT INTO ' . $this->quote($this->table)
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' VALUES ' . \implode(', ', $rowPlaceholders);

        return [$sql, $bindings];
    }

    public function insert(): BuildResult
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        foreach ($bindings as $binding) {
            $this->addBinding($binding);
        }

        return new BuildResult($sql, $this->bindings);
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

        $parts = ['UPDATE ' . $this->quote($this->table) . ' SET ' . \implode(', ', $assignments)];

        $this->compileWhereClauses($parts);

        $this->compileOrderAndLimit($parts);

        return new BuildResult(\implode(' ', $parts), $this->bindings);
    }

    public function delete(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $parts = ['DELETE FROM ' . $this->quote($this->table)];

        $this->compileWhereClauses($parts);

        $this->compileOrderAndLimit($parts);

        return new BuildResult(\implode(' ', $parts), $this->bindings);
    }

    /**
     * @param  array<string>  $parts
     */
    protected function compileWhereClauses(array &$parts): void
    {
        $grouped = Query::groupByType($this->pendingQueries);
        $whereClauses = [];

        foreach ($grouped->filters as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->table);
            $whereClauses[] = $condition->getExpression();
            foreach ($condition->getBindings() as $binding) {
                $this->addBinding($binding);
            }
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub['subquery']->build();
            $prefix = $sub['not'] ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub['column']) . ' ' . $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub['subquery']->build();
            $prefix = $sub['not'] ? 'NOT EXISTS' : 'EXISTS';
            $whereClauses[] = $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        if (! empty($whereClauses)) {
            $parts[] = 'WHERE ' . \implode(' AND ', $whereClauses);
        }
    }

    /**
     * @param  array<string>  $parts
     */
    protected function compileOrderAndLimit(array &$parts): void
    {
        $orderClauses = [];
        $orderQueries = Query::getByType($this->pendingQueries, [
            Method::OrderAsc,
            Method::OrderDesc,
            Method::OrderRandom,
        ], false);
        foreach ($orderQueries as $orderQuery) {
            $orderClauses[] = $this->compileOrder($orderQuery);
        }
        foreach ($this->rawOrders as $rawOrder) {
            $orderClauses[] = $rawOrder['expression'];
            foreach ($rawOrder['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($orderClauses)) {
            $parts[] = 'ORDER BY ' . \implode(', ', $orderClauses);
        }

        $grouped = Query::groupByType($this->pendingQueries);
        if ($grouped->limit !== null) {
            $parts[] = 'LIMIT ?';
            $this->addBinding($grouped->limit);
        }
    }

    protected function shouldEmitOffset(?int $offset, ?int $limit): bool
    {
        return $offset !== null && $limit !== null;
    }

    /**
     * Hook for subclasses to inject a vector distance ORDER BY expression.
     *
     * @return array{expression: string, bindings: list<mixed>}|null
     */
    protected function compileVectorOrderExpr(): ?array
    {
        return null;
    }

    protected function validateTable(): void
    {
        if ($this->table === '' && $this->fromSubquery === null) {
            throw new ValidationException('No table specified. Call from() or into() before building a query.');
        }
    }

    protected function validateRows(string $operation): void
    {
        if (empty($this->pendingRows)) {
            throw new ValidationException("No rows to {$operation}. Call set() before {$operation}().");
        }

        foreach ($this->pendingRows as $row) {
            if (empty($row)) {
                throw new ValidationException('Cannot ' . $operation . ' an empty row. Each set() call must include at least one column.');
            }
        }
    }

    /**
     * Validates that all rows have the same columns and returns the column list.
     *
     * @return list<string>
     */
    protected function validateAndGetColumns(): array
    {
        $columns = \array_keys($this->pendingRows[0]);

        foreach ($columns as $col) {
            if ($col === '') {
                throw new ValidationException('Column names must be non-empty strings.');
            }
        }

        if (\count($this->pendingRows) > 1) {
            $expectedKeys = $columns;
            \sort($expectedKeys);

            foreach ($this->pendingRows as $i => $row) {
                $rowKeys = \array_keys($row);
                \sort($rowKeys);

                if ($rowKeys !== $expectedKeys) {
                    throw new ValidationException("Row {$i} has different columns than row 0. All rows in a batch must have the same columns.");
                }
            }
        }

        return $columns;
    }

    /**
     * @return list<mixed>
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    public function reset(): static
    {
        $this->pendingQueries = [];
        $this->bindings = [];
        $this->table = '';
        $this->tableAlias = '';
        $this->unions = [];
        $this->pendingRows = [];
        $this->rawSets = [];
        $this->rawSetBindings = [];
        $this->conflictKeys = [];
        $this->conflictUpdateColumns = [];
        $this->conflictRawSets = [];
        $this->conflictRawSetBindings = [];
        $this->lockMode = null;
        $this->insertSelectSource = null;
        $this->insertSelectColumns = [];
        $this->ctes = [];
        $this->rawSelects = [];
        $this->windowSelects = [];
        $this->caseSelects = [];
        $this->caseSets = [];
        $this->whereInSubqueries = [];
        $this->subSelects = [];
        $this->fromSubquery = null;
        $this->rawOrders = [];
        $this->rawGroups = [];
        $this->rawHavings = [];
        $this->joinBuilders = [];
        $this->existsSubqueries = [];

        return $this;
    }

    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAndWrap($query->getAttribute());
        $values = $query->getValues();

        return match ($method) {
            Method::Equal => $this->compileIn($attribute, $values),
            Method::NotEqual => $this->compileNotIn($attribute, $values),
            Method::LessThan => $this->compileComparison($attribute, '<', $values),
            Method::LessThanEqual => $this->compileComparison($attribute, '<=', $values),
            Method::GreaterThan => $this->compileComparison($attribute, '>', $values),
            Method::GreaterThanEqual => $this->compileComparison($attribute, '>=', $values),
            Method::Between => $this->compileBetween($attribute, $values, false),
            Method::NotBetween => $this->compileBetween($attribute, $values, true),
            Method::StartsWith => $this->compileLike($attribute, $values, '', '%', false),
            Method::NotStartsWith => $this->compileLike($attribute, $values, '', '%', true),
            Method::EndsWith => $this->compileLike($attribute, $values, '%', '', false),
            Method::NotEndsWith => $this->compileLike($attribute, $values, '%', '', true),
            Method::Contains => $this->compileContains($attribute, $values),
            Method::ContainsAny => $this->compileIn($attribute, $values),
            Method::ContainsAll => $this->compileContainsAll($attribute, $values),
            Method::NotContains => $this->compileNotContains($attribute, $values),
            Method::Search => $this->compileSearch($attribute, $values, false),
            Method::NotSearch => $this->compileSearch($attribute, $values, true),
            Method::Regex => $this->compileRegex($attribute, $values),
            Method::IsNull => $attribute . ' IS NULL',
            Method::IsNotNull => $attribute . ' IS NOT NULL',
            Method::And => $this->compileLogical($query, 'AND'),
            Method::Or => $this->compileLogical($query, 'OR'),
            Method::Having => $this->compileLogical($query, 'AND'),
            Method::Exists => $this->compileExists($query),
            Method::NotExists => $this->compileNotExists($query),
            Method::Raw => $this->compileRaw($query),
            default => throw new UnsupportedException('Unsupported filter type: ' . $method->value),
        };
    }

    public function compileOrder(Query $query): string
    {
        return match ($query->getMethod()) {
            Method::OrderAsc => $this->resolveAndWrap($query->getAttribute()) . ' ASC',
            Method::OrderDesc => $this->resolveAndWrap($query->getAttribute()) . ' DESC',
            Method::OrderRandom => $this->compileRandom(),
            default => throw new UnsupportedException('Unsupported order type: ' . $query->getMethod()->value),
        };
    }

    public function compileLimit(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'LIMIT ?';
    }

    public function compileOffset(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'OFFSET ?';
    }

    public function compileSelect(Query $query): string
    {
        /** @var array<string> $values */
        $values = $query->getValues();
        $columns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $values
        );

        return \implode(', ', $columns);
    }

    public function compileCursor(Query $query): string
    {
        $value = $query->getValue();
        $this->addBinding($value);

        $operator = $query->getMethod() === Method::CursorAfter ? '>' : '<';

        return $this->quote('_cursor') . ' ' . $operator . ' ?';
    }

    public function compileAggregate(Query $query): string
    {
        $method = $query->getMethod();

        if ($method === Method::CountDistinct) {
            $attr = $query->getAttribute();
            $col = ($attr === '*' || $attr === '') ? '*' : $this->resolveAndWrap($attr);
            /** @var string $alias */
            $alias = $query->getValue('');
            $sql = 'COUNT(DISTINCT ' . $col . ')';

            if ($alias !== '') {
                $sql .= ' AS ' . $this->quote($alias);
            }

            return $sql;
        }

        $func = match ($method) {
            Method::Count => 'COUNT',
            Method::Sum => 'SUM',
            Method::Avg => 'AVG',
            Method::Min => 'MIN',
            Method::Max => 'MAX',
            default => throw new ValidationException("Unknown aggregate: {$method->value}"),
        };
        $attr = $query->getAttribute();
        $col = ($attr === '*' || $attr === '') ? '*' : $this->resolveAndWrap($attr);
        /** @var string $alias */
        $alias = $query->getValue('');
        $sql = $func . '(' . $col . ')';

        if ($alias !== '') {
            $sql .= ' AS ' . $this->quote($alias);
        }

        return $sql;
    }

    public function compileGroupBy(Query $query): string
    {
        /** @var array<string> $values */
        $values = $query->getValues();
        $columns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $values
        );

        return \implode(', ', $columns);
    }

    public function compileJoin(Query $query): string
    {
        $type = match ($query->getMethod()) {
            Method::Join => 'JOIN',
            Method::LeftJoin => 'LEFT JOIN',
            Method::RightJoin => 'RIGHT JOIN',
            Method::CrossJoin => 'CROSS JOIN',
            default => throw new UnsupportedException('Unsupported join type: ' . $query->getMethod()->value),
        };

        $table = $this->quote($query->getAttribute());
        $values = $query->getValues();

        // Handle alias for cross join (alias is values[0])
        if ($query->getMethod() === Method::CrossJoin) {
            /** @var string $alias */
            $alias = $values[0] ?? '';
            if ($alias !== '') {
                $table .= ' AS ' . $this->quote($alias);
            }

            return $type . ' ' . $table;
        }

        if (empty($values)) {
            return $type . ' ' . $table;
        }

        /** @var string $leftCol */
        $leftCol = $values[0];
        /** @var string $operator */
        $operator = $values[1];
        /** @var string $rightCol */
        $rightCol = $values[2];
        /** @var string $alias */
        $alias = $values[3] ?? '';

        if ($alias !== '') {
            $table .= ' AS ' . $this->quote($alias);
        }

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        if (! \in_array($operator, $allowedOperators, true)) {
            throw new ValidationException('Invalid join operator: ' . $operator);
        }

        $left = $this->resolveAndWrap($leftCol);
        $right = $this->resolveAndWrap($rightCol);

        return $type . ' ' . $table . ' ON ' . $left . ' ' . $operator . ' ' . $right;
    }

    protected function compileJoinWithBuilder(Query $query, JoinBuilder $joinBuilder): string
    {
        $type = match ($query->getMethod()) {
            Method::Join => 'JOIN',
            Method::LeftJoin => 'LEFT JOIN',
            Method::RightJoin => 'RIGHT JOIN',
            Method::CrossJoin => 'CROSS JOIN',
            default => throw new UnsupportedException('Unsupported join type: ' . $query->getMethod()->value),
        };

        $table = $this->quote($query->getAttribute());
        $values = $query->getValues();

        // Handle alias
        if ($query->getMethod() === Method::CrossJoin) {
            /** @var string $alias */
            $alias = $values[0] ?? '';
        } else {
            /** @var string $alias */
            $alias = $values[3] ?? '';
        }

        if ($alias !== '') {
            $table .= ' AS ' . $this->quote($alias);
        }

        $onParts = [];

        foreach ($joinBuilder->getOns() as $on) {
            $left = $this->resolveAndWrap($on['left']);
            $right = $this->resolveAndWrap($on['right']);
            $onParts[] = $left . ' ' . $on['operator'] . ' ' . $right;
        }

        foreach ($joinBuilder->getWheres() as $where) {
            $onParts[] = $where['expression'];
            foreach ($where['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        if (empty($onParts)) {
            return $type . ' ' . $table;
        }

        return $type . ' ' . $table . ' ON ' . \implode(' AND ', $onParts);
    }

    protected function resolveAttribute(string $attribute): string
    {
        foreach ($this->attributeHooks as $hook) {
            $attribute = $hook->resolve($attribute);
        }

        return $attribute;
    }

    protected function resolveAndWrap(string $attribute): string
    {
        return $this->quote($this->resolveAttribute($attribute));
    }

    protected function addBinding(mixed $value): void
    {
        $this->bindings[] = $value;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileLike(string $attribute, array $values, string $prefix, string $suffix, bool $not): string
    {
        /** @var string $rawVal */
        $rawVal = $values[0];
        $val = $this->escapeLikeValue($rawVal);
        $this->addBinding($prefix . $val . $suffix);
        $keyword = $not ? 'NOT LIKE' : 'LIKE';

        return $attribute . ' ' . $keyword . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' LIKE ?';
        }

        return '(' . \implode(' OR ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileContainsAll(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' LIKE ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileNotContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' NOT LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' NOT LIKE ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * Escape LIKE metacharacters in user input before wrapping with wildcards.
     */
    protected function escapeLikeValue(string $value): string
    {
        return \str_replace(['\\', '%', '_'], ['\\\\', '\\%', '\\_'], $value);
    }

    /**
     * Resolve the placement for a join filter condition.
     * ClickHouse overrides this to always return Placement::Where since it
     * does not support subqueries in JOIN ON conditions.
     */
    protected function resolveJoinFilterPlacement(Placement $requested, bool $isCrossJoin): Placement
    {
        return $isCrossJoin ? Placement::Where : $requested;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileIn(string $attribute, array $values): string
    {
        if ($values === []) {
            return '1 = 0';
        }

        $hasNulls = false;
        $nonNulls = [];

        foreach ($values as $value) {
            if ($value === null) {
                $hasNulls = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        $hasNonNulls = $nonNulls !== [];

        if ($hasNulls && ! $hasNonNulls) {
            return $attribute . ' IS NULL';
        }

        $placeholders = \array_fill(0, \count($nonNulls), '?');
        foreach ($nonNulls as $value) {
            $this->addBinding($value);
        }
        $inClause = $attribute . ' IN (' . \implode(', ', $placeholders) . ')';

        if ($hasNulls) {
            return '(' . $inClause . ' OR ' . $attribute . ' IS NULL)';
        }

        return $inClause;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileNotIn(string $attribute, array $values): string
    {
        if ($values === []) {
            return '1 = 1';
        }

        $hasNulls = false;
        $nonNulls = [];

        foreach ($values as $value) {
            if ($value === null) {
                $hasNulls = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        $hasNonNulls = $nonNulls !== [];

        if ($hasNulls && ! $hasNonNulls) {
            return $attribute . ' IS NOT NULL';
        }

        if (\count($nonNulls) === 1) {
            $this->addBinding($nonNulls[0]);
            $notClause = $attribute . ' != ?';
        } else {
            $placeholders = \array_fill(0, \count($nonNulls), '?');
            foreach ($nonNulls as $value) {
                $this->addBinding($value);
            }
            $notClause = $attribute . ' NOT IN (' . \implode(', ', $placeholders) . ')';
        }

        if ($hasNulls) {
            return '(' . $notClause . ' AND ' . $attribute . ' IS NOT NULL)';
        }

        return $notClause;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileComparison(string $attribute, string $operator, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' ' . $operator . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileBetween(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);
        $this->addBinding($values[1]);
        $keyword = $not ? 'NOT BETWEEN' : 'BETWEEN';

        return $attribute . ' ' . $keyword . ' ? AND ?';
    }

    private function compileLogical(Query $query, string $operator): string
    {
        $parts = [];
        foreach ($query->getValues() as $subQuery) {
            /** @var Query $subQuery */
            $parts[] = $this->compileFilter($subQuery);
        }

        if ($parts === []) {
            return $operator === 'OR' ? '1 = 0' : '1 = 1';
        }

        return '(' . \implode(' ' . $operator . ' ', $parts) . ')';
    }

    private function compileExists(Query $query): string
    {
        $parts = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $parts[] = $this->resolveAndWrap($attr) . ' IS NOT NULL';
        }

        if ($parts === []) {
            return '1 = 1';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileNotExists(Query $query): string
    {
        $parts = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $parts[] = $this->resolveAndWrap($attr) . ' IS NULL';
        }

        if ($parts === []) {
            return '1 = 1';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileRaw(Query $query): string
    {
        $attribute = $query->getAttribute();

        if ($attribute === '') {
            return '1 = 1';
        }

        foreach ($query->getValues() as $binding) {
            $this->addBinding($binding);
        }

        return $attribute;
    }
}
