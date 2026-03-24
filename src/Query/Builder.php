<?php

namespace Utopia\Query;

use Closure;
use Utopia\Query\AST\AliasedExpr;
use Utopia\Query\AST\BetweenExpr;
use Utopia\Query\AST\BinaryExpr;
use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\CteDefinition;
use Utopia\Query\AST\Expr;
use Utopia\Query\AST\FunctionCall;
use Utopia\Query\AST\InExpr;
use Utopia\Query\AST\JoinClause as AstJoinClause;
use Utopia\Query\AST\Literal;
use Utopia\Query\AST\OrderByItem;
use Utopia\Query\AST\Raw;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Serializer;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\SubquerySource;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\UnaryExpr;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\Case\Expression as CaseExpression;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\CteClause;
use Utopia\Query\Builder\ExistsSubquery;
use Utopia\Query\Builder\Feature;
use Utopia\Query\Builder\GroupedQueries;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\LateralJoin;
use Utopia\Query\Builder\LockMode;
use Utopia\Query\Builder\SubSelect;
use Utopia\Query\Builder\UnionClause;
use Utopia\Query\Builder\UnionType;
use Utopia\Query\Builder\WhereInSubquery;
use Utopia\Query\Builder\WindowDefinition;
use Utopia\Query\Builder\WindowFrame;
use Utopia\Query\Builder\WindowSelect;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\NullsPosition;
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

    protected ?LockMode $lockMode = null;

    protected ?string $lockOfTable = null;

    protected ?Builder $insertSelectSource = null;

    /** @var list<string> */
    protected array $insertSelectColumns = [];

    /** @var list<CteClause> */
    protected array $ctes = [];

    /** @var list<Condition> */
    protected array $rawSelects = [];

    /** @var list<WindowSelect> */
    protected array $windowSelects = [];

    /** @var list<WindowDefinition> */
    protected array $windowDefinitions = [];

    /** @var ?array{percent: float, method: string} */
    protected ?array $tableSample = null;

    /** @var list<CaseExpression> */
    protected array $caseSelects = [];

    /** @var array<string, CaseExpression> */
    protected array $caseSets = [];

    /** @var string[] */
    protected array $conflictKeys = [];

    /** @var string[] */
    protected array $conflictUpdateColumns = [];

    /** @var array<string, string> */
    protected array $conflictRawSets = [];

    /** @var array<string, list<mixed>> */
    protected array $conflictRawSetBindings = [];

    /** @var array<string, string> Column-specific expressions for INSERT (e.g. 'location' => 'ST_GeomFromText(?)') */
    protected array $insertColumnExpressions = [];

    /** @var array<string, list<mixed>> Extra bindings for insert column expressions */
    protected array $insertColumnExpressionBindings = [];

    protected string $insertAlias = '';

    /** @var list<WhereInSubquery> */
    protected array $whereInSubqueries = [];

    /** @var list<SubSelect> */
    protected array $subSelects = [];

    protected ?SubSelect $fromSubquery = null;

    protected bool $noTable = false;

    /** @var list<Condition> */
    protected array $rawOrders = [];

    /** @var list<Condition> */
    protected array $rawGroups = [];

    /** @var list<Condition> */
    protected array $rawHavings = [];

    /** @var array<int, JoinBuilder> */
    protected array $joinBuilders = [];

    /** @var list<ExistsSubquery> */
    protected array $existsSubqueries = [];

    /** @var list<LateralJoin> */
    protected array $lateralJoins = [];

    /** @var list<Closure> */
    protected array $beforeBuildCallbacks = [];

    /** @var list<Closure(BuildResult): BuildResult> */
    protected array $afterBuildCallbacks = [];

    protected bool $qualifyColumns = false;

    /** @var array<string, true> */
    protected array $aggregationAliases = [];

    protected ?int $fetchCount = null;

    protected bool $fetchWithTies = false;

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

    protected function buildTableClause(): string
    {
        if ($this->noTable) {
            return '';
        }

        $fromSub = $this->fromSubquery;
        if ($fromSub !== null) {
            $subResult = $fromSub->subquery->build();
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }

            return 'FROM (' . $subResult->query . ') AS ' . $this->quote($fromSub->alias);
        }

        $sql = 'FROM ' . $this->quote($this->table);

        if ($this->tableAlias !== '') {
            $sql .= ' AS ' . $this->quote($this->tableAlias);
        }

        if ($this->tableSample !== null) {
            $sql .= ' TABLESAMPLE ' . $this->tableSample['method'] . '(' . $this->tableSample['percent'] . ')';
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
        $this->noTable = false;

        return $this;
    }

    /**
     * Build a query without a FROM clause (e.g. SELECT 1, SELECT CONNECTION_ID()).
     */
    public function fromNone(): static
    {
        $this->noTable = true;
        $this->table = '';
        $this->tableAlias = '';
        $this->fromSubquery = null;

        return $this;
    }

    public function into(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    /**
     * Set an alias for the INSERT target table (e.g. INSERT INTO table AS alias).
     * Used by PostgreSQL ON CONFLICT to reference the existing row.
     */
    public function insertAs(string $alias): static
    {
        $this->insertAlias = $alias;

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

    /**
     * Register a raw expression wrapper for a column in INSERT statements.
     *
     * The expression must contain exactly one `?` placeholder which will receive
     * the column's value from each row. E.g. `ST_GeomFromText(?, 4326)`.
     *
     * @param  list<mixed>  $extraBindings  Additional bindings beyond the column value (e.g. SRID)
     */
    public function insertColumnExpression(string $column, string $expression, array $extraBindings = []): static
    {
        $this->insertColumnExpressions[$column] = $expression;
        if (! empty($extraBindings)) {
            $this->insertColumnExpressionBindings[$column] = $extraBindings;
        }

        return $this;
    }

    public function filterWhereIn(string $column, Builder $subquery): static
    {
        $this->whereInSubqueries[] = new WhereInSubquery($column, $subquery, false);

        return $this;
    }

    public function filterWhereNotIn(string $column, Builder $subquery): static
    {
        $this->whereInSubqueries[] = new WhereInSubquery($column, $subquery, true);

        return $this;
    }

    public function selectSub(Builder $subquery, string $alias): static
    {
        $this->subSelects[] = new SubSelect($subquery, $alias);

        return $this;
    }

    public function fromSub(Builder $subquery, string $alias): static
    {
        $this->fromSubquery = new SubSelect($subquery, $alias);
        $this->table = '';

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function orderByRaw(string $expression, array $bindings = []): static
    {
        $this->rawOrders[] = new Condition($expression, $bindings);

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function groupByRaw(string $expression, array $bindings = []): static
    {
        $this->rawGroups[] = new Condition($expression, $bindings);

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function havingRaw(string $expression, array $bindings = []): static
    {
        $this->rawHavings[] = new Condition($expression, $bindings);

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
    public function joinWhere(string $table, Closure $callback, JoinType $type = JoinType::Inner, string $alias = ''): static
    {
        $joinBuilder = new JoinBuilder();
        $callback($joinBuilder);

        $method = match ($type) {
            JoinType::Left => Method::LeftJoin,
            JoinType::Right => Method::RightJoin,
            JoinType::Cross => Method::CrossJoin,
            JoinType::FullOuter => Method::FullOuterJoin,
            JoinType::Natural => Method::NaturalJoin,
            default => Method::Join,
        };

        if ($method === Method::CrossJoin || $method === Method::NaturalJoin) {
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
        $this->existsSubqueries[] = new ExistsSubquery($subquery, false);

        return $this;
    }

    public function filterNotExists(Builder $subquery): static
    {
        $this->existsSubqueries[] = new ExistsSubquery($subquery, true);

        return $this;
    }

    public function explain(bool $analyze = false): BuildResult
    {
        $result = $this->build();
        $prefix = $analyze ? 'EXPLAIN ANALYZE ' : 'EXPLAIN ';

        return new BuildResult($prefix . $result->query, $result->bindings, readOnly: true);
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

    public function sortAsc(string $attribute, ?NullsPosition $nulls = null): static
    {
        $this->pendingQueries[] = Query::orderAsc($attribute, $nulls);

        return $this;
    }

    public function sortDesc(string $attribute, ?NullsPosition $nulls = null): static
    {
        $this->pendingQueries[] = Query::orderDesc($attribute, $nulls);

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

    public function fetch(int $count, bool $withTies = false): static
    {
        $this->fetchCount = $count;
        $this->fetchWithTies = $withTies;

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

    public function naturalJoin(string $table, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::naturalJoin($table, $alias);

        return $this;
    }

    public function union(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Union, $result->query, $result->bindings);

        return $this;
    }

    public function unionAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::UnionAll, $result->query, $result->bindings);

        return $this;
    }

    public function intersect(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Intersect, $result->query, $result->bindings);

        return $this;
    }

    public function intersectAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::IntersectAll, $result->query, $result->bindings);

        return $this;
    }

    public function except(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Except, $result->query, $result->bindings);

        return $this;
    }

    public function exceptAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::ExceptAll, $result->query, $result->bindings);

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

    /**
     * @param  list<string>  $columns
     */
    public function with(string $name, self $query, array $columns = []): static
    {
        $result = $query->build();
        $this->ctes[] = new CteClause($name, $result->query, $result->bindings, false, $columns);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    public function withRecursive(string $name, self $query, array $columns = []): static
    {
        $result = $query->build();
        $this->ctes[] = new CteClause($name, $result->query, $result->bindings, true, $columns);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    public function withRecursiveSeedStep(string $name, self $seed, self $step, array $columns = []): static
    {
        $seedResult = $seed->build();
        $stepResult = $step->build();
        $query = $seedResult->query . ' UNION ALL ' . $stepResult->query;
        $bindings = \array_merge($seedResult->bindings, $stepResult->bindings);
        $this->ctes[] = new CteClause($name, $query, $bindings, true, $columns);

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function selectRaw(string $expression, array $bindings = []): static
    {
        $this->rawSelects[] = new Condition($expression, $bindings);

        return $this;
    }

    public function selectCast(string $column, string $type, string $alias = ''): static
    {
        $expr = 'CAST(' . $this->resolveAndWrap($column) . ' AS ' . $type . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }
        $this->rawSelects[] = new Condition($expr, []);

        return $this;
    }

    public function selectWindow(string $function, string $alias, ?array $partitionBy = null, ?array $orderBy = null, ?string $windowName = null, ?WindowFrame $frame = null): static
    {
        $this->windowSelects[] = new WindowSelect($function, $alias, $partitionBy, $orderBy, $windowName, $frame);

        return $this;
    }

    public function window(string $name, ?array $partitionBy = null, ?array $orderBy = null, ?WindowFrame $frame = null): static
    {
        $this->windowDefinitions[] = new WindowDefinition($name, $partitionBy, $orderBy, $frame);

        return $this;
    }

    public function selectCase(CaseExpression $case): static
    {
        $this->caseSelects[] = $case;

        return $this;
    }

    public function setCase(string $column, CaseExpression $case): static
    {
        $this->caseSets[$column] = $case;

        return $this;
    }

    public function when(bool $condition, Closure $callback): static
    {
        if ($condition) {
            $callback($this);
        }

        return $this;
    }

    public function beforeBuild(Closure $callback): static
    {
        $this->beforeBuildCallbacks[] = $callback;

        return $this;
    }

    public function afterBuild(Closure $callback): static
    {
        $this->afterBuildCallbacks[] = $callback;

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

        foreach ($this->beforeBuildCallbacks as $callback) {
            $callback($this);
        }

        $this->validateTable();

        // CTE prefix
        $ctePrefix = '';
        if (! empty($this->ctes)) {
            $hasRecursive = false;
            $cteParts = [];
            foreach ($this->ctes as $cte) {
                if ($cte->recursive) {
                    $hasRecursive = true;
                }
                foreach ($cte->bindings as $binding) {
                    $this->addBinding($binding);
                }
                $cteName = $this->quote($cte->name);
                if (! empty($cte->columns)) {
                    $cteName .= '(' . \implode(', ', \array_map(fn (string $col): string => $this->quote($col), $cte->columns)) . ')';
                }
                $cteParts[] = $cteName . ' AS (' . $cte->query . ')';
            }
            $keyword = $hasRecursive ? 'WITH RECURSIVE' : 'WITH';
            $ctePrefix = $keyword . ' ' . \implode(', ', $cteParts) . ' ';
        }

        $grouped = Query::groupByType($this->pendingQueries);

        $this->qualifyColumns = false;
        $this->aggregationAliases = [];
        if (! empty($grouped->joins) && $this->tableAlias !== '') {
            $this->qualifyColumns = true;
            foreach ($grouped->aggregations as $agg) {
                /** @var string $aggAlias */
                $aggAlias = $agg->getValue('');
                if ($aggAlias !== '') {
                    $this->aggregationAliases[$aggAlias] = true;
                }
            }
        }

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
            $subResult = $subSelect->subquery->build();
            $selectParts[] = '(' . $subResult->query . ') AS ' . $this->quote($subSelect->alias);
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // Raw selects
        foreach ($this->rawSelects as $rawSelect) {
            $selectParts[] = $rawSelect->expression;
            foreach ($rawSelect->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // Window function selects
        foreach ($this->windowSelects as $win) {
            if ($win->windowName !== null) {
                $selectParts[] = $win->function . ' OVER ' . $this->quote($win->windowName) . ' AS ' . $this->quote($win->alias);
            } else {
                $overParts = [];

                if ($win->partitionBy !== null && $win->partitionBy !== []) {
                    $partCols = \array_map(
                        fn (string $col): string => $this->resolveAndWrap($col),
                        $win->partitionBy
                    );
                    $overParts[] = 'PARTITION BY ' . \implode(', ', $partCols);
                }

                if ($win->orderBy !== null && $win->orderBy !== []) {
                    $orderCols = [];
                    foreach ($win->orderBy as $col) {
                        if (\str_starts_with($col, '-')) {
                            $orderCols[] = $this->resolveAndWrap(\substr($col, 1)) . ' DESC';
                        } else {
                            $orderCols[] = $this->resolveAndWrap($col) . ' ASC';
                        }
                    }
                    $overParts[] = 'ORDER BY ' . \implode(', ', $orderCols);
                }

                if ($win->frame !== null) {
                    $overParts[] = $win->frame->toSql();
                }

                $overClause = \implode(' ', $overParts);
                $selectParts[] = $win->function . ' OVER (' . $overClause . ') AS ' . $this->quote($win->alias);
            }
        }

        // CASE selects
        foreach ($this->caseSelects as $caseSelect) {
            $selectParts[] = $caseSelect->sql;
            foreach ($caseSelect->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $selectSQL = ! empty($selectParts) ? \implode(', ', $selectParts) : '*';

        $selectKeyword = $grouped->distinct ? 'SELECT DISTINCT' : 'SELECT';
        $parts[] = $selectKeyword . ' ' . $selectSQL;

        // FROM
        $tableClause = $this->buildTableClause();
        if ($tableClause !== '') {
            $parts[] = $tableClause;
        }

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
                    Method::Join => JoinType::Inner,
                    Method::LeftJoin => JoinType::Left,
                    Method::RightJoin => JoinType::Right,
                    Method::CrossJoin => JoinType::Cross,
                    Method::FullOuterJoin => JoinType::FullOuter,
                    Method::NaturalJoin => JoinType::Natural,
                    default => JoinType::Inner,
                };
                $isCrossJoin = $joinType === JoinType::Cross || $joinType === JoinType::Natural;

                $joinValues = $joinQuery->getValues();
                if ($isCrossJoin) {
                    /** @var string $joinAlias */
                    $joinAlias = $joinValues[0] ?? '';
                } else {
                    /** @var string $joinAlias */
                    $joinAlias = $joinValues[3] ?? '';
                }
                $effectiveJoinTable = $joinAlias !== '' ? $joinAlias : $joinTable;

                foreach ($this->joinFilterHooks as $hook) {
                    $result = $hook->filterJoin($effectiveJoinTable, $joinType);
                    if ($result === null) {
                        continue;
                    }

                    $placement = $this->resolveJoinFilterPlacement($result->placement, $isCrossJoin);

                    if ($placement === Placement::On) {
                        $joinSQL .= ' AND ' . $result->condition->expression;
                        foreach ($result->condition->bindings as $binding) {
                            $this->addBinding($binding);
                        }
                    } else {
                        $joinFilterWhereClauses[] = $result->condition;
                    }
                }

                $parts[] = $joinSQL;
            }
        }

        foreach ($this->lateralJoins as $lateral) {
            $subResult = $lateral->subquery->build();
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
            $joinKeyword = match ($lateral->type) {
                JoinType::Left => 'LEFT JOIN',
                default => 'JOIN',
            };
            $parts[] = $joinKeyword . ' LATERAL (' . $subResult->query . ') AS ' . $this->quote($lateral->alias) . ' ON true';
        }

        // Hook: after joins (e.g. ClickHouse PREWHERE)
        $this->buildAfterJoins($parts, $grouped);

        // WHERE
        $whereClauses = [];

        foreach ($grouped->filters as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->tableAlias ?: $this->table);
            $whereClauses[] = $condition->expression;
            foreach ($condition->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        foreach ($joinFilterWhereClauses as $condition) {
            $whereClauses[] = $condition->expression;
            foreach ($condition->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub->column) . ' ' . $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT EXISTS' : 'EXISTS';
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
            $groupByParts[] = $rawGroup->expression;
            foreach ($rawGroup->bindings as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($groupByParts)) {
            $parts[] = 'GROUP BY ' . \implode(', ', $groupByParts);
        }

        // HAVING
        $havingClauses = [];
        $aliasToExpr = [];
        if (! empty($grouped->aggregations)) {
            foreach ($grouped->aggregations as $agg) {
                /** @var string $alias */
                $alias = $agg->getValue('');
                if ($alias !== '') {
                    $method = $agg->getMethod();
                    $attr = $agg->getAttribute();
                    $col = match (true) {
                        $attr === '*', $attr === '' => '*',
                        \is_numeric($attr) => $attr,
                        default => $this->resolveAndWrap($attr),
                    };
                    if ($method === Method::CountDistinct) {
                        $aliasToExpr[$alias] = 'COUNT(DISTINCT ' . $col . ')';
                    } else {
                        $func = match ($method) {
                            Method::Count => 'COUNT',
                            Method::Sum => 'SUM',
                            Method::Avg => 'AVG',
                            Method::Min => 'MIN',
                            Method::Max => 'MAX',
                            Method::Stddev => 'STDDEV',
                            Method::StddevPop => 'STDDEV_POP',
                            Method::StddevSamp => 'STDDEV_SAMP',
                            Method::Variance => 'VARIANCE',
                            Method::VarPop => 'VAR_POP',
                            Method::VarSamp => 'VAR_SAMP',
                            Method::BitAnd => 'BIT_AND',
                            Method::BitOr => 'BIT_OR',
                            Method::BitXor => 'BIT_XOR',
                            default => $method->value,
                        };
                        $aliasToExpr[$alias] = $func . '(' . $col . ')';
                    }
                }
            }
        }
        if (! empty($grouped->having)) {
            foreach ($grouped->having as $havingQuery) {
                foreach ($havingQuery->getValues() as $subQuery) {
                    /** @var Query $subQuery */
                    $attr = $subQuery->getAttribute();
                    if (isset($aliasToExpr[$attr])) {
                        $havingClauses[] = $this->compileHavingCondition($subQuery, $aliasToExpr[$attr]);
                    } else {
                        $havingClauses[] = $this->compileFilter($subQuery);
                    }
                }
            }
        }
        foreach ($this->rawHavings as $rawHaving) {
            $havingClauses[] = $rawHaving->expression;
            foreach ($rawHaving->bindings as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($havingClauses)) {
            $parts[] = 'HAVING ' . \implode(' AND ', $havingClauses);
        }

        // WINDOW
        if (! empty($this->windowDefinitions)) {
            $windowParts = [];
            foreach ($this->windowDefinitions as $winDef) {
                $overParts = [];
                if ($winDef->partitionBy !== null && $winDef->partitionBy !== []) {
                    $partCols = \array_map(fn (string $col): string => $this->resolveAndWrap($col), $winDef->partitionBy);
                    $overParts[] = 'PARTITION BY ' . \implode(', ', $partCols);
                }
                if ($winDef->orderBy !== null && $winDef->orderBy !== []) {
                    $orderCols = [];
                    foreach ($winDef->orderBy as $col) {
                        if (\str_starts_with($col, '-')) {
                            $orderCols[] = $this->resolveAndWrap(\substr($col, 1)) . ' DESC';
                        } else {
                            $orderCols[] = $this->resolveAndWrap($col) . ' ASC';
                        }
                    }
                    $overParts[] = 'ORDER BY ' . \implode(', ', $orderCols);
                }
                if ($winDef->frame !== null) {
                    $overParts[] = $winDef->frame->toSql();
                }
                $windowParts[] = $this->quote($winDef->name) . ' AS (' . \implode(' ', $overParts) . ')';
            }
            $parts[] = 'WINDOW ' . \implode(', ', $windowParts);
        }

        // ORDER BY
        $orderClauses = [];

        $vectorOrderExpr = $this->compileVectorOrderExpr();
        if ($vectorOrderExpr !== null) {
            $orderClauses[] = $vectorOrderExpr->expression;
            foreach ($vectorOrderExpr->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        foreach ($this->rawOrders as $rawOrder) {
            $orderClauses[] = $rawOrder->expression;
            foreach ($rawOrder->bindings as $binding) {
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

        // FETCH FIRST
        if ($this->fetchCount !== null) {
            $this->addBinding($this->fetchCount);
            $parts[] = $this->fetchWithTies
                ? 'FETCH FIRST ? ROWS WITH TIES'
                : 'FETCH FIRST ? ROWS ONLY';
        }

        // LOCKING
        if ($this->lockMode !== null) {
            $lockSql = $this->lockMode->toSql();
            if ($this->lockOfTable !== null) {
                $lockSql .= ' OF ' . $this->quote($this->lockOfTable);
            }
            $parts[] = $lockSql;
        }

        $sql = \implode(' ', $parts);

        // UNION
        if (! empty($this->unions)) {
            $sql = '(' . $sql . ')';
        }
        foreach ($this->unions as $union) {
            $sql .= ' ' . $union->type->value . ' (' . $union->query . ')';
            foreach ($union->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $sql = $ctePrefix . $sql;

        $result = new BuildResult($sql, $this->bindings, readOnly: true);

        foreach ($this->afterBuildCallbacks as $callback) {
            $result = $callback($result);
        }

        return $result;
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
                if (isset($this->insertColumnExpressions[$col])) {
                    $placeholders[] = $this->insertColumnExpressions[$col];
                    foreach ($this->insertColumnExpressionBindings[$col] ?? [] as $extra) {
                        $bindings[] = $extra;
                    }
                } else {
                    $placeholders[] = '?';
                }
            }
            $rowPlaceholders[] = '(' . \implode(', ', $placeholders) . ')';
        }

        $tablePart = $this->quote($this->table);
        if ($this->insertAlias !== '') {
            $tablePart .= ' AS ' . $this->quote($this->insertAlias);
        }

        $sql = 'INSERT INTO ' . $tablePart
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

    public function insertDefaultValues(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $sql = 'INSERT INTO ' . $this->quote($this->table) . ' DEFAULT VALUES';

        return new BuildResult($sql, $this->bindings);
    }

    /**
     * @return list<string>
     */
    protected function compileAssignments(): array
    {
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
            $assignments[] = $this->resolveAndWrap($col) . ' = ' . $caseData->sql;
            foreach ($caseData->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        return $assignments;
    }

    public function update(): BuildResult
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

        return new BuildResult(\implode(' ', $parts), $this->bindings);
    }

    public function delete(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = ['DELETE FROM ' . $this->quote($this->table)];

        $this->compileWhereClauses($parts, $grouped);

        $this->compileOrderAndLimit($parts, $grouped);

        return new BuildResult(\implode(' ', $parts), $this->bindings);
    }

    /**
     * @param  array<string>  $parts
     */
    protected function compileWhereClauses(array &$parts, ?GroupedQueries $grouped = null): void
    {
        $grouped ??= Query::groupByType($this->pendingQueries);
        $whereClauses = [];

        foreach ($grouped->filters as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->tableAlias ?: $this->table);
            $whereClauses[] = $condition->expression;
            foreach ($condition->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub->column) . ' ' . $prefix . ' (' . $subResult->query . ')';
            foreach ($subResult->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT EXISTS' : 'EXISTS';
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
    protected function compileOrderAndLimit(array &$parts, ?GroupedQueries $grouped = null): void
    {
        $grouped ??= Query::groupByType($this->pendingQueries);

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
            $orderClauses[] = $rawOrder->expression;
            foreach ($rawOrder->bindings as $binding) {
                $this->addBinding($binding);
            }
        }
        if (! empty($orderClauses)) {
            $parts[] = 'ORDER BY ' . \implode(', ', $orderClauses);
        }

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
     */
    protected function compileVectorOrderExpr(): ?Condition
    {
        return null;
    }

    protected function validateTable(): void
    {
        if ($this->noTable) {
            return;
        }
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
        $this->insertColumnExpressions = [];
        $this->insertColumnExpressionBindings = [];
        $this->insertAlias = '';
        $this->lockMode = null;
        $this->lockOfTable = null;
        $this->insertSelectSource = null;
        $this->insertSelectColumns = [];
        $this->ctes = [];
        $this->rawSelects = [];
        $this->windowSelects = [];
        $this->windowDefinitions = [];
        $this->tableSample = null;
        $this->caseSelects = [];
        $this->caseSets = [];
        $this->whereInSubqueries = [];
        $this->subSelects = [];
        $this->fromSubquery = null;
        $this->noTable = false;
        $this->rawOrders = [];
        $this->rawGroups = [];
        $this->rawHavings = [];
        $this->joinBuilders = [];
        $this->existsSubqueries = [];
        $this->lateralJoins = [];
        $this->beforeBuildCallbacks = [];
        $this->afterBuildCallbacks = [];
        $this->fetchCount = null;
        $this->fetchWithTies = false;

        return $this;
    }

    public function clone(): static
    {
        return clone $this;
    }

    public function __clone(): void
    {
        if ($this->insertSelectSource !== null) {
            $this->insertSelectSource = clone $this->insertSelectSource;
        }
        if ($this->fromSubquery !== null) {
            $this->fromSubquery = new SubSelect(clone $this->fromSubquery->subquery, $this->fromSubquery->alias);
        }
        $this->subSelects = \array_map(fn (SubSelect $s) => new SubSelect(clone $s->subquery, $s->alias), $this->subSelects);
        $this->whereInSubqueries = \array_map(fn (WhereInSubquery $s) => new WhereInSubquery($s->column, clone $s->subquery, $s->not), $this->whereInSubqueries);
        $this->existsSubqueries = \array_map(fn (ExistsSubquery $s) => new ExistsSubquery(clone $s->subquery, $s->not), $this->existsSubqueries);
        $this->joinBuilders = \array_map(fn (JoinBuilder $j) => clone $j, $this->joinBuilders);
        $this->pendingQueries = \array_map(fn (Query $q) => clone $q, $this->pendingQueries);
        $this->lateralJoins = \array_map(fn (LateralJoin $l) => new LateralJoin(clone $l->subquery, $l->alias, $l->type), $this->lateralJoins);
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
            Method::ContainsAny => $query->onArray() ? $this->compileIn($attribute, $values) : $this->compileContains($attribute, $values),
            Method::ContainsAll => $this->compileContainsAll($attribute, $values),
            Method::NotContains => $this->compileNotContains($attribute, $values),
            Method::Search => throw new UnsupportedException('Full-text search is not supported by this dialect.'),
            Method::NotSearch => throw new UnsupportedException('Full-text search is not supported by this dialect.'),
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

    protected function compileHavingCondition(Query $query, string $expression): string
    {
        $method = $query->getMethod();
        $values = $query->getValues();

        return match ($method) {
            Method::Equal => $this->compileIn($expression, $values),
            Method::NotEqual => $this->compileNotIn($expression, $values),
            Method::LessThan => $this->compileComparison($expression, '<', $values),
            Method::LessThanEqual => $this->compileComparison($expression, '<=', $values),
            Method::GreaterThan => $this->compileComparison($expression, '>', $values),
            Method::GreaterThanEqual => $this->compileComparison($expression, '>=', $values),
            Method::Between => $this->compileBetween($expression, $values, false),
            Method::NotBetween => $this->compileBetween($expression, $values, true),
            Method::IsNull => $expression . ' IS NULL',
            Method::IsNotNull => $expression . ' IS NOT NULL',
            default => throw new UnsupportedException('Unsupported HAVING condition type: ' . $method->value),
        };
    }

    public function compileOrder(Query $query): string
    {
        $sql = match ($query->getMethod()) {
            Method::OrderAsc => $this->resolveAndWrap($query->getAttribute()) . ' ASC',
            Method::OrderDesc => $this->resolveAndWrap($query->getAttribute()) . ' DESC',
            Method::OrderRandom => $this->compileRandom(),
            default => throw new UnsupportedException('Unsupported order type: ' . $query->getMethod()->value),
        };

        $nulls = $query->getValue(null);
        if ($nulls instanceof NullsPosition) {
            $sql .= ' NULLS ' . $nulls->value;
        }

        return $sql;
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
            Method::Stddev => 'STDDEV',
            Method::StddevPop => 'STDDEV_POP',
            Method::StddevSamp => 'STDDEV_SAMP',
            Method::Variance => 'VARIANCE',
            Method::VarPop => 'VAR_POP',
            Method::VarSamp => 'VAR_SAMP',
            Method::BitAnd => 'BIT_AND',
            Method::BitOr => 'BIT_OR',
            Method::BitXor => 'BIT_XOR',
            default => throw new ValidationException("Unknown aggregate: {$method->value}"),
        };
        $attr = $query->getAttribute();
        $col = match (true) {
            $attr === '*', $attr === '' => '*',
            \is_numeric($attr) => $attr,
            default => $this->resolveAndWrap($attr),
        };
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
            Method::FullOuterJoin => 'FULL OUTER JOIN',
            Method::NaturalJoin => 'NATURAL JOIN',
            default => throw new UnsupportedException('Unsupported join type: ' . $query->getMethod()->value),
        };

        $table = $this->quote($query->getAttribute());
        $values = $query->getValues();

        // Handle alias for cross join and natural join (alias is values[0])
        if ($query->getMethod() === Method::CrossJoin || $query->getMethod() === Method::NaturalJoin) {
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
            Method::FullOuterJoin => 'FULL OUTER JOIN',
            Method::NaturalJoin => 'NATURAL JOIN',
            default => throw new UnsupportedException('Unsupported join type: ' . $query->getMethod()->value),
        };

        $table = $this->quote($query->getAttribute());
        $values = $query->getValues();

        // Handle alias
        if ($query->getMethod() === Method::CrossJoin || $query->getMethod() === Method::NaturalJoin) {
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

        foreach ($joinBuilder->ons as $on) {
            $left = $this->resolveAndWrap($on->left);
            $right = $this->resolveAndWrap($on->right);
            $onParts[] = $left . ' ' . $on->operator . ' ' . $right;
        }

        foreach ($joinBuilder->wheres as $where) {
            $onParts[] = $where->expression;
            foreach ($where->bindings as $binding) {
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
        $resolved = $this->resolveAttribute($attribute);

        if ($this->qualifyColumns
            && $resolved !== '*'
            && ! \str_contains($resolved, '.')
            && ! isset($this->aggregationAliases[$resolved])
        ) {
            $resolved = $this->tableAlias . '.' . $resolved;
        }

        return $this->quote($resolved);
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
        $like = $this->getLikeKeyword();
        $keyword = $not ? 'NOT ' . $like : $like;

        return $attribute . ' ' . $keyword . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileContains(string $attribute, array $values): string
    {
        $like = $this->getLikeKeyword();
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' ' . $like . ' ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' ' . $like . ' ?';
        }

        return '(' . \implode(' OR ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileContainsAll(string $attribute, array $values): string
    {
        $like = $this->getLikeKeyword();
        /** @var array<string> $values */
        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' ' . $like . ' ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileNotContains(string $attribute, array $values): string
    {
        $like = $this->getLikeKeyword();
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' NOT ' . $like . ' ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' NOT ' . $like . ' ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    protected function getLikeKeyword(): string
    {
        return 'LIKE';
    }

    /**
     * Escape LIKE metacharacters in user input before wrapping with wildcards.
     */
    protected function escapeLikeValue(mixed $value): string
    {
        if (\is_array($value)) {
            $value = \json_encode($value) ?: '';
        } elseif (\is_int($value) || \is_float($value) || \is_bool($value)) {
            $value = (string) $value;
        } elseif (!\is_string($value)) {
            $value = '';
        }

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
    protected function compileIn(string $attribute, array $values): string
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
    protected function compileNotIn(string $attribute, array $values): string
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
    protected function compileComparison(string $attribute, string $operator, array $values): string
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

    public function toAst(): SelectStatement
    {
        $grouped = Query::groupByType($this->pendingQueries);

        $columns = $this->buildAstColumns($grouped);
        $from = $this->buildAstFrom();
        $joins = $this->buildAstJoins($grouped);
        $where = $this->buildAstWhere($grouped);
        $groupByExprs = $this->buildAstGroupBy($grouped);
        $having = $this->buildAstHaving($grouped);
        $orderByItems = $this->buildAstOrderBy();
        $limit = $grouped->limit !== null ? new Literal($grouped->limit) : null;
        $offset = $grouped->offset !== null ? new Literal($grouped->offset) : null;
        $cteDefinitions = $this->buildAstCtes();

        return new SelectStatement(
            columns: $columns,
            from: $from,
            joins: $joins,
            where: $where,
            groupBy: $groupByExprs,
            having: $having,
            orderBy: $orderByItems,
            limit: $limit,
            offset: $offset,
            distinct: $grouped->distinct,
            ctes: $cteDefinitions,
        );
    }

    /**
     * @return Expr[]
     */
    private function buildAstColumns(GroupedQueries $grouped): array
    {
        $columns = [];

        foreach ($grouped->aggregations as $agg) {
            $columns[] = $this->aggregateQueryToAstExpr($agg);
        }

        if (!empty($grouped->selections)) {
            /** @var array<string> $selectedCols */
            $selectedCols = $grouped->selections[0]->getValues();
            foreach ($selectedCols as $col) {
                $columns[] = $this->columnNameToAstExpr($col);
            }
        }

        if (empty($columns)) {
            $columns[] = new Star();
        }

        return $columns;
    }

    private function columnNameToAstExpr(string $col): Expr
    {
        if ($col === '*') {
            return new Star();
        }

        if (\str_contains($col, '.')) {
            $parts = \explode('.', $col, 3);
            if (\count($parts) === 3) {
                if ($parts[2] === '*') {
                    return new Star($parts[1], $parts[0]);
                }
                return new ColumnRef($parts[2], $parts[1], $parts[0]);
            }
            if ($parts[1] === '*') {
                return new Star($parts[0]);
            }
            return new ColumnRef($parts[1], $parts[0]);
        }

        return new ColumnRef($col);
    }

    private function aggregateQueryToAstExpr(Query $query): Expr
    {
        $method = $query->getMethod();
        $attr = $query->getAttribute();
        /** @var string $alias */
        $alias = $query->getValue('');

        $funcName = match ($method) {
            Method::Count => 'COUNT',
            Method::CountDistinct => 'COUNT',
            Method::Sum => 'SUM',
            Method::Avg => 'AVG',
            Method::Min => 'MIN',
            Method::Max => 'MAX',
            Method::Stddev => 'STDDEV',
            Method::StddevPop => 'STDDEV_POP',
            Method::StddevSamp => 'STDDEV_SAMP',
            Method::Variance => 'VARIANCE',
            Method::VarPop => 'VAR_POP',
            Method::VarSamp => 'VAR_SAMP',
            Method::BitAnd => 'BIT_AND',
            Method::BitOr => 'BIT_OR',
            Method::BitXor => 'BIT_XOR',
            default => \strtoupper($method->value),
        };

        $arg = ($attr === '*' || $attr === '') ? new Star() : new ColumnRef($attr);
        $distinct = $method === Method::CountDistinct;

        $funcCall = new FunctionCall($funcName, [$arg], $distinct);

        if ($alias !== '') {
            return new AliasedExpr($funcCall, $alias);
        }

        return $funcCall;
    }

    private function buildAstFrom(): TableRef|SubquerySource|null
    {
        if ($this->noTable) {
            return null;
        }

        if ($this->table === '') {
            return null;
        }

        $alias = $this->tableAlias !== '' ? $this->tableAlias : null;
        return new TableRef($this->table, $alias);
    }

    /**
     * @return AstJoinClause[]
     */
    private function buildAstJoins(GroupedQueries $grouped): array
    {
        $joins = [];

        foreach ($grouped->joins as $joinQuery) {
            $joinMethod = $joinQuery->getMethod();
            $table = $joinQuery->getAttribute();
            $values = $joinQuery->getValues();

            $type = match ($joinMethod) {
                Method::Join => 'JOIN',
                Method::LeftJoin => 'LEFT JOIN',
                Method::RightJoin => 'RIGHT JOIN',
                Method::CrossJoin => 'CROSS JOIN',
                Method::FullOuterJoin => 'FULL OUTER JOIN',
                Method::NaturalJoin => 'NATURAL JOIN',
                default => 'JOIN',
            };

            $isCrossOrNatural = $joinMethod === Method::CrossJoin || $joinMethod === Method::NaturalJoin;

            if ($isCrossOrNatural) {
                /** @var string $joinAlias */
                $joinAlias = $values[0] ?? '';
                $tableRef = new TableRef($table, $joinAlias !== '' ? $joinAlias : null);
                $joins[] = new AstJoinClause($type, $tableRef, null);
            } else {
                /** @var string $leftCol */
                $leftCol = $values[0] ?? '';
                /** @var string $operator */
                $operator = $values[1] ?? '=';
                /** @var string $rightCol */
                $rightCol = $values[2] ?? '';
                /** @var string $joinAlias */
                $joinAlias = $values[3] ?? '';

                $tableRef = new TableRef($table, $joinAlias !== '' ? $joinAlias : null);

                $condition = null;
                if ($leftCol !== '' && $rightCol !== '') {
                    $condition = new BinaryExpr(
                        $this->columnNameToAstExpr($leftCol),
                        $operator,
                        $this->columnNameToAstExpr($rightCol),
                    );
                }

                $joins[] = new AstJoinClause($type, $tableRef, $condition);
            }
        }

        return $joins;
    }

    private function buildAstWhere(GroupedQueries $grouped): ?Expr
    {
        if (empty($grouped->filters)) {
            return null;
        }

        $exprs = [];
        foreach ($grouped->filters as $filter) {
            $exprs[] = $this->queryToAstExpr($filter);
        }

        return $this->combineAstExprs($exprs, 'AND');
    }

    private function queryToAstExpr(Query $query): Expr
    {
        $method = $query->getMethod();
        $attr = $query->getAttribute();
        $values = $query->getValues();

        return match ($method) {
            Method::Equal => $this->buildEqualAstExpr($attr, $values),
            Method::NotEqual => $this->buildNotEqualAstExpr($attr, $values),
            Method::GreaterThan => new BinaryExpr(new ColumnRef($attr), '>', new Literal($values[0] ?? null)),
            Method::GreaterThanEqual => new BinaryExpr(new ColumnRef($attr), '>=', new Literal($values[0] ?? null)),
            Method::LessThan => new BinaryExpr(new ColumnRef($attr), '<', new Literal($values[0] ?? null)),
            Method::LessThanEqual => new BinaryExpr(new ColumnRef($attr), '<=', new Literal($values[0] ?? null)),
            Method::Between => new BetweenExpr(new ColumnRef($attr), new Literal($values[0] ?? null), new Literal($values[1] ?? null)),
            Method::NotBetween => new BetweenExpr(new ColumnRef($attr), new Literal($values[0] ?? null), new Literal($values[1] ?? null), true),
            Method::IsNull => new UnaryExpr('IS NULL', new ColumnRef($attr), false),
            Method::IsNotNull => new UnaryExpr('IS NOT NULL', new ColumnRef($attr), false),
            Method::Contains => $this->buildContainsAstExpr($attr, $values, false),
            Method::ContainsAny => $this->buildContainsAstExpr($attr, $values, false),
            Method::NotContains => $this->buildContainsAstExpr($attr, $values, true),
            Method::StartsWith => new BinaryExpr(new ColumnRef($attr), 'LIKE', new Literal(($values[0] ?? '') . '%')),
            Method::NotStartsWith => new BinaryExpr(new ColumnRef($attr), 'NOT LIKE', new Literal(($values[0] ?? '') . '%')),
            Method::EndsWith => new BinaryExpr(new ColumnRef($attr), 'LIKE', new Literal('%' . ($values[0] ?? ''))),
            Method::NotEndsWith => new BinaryExpr(new ColumnRef($attr), 'NOT LIKE', new Literal('%' . ($values[0] ?? ''))),
            Method::And => $this->buildLogicalAstExpr($query, 'AND'),
            Method::Or => $this->buildLogicalAstExpr($query, 'OR'),
            Method::Raw => new Raw($attr),
            default => new Raw($attr !== '' ? $attr : '1 = 1'),
        };
    }

    /**
     * @param array<mixed> $values
     */
    private function buildEqualAstExpr(string $attr, array $values): Expr
    {
        if (\count($values) === 1) {
            if ($values[0] === null) {
                return new UnaryExpr('IS NULL', new ColumnRef($attr), false);
            }
            return new BinaryExpr(new ColumnRef($attr), '=', new Literal($values[0]));
        }

        $literals = \array_map(fn ($v) => new Literal($v), $values);
        return new InExpr(new ColumnRef($attr), $literals);
    }

    /**
     * @param array<mixed> $values
     */
    private function buildNotEqualAstExpr(string $attr, array $values): Expr
    {
        if (\count($values) === 1) {
            if ($values[0] === null) {
                return new UnaryExpr('IS NOT NULL', new ColumnRef($attr), false);
            }
            return new BinaryExpr(new ColumnRef($attr), '!=', new Literal($values[0]));
        }

        $literals = \array_map(fn ($v) => new Literal($v), $values);
        return new InExpr(new ColumnRef($attr), $literals, true);
    }

    /**
     * @param array<mixed> $values
     */
    private function buildContainsAstExpr(string $attr, array $values, bool $negated): Expr
    {
        if (\count($values) === 1) {
            $op = $negated ? 'NOT LIKE' : 'LIKE';
            return new BinaryExpr(new ColumnRef($attr), $op, new Literal('%' . $values[0] . '%'));
        }

        $parts = [];
        $op = $negated ? 'NOT LIKE' : 'LIKE';
        foreach ($values as $value) {
            $parts[] = new BinaryExpr(new ColumnRef($attr), $op, new Literal('%' . $value . '%'));
        }

        $combinator = $negated ? 'AND' : 'OR';
        return $this->combineAstExprs($parts, $combinator);
    }

    private function buildLogicalAstExpr(Query $query, string $operator): Expr
    {
        $parts = [];
        foreach ($query->getValues() as $subQuery) {
            if ($subQuery instanceof Query) {
                $parts[] = $this->queryToAstExpr($subQuery);
            }
        }

        if (empty($parts)) {
            return new Literal($operator === 'OR' ? false : true);
        }

        return $this->combineAstExprs($parts, $operator);
    }

    /**
     * @param Expr[] $exprs
     */
    private function combineAstExprs(array $exprs, string $operator): Expr
    {
        if (\count($exprs) === 1) {
            return $exprs[0];
        }

        $result = $exprs[0];
        for ($i = 1; $i < \count($exprs); $i++) {
            $result = new BinaryExpr($result, $operator, $exprs[$i]);
        }

        return $result;
    }

    /**
     * @return Expr[]
     */
    private function buildAstGroupBy(GroupedQueries $grouped): array
    {
        $exprs = [];
        foreach ($grouped->groupBy as $col) {
            $exprs[] = $this->columnNameToAstExpr($col);
        }
        return $exprs;
    }

    private function buildAstHaving(GroupedQueries $grouped): ?Expr
    {
        if (empty($grouped->having)) {
            return null;
        }

        $parts = [];
        foreach ($grouped->having as $havingQuery) {
            foreach ($havingQuery->getValues() as $subQuery) {
                if ($subQuery instanceof Query) {
                    $parts[] = $this->queryToAstExpr($subQuery);
                }
            }
        }

        if (empty($parts)) {
            return null;
        }

        return $this->combineAstExprs($parts, 'AND');
    }

    /**
     * @return OrderByItem[]
     */
    private function buildAstOrderBy(): array
    {
        $items = [];
        $orderQueries = Query::getByType($this->pendingQueries, [
            Method::OrderAsc,
            Method::OrderDesc,
            Method::OrderRandom,
        ], false);

        foreach ($orderQueries as $orderQuery) {
            $method = $orderQuery->getMethod();

            if ($method === Method::OrderRandom) {
                $items[] = new OrderByItem(new Raw('RAND()'), 'ASC');
                continue;
            }

            $direction = $method === Method::OrderAsc ? 'ASC' : 'DESC';
            $attr = $orderQuery->getAttribute();
            $expr = $this->columnNameToAstExpr($attr);

            $nulls = null;
            $nullsVal = $orderQuery->getValue(null);
            if ($nullsVal instanceof NullsPosition) {
                $nulls = $nullsVal->value;
            }

            $items[] = new OrderByItem($expr, $direction, $nulls);
        }

        return $items;
    }

    /**
     * @return CteDefinition[]
     */
    private function buildAstCtes(): array
    {
        $defs = [];
        foreach ($this->ctes as $cte) {
            $innerStmt = $this->parseSqlToAst($cte->query);
            $defs[] = new CteDefinition($cte->name, $innerStmt, $cte->columns, $cte->recursive);
        }
        return $defs;
    }

    private function parseSqlToAst(string $sql): SelectStatement
    {
        $tokenizer = new \Utopia\Query\Tokenizer\Tokenizer();
        $tokens = \Utopia\Query\Tokenizer\Tokenizer::filter($tokenizer->tokenize($sql));
        $parser = new \Utopia\Query\AST\Parser();
        return $parser->parse($tokens);
    }

    public static function fromAst(SelectStatement $ast): static
    {
        $builder = new static();

        if ($ast->from instanceof TableRef) {
            $builder->from($ast->from->name, $ast->from->alias ?? '');
        }

        $builder->applyAstColumns($ast);
        $builder->applyAstJoins($ast);
        $builder->applyAstWhere($ast);
        $builder->applyAstGroupBy($ast);
        $builder->applyAstHaving($ast);
        $builder->applyAstOrderBy($ast);
        $builder->applyAstLimitOffset($ast);
        $builder->applyAstCtes($ast);

        if ($ast->distinct) {
            $builder->distinct();
        }

        return $builder;
    }

    private function applyAstColumns(SelectStatement $ast): void
    {
        $selectCols = [];
        $hasNonStar = false;

        foreach ($ast->columns as $col) {
            if ($col instanceof Star && $col->table === null) {
                continue;
            }

            if ($col instanceof AliasedExpr && $col->expr instanceof FunctionCall) {
                $this->applyAstAggregateColumn($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof FunctionCall) {
                $this->applyAstUnaliasedFunctionColumn($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof ColumnRef) {
                $selectCols[] = $this->astColumnRefToString($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof Star) {
                $selectCols[] = $col->table !== null ? $col->table . '.*' : '*';
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof AliasedExpr && $col->expr instanceof ColumnRef) {
                $selectCols[] = $this->astColumnRefToString($col->expr);
                $hasNonStar = true;
                continue;
            }

            $serializer = new Serializer();
            $this->selectRaw($serializer->serializeExpr($col));
            $hasNonStar = true;
        }

        if (!empty($selectCols)) {
            $this->select($selectCols);
        }
    }

    private function applyAstAggregateColumn(AliasedExpr $aliased): void
    {
        $fn = $aliased->expr;
        if (!$fn instanceof FunctionCall) {
            return;
        }

        $name = \strtoupper($fn->name);
        $attr = $this->astFuncArgToAttribute($fn);
        $alias = $aliased->alias;

        if ($fn->distinct && $name === 'COUNT') {
            $this->pendingQueries[] = Query::countDistinct($attr, $alias);
            return;
        }

        $method = match ($name) {
            'COUNT' => Method::Count,
            'SUM' => Method::Sum,
            'AVG' => Method::Avg,
            'MIN' => Method::Min,
            'MAX' => Method::Max,
            'STDDEV' => Method::Stddev,
            'STDDEV_POP' => Method::StddevPop,
            'STDDEV_SAMP' => Method::StddevSamp,
            'VARIANCE' => Method::Variance,
            'VAR_POP' => Method::VarPop,
            'VAR_SAMP' => Method::VarSamp,
            'BIT_AND' => Method::BitAnd,
            'BIT_OR' => Method::BitOr,
            'BIT_XOR' => Method::BitXor,
            default => null,
        };

        if ($method !== null) {
            $this->pendingQueries[] = new Query($method, $attr, $alias !== '' ? [$alias] : []);
            return;
        }

        $serializer = new Serializer();
        $this->selectRaw($serializer->serializeExpr($aliased));
    }

    private function applyAstUnaliasedFunctionColumn(FunctionCall $fn): void
    {
        $name = \strtoupper($fn->name);
        $attr = $this->astFuncArgToAttribute($fn);

        if ($fn->distinct && $name === 'COUNT') {
            $this->pendingQueries[] = Query::countDistinct($attr);
            return;
        }

        $method = match ($name) {
            'COUNT' => Method::Count,
            'SUM' => Method::Sum,
            'AVG' => Method::Avg,
            'MIN' => Method::Min,
            'MAX' => Method::Max,
            default => null,
        };

        if ($method !== null) {
            $this->pendingQueries[] = new Query($method, $attr, []);
            return;
        }

        $serializer = new Serializer();
        $this->selectRaw($serializer->serializeExpr($fn));
    }

    private function astFuncArgToAttribute(FunctionCall $fn): string
    {
        if (empty($fn->arguments)) {
            return '*';
        }

        $firstArg = $fn->arguments[0];
        if ($firstArg instanceof Star) {
            return '*';
        }
        if ($firstArg instanceof ColumnRef) {
            return $this->astColumnRefToString($firstArg);
        }

        return '*';
    }

    private function astColumnRefToString(ColumnRef $ref): string
    {
        $parts = [];
        if ($ref->schema !== null) {
            $parts[] = $ref->schema;
        }
        if ($ref->table !== null) {
            $parts[] = $ref->table;
        }
        $parts[] = $ref->name;
        return \implode('.', $parts);
    }

    private function applyAstJoins(SelectStatement $ast): void
    {
        foreach ($ast->joins as $join) {
            if (!$join->table instanceof TableRef) {
                continue;
            }

            $table = $join->table->name;
            $alias = $join->table->alias ?? '';
            $type = \strtoupper($join->type);

            if ($type === 'CROSS JOIN') {
                $this->crossJoin($table, $alias);
                continue;
            }

            if ($type === 'NATURAL JOIN') {
                $this->naturalJoin($table, $alias);
                continue;
            }

            $leftCol = '';
            $operator = '=';
            $rightCol = '';

            if ($join->condition instanceof BinaryExpr) {
                $leftCol = $this->astExprToColumnString($join->condition->left);
                $operator = $join->condition->operator;
                $rightCol = $this->astExprToColumnString($join->condition->right);
            }

            $method = match ($type) {
                'LEFT JOIN', 'LEFT OUTER JOIN' => Method::LeftJoin,
                'RIGHT JOIN', 'RIGHT OUTER JOIN' => Method::RightJoin,
                'FULL OUTER JOIN', 'FULL JOIN' => Method::FullOuterJoin,
                'INNER JOIN', 'JOIN' => Method::Join,
                default => Method::Join,
            };

            $values = [$leftCol, $operator, $rightCol];
            if ($alias !== '') {
                $values[] = $alias;
            }
            $this->pendingQueries[] = new Query($method, $table, $values);
        }
    }

    private function astExprToColumnString(Expr $expr): string
    {
        if ($expr instanceof ColumnRef) {
            return $this->astColumnRefToString($expr);
        }

        $serializer = new Serializer();
        return $serializer->serializeExpr($expr);
    }

    private function applyAstWhere(SelectStatement $ast): void
    {
        if ($ast->where === null) {
            return;
        }

        $queries = $this->astWhereToQueries($ast->where);
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }
    }

    /**
     * @return Query[]
     */
    private function astWhereToQueries(Expr $expr): array
    {
        if ($expr instanceof BinaryExpr && \strtoupper($expr->operator) === 'AND') {
            $left = $this->astWhereToQueries($expr->left);
            $right = $this->astWhereToQueries($expr->right);
            return \array_merge($left, $right);
        }

        $query = $this->astExprToSingleQuery($expr);
        if ($query !== null) {
            return [$query];
        }

        $serializer = new Serializer();
        return [Query::raw($serializer->serializeExpr($expr))];
    }

    private function astExprToSingleQuery(Expr $expr): ?Query
    {
        if ($expr instanceof BinaryExpr) {
            $op = \strtoupper($expr->operator);

            if ($op === 'AND') {
                $leftQueries = $this->astWhereToQueries($expr->left);
                $rightQueries = $this->astWhereToQueries($expr->right);
                $all = \array_merge($leftQueries, $rightQueries);
                return Query::and($all);
            }

            if ($op === 'OR') {
                $leftQ = $this->astExprToSingleQuery($expr->left);
                $rightQ = $this->astExprToSingleQuery($expr->right);
                $parts = [];
                if ($leftQ !== null) {
                    $parts[] = $leftQ;
                }
                if ($rightQ !== null) {
                    $parts[] = $rightQ;
                }
                if (!empty($parts)) {
                    return Query::or($parts);
                }
                return null;
            }

            if ($expr->left instanceof ColumnRef && $expr->right instanceof Literal) {
                $attr = $this->astColumnRefToString($expr->left);
                $val = $expr->right->value;

                return match ($op) {
                    '=' => Query::equal($attr, [$val]),
                    '!=' , '<>' => Query::notEqual($attr, $val),
                    '>' => Query::greaterThan($attr, $val),
                    '>=' => Query::greaterThanEqual($attr, $val),
                    '<' => Query::lessThan($attr, $val),
                    '<=' => Query::lessThanEqual($attr, $val),
                    'LIKE' => $this->likeToQuery($attr, $val),
                    'NOT LIKE' => $this->notLikeToQuery($attr, $val),
                    default => null,
                };
            }
        }

        if ($expr instanceof InExpr && $expr->expr instanceof ColumnRef && \is_array($expr->list)) {
            $attr = $this->astColumnRefToString($expr->expr);
            $values = \array_map(fn (Expr $item) => $item instanceof Literal ? $item->value : null, $expr->list);
            if ($expr->negated) {
                return Query::notEqual($attr, $values);
            }
            return Query::equal($attr, $values);
        }

        if ($expr instanceof BetweenExpr && $expr->expr instanceof ColumnRef) {
            $attr = $this->astColumnRefToString($expr->expr);
            $low = $expr->low instanceof Literal ? $expr->low->value : 0;
            $high = $expr->high instanceof Literal ? $expr->high->value : 0;
            if ($expr->negated) {
                return Query::notBetween($attr, $low, $high);
            }
            return Query::between($attr, $low, $high);
        }

        if ($expr instanceof UnaryExpr) {
            $op = \strtoupper($expr->operator);
            if ($expr->operand instanceof ColumnRef) {
                $attr = $this->astColumnRefToString($expr->operand);
                return match ($op) {
                    'IS NULL' => Query::isNull($attr),
                    'IS NOT NULL' => Query::isNotNull($attr),
                    default => null,
                };
            }
        }

        return null;
    }

    private function likeToQuery(string $attr, mixed $val): Query
    {
        $str = (string) $val;
        if (\str_starts_with($str, '%') && \str_ends_with($str, '%') && \strlen($str) > 2) {
            return new Query(Method::Contains, $attr, [\substr($str, 1, -1)]);
        }
        if (\str_ends_with($str, '%') && !\str_starts_with($str, '%')) {
            return Query::startsWith($attr, \substr($str, 0, -1));
        }
        if (\str_starts_with($str, '%') && !\str_ends_with($str, '%')) {
            return Query::endsWith($attr, \substr($str, 1));
        }
        return Query::raw($attr . ' LIKE ?', [$val]);
    }

    private function notLikeToQuery(string $attr, mixed $val): Query
    {
        $str = (string) $val;
        if (\str_starts_with($str, '%') && \str_ends_with($str, '%') && \strlen($str) > 2) {
            return new Query(Method::NotContains, $attr, [\substr($str, 1, -1)]);
        }
        if (\str_ends_with($str, '%') && !\str_starts_with($str, '%')) {
            return Query::notStartsWith($attr, \substr($str, 0, -1));
        }
        if (\str_starts_with($str, '%') && !\str_ends_with($str, '%')) {
            return Query::notEndsWith($attr, \substr($str, 1));
        }
        return Query::raw($attr . ' NOT LIKE ?', [$val]);
    }

    private function applyAstGroupBy(SelectStatement $ast): void
    {
        if (empty($ast->groupBy)) {
            return;
        }

        $cols = [];
        foreach ($ast->groupBy as $expr) {
            if ($expr instanceof ColumnRef) {
                $cols[] = $this->astColumnRefToString($expr);
            }
        }

        if (!empty($cols)) {
            $this->groupBy($cols);
        }
    }

    private function applyAstHaving(SelectStatement $ast): void
    {
        if ($ast->having === null) {
            return;
        }

        $queries = $this->astWhereToQueries($ast->having);
        if (!empty($queries)) {
            $this->having($queries);
        }
    }

    private function applyAstOrderBy(SelectStatement $ast): void
    {
        foreach ($ast->orderBy as $item) {
            if ($item->expr instanceof ColumnRef) {
                $attr = $this->astColumnRefToString($item->expr);
                $nulls = null;
                if ($item->nulls !== null) {
                    $nulls = NullsPosition::tryFrom($item->nulls);
                }

                if (\strtoupper($item->direction) === 'DESC') {
                    $this->sortDesc($attr, $nulls);
                } else {
                    $this->sortAsc($attr, $nulls);
                }
            } else {
                $serializer = new Serializer();
                $rawExpr = $serializer->serializeExpr($item->expr);
                $dir = \strtoupper($item->direction) === 'DESC' ? ' DESC' : ' ASC';
                $this->orderByRaw($rawExpr . $dir);
            }
        }
    }

    private function applyAstLimitOffset(SelectStatement $ast): void
    {
        if ($ast->limit instanceof Literal && ($ast->limit->value !== null)) {
            $this->limit((int) $ast->limit->value);
        }

        if ($ast->offset instanceof Literal && ($ast->offset->value !== null)) {
            $this->offset((int) $ast->offset->value);
        }
    }

    private function applyAstCtes(SelectStatement $ast): void
    {
        foreach ($ast->ctes as $cte) {
            $serializer = new Serializer();
            $cteSql = $serializer->serialize($cte->query);

            $this->ctes[] = new CteClause(
                $cte->name,
                $cteSql,
                [],
                $cte->recursive,
                $cte->columns,
            );
        }
    }
}
