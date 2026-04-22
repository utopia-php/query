<?php

namespace Utopia\Query;

use Closure;
use Utopia\Query\AST\Call\Func;
use Utopia\Query\AST\Definition\Cte as CteDefinition;
use Utopia\Query\AST\Expression;
use Utopia\Query\AST\Expression\Aliased;
use Utopia\Query\AST\Expression\Between;
use Utopia\Query\AST\Expression\Binary;
use Utopia\Query\AST\Expression\In;
use Utopia\Query\AST\Expression\Unary;
use Utopia\Query\AST\JoinClause as AstJoinClause;
use Utopia\Query\AST\Literal;
use Utopia\Query\AST\OrderByItem;
use Utopia\Query\AST\Parser;
use Utopia\Query\AST\Raw;
use Utopia\Query\AST\Reference\Column;
use Utopia\Query\AST\Reference\Table;
use Utopia\Query\AST\Serializer;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\Statement\Select;
use Utopia\Query\Builder\Case\Expression as CaseExpression;
use Utopia\Query\Builder\Case\Kind as CaseKind;
use Utopia\Query\Builder\Case\WhenClause;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\CteClause;
use Utopia\Query\Builder\ExistsSubquery;
use Utopia\Query\Builder\Feature;
use Utopia\Query\Builder\GroupedQueries;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\LateralJoin;
use Utopia\Query\Builder\LockMode;
use Utopia\Query\Builder\Plan;
use Utopia\Query\Builder\SubSelect;
use Utopia\Query\Builder\UnionClause;
use Utopia\Query\Builder\UnionType;
use Utopia\Query\Builder\WhereInSubquery;
use Utopia\Query\Builder\WindowDefinition;
use Utopia\Query\Builder\WindowFrame;
use Utopia\Query\Builder\WindowSelect;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Attribute;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;
use Utopia\Query\Tokenizer\Tokenizer;

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

    protected string $alias = '';

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
    protected array $rows = [];

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
    protected ?array $sample = null;

    /** @var list<CaseExpression> */
    protected array $cases = [];

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

    protected bool $tableless = false;

    /** @var list<Condition> */
    protected array $rawOrders = [];

    /** @var list<Condition> */
    protected array $rawGroups = [];

    /** @var list<Condition> */
    protected array $rawHavings = [];

    /** @var array<int, JoinBuilder> */
    protected array $joins = [];

    /** @var list<ExistsSubquery> */
    protected array $existsSubqueries = [];

    /** @var list<LateralJoin> */
    protected array $lateralJoins = [];

    /** @var list<Closure> */
    protected array $beforeBuildCallbacks = [];

    /** @var list<Closure(Plan): Plan> */
    protected array $afterBuildCallbacks = [];

    /** @var (\Closure(Plan): (array<mixed>|int))|null */
    protected ?\Closure $executor = null;

    protected bool $qualify = false;

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
        if ($this->tableless) {
            return '';
        }

        $fromSub = $this->fromSubquery;
        if ($fromSub !== null) {
            $subResult = $fromSub->subquery->build();
            $this->addBindings($subResult->bindings);

            return 'FROM (' . $subResult->query . ') AS ' . $this->quote($fromSub->alias);
        }

        $sql = 'FROM ' . $this->quote($this->table);

        if ($this->alias !== '') {
            $sql .= ' AS ' . $this->quote($this->alias);
        }

        if ($this->sample !== null) {
            $sql .= ' TABLESAMPLE ' . $this->sample['method'] . '(' . $this->sample['percent'] . ')';
        }

        return $sql;
    }

    /**
     * Hook called after JOIN clauses and before WHERE. Override to inject
     * dialect-specific clauses such as PREWHERE (ClickHouse) or ARRAY JOIN.
     * Implementations must add any bindings they emit via $this->addBindings()
     * at the moment their fragment is emitted so ordering is preserved.
     */
    protected function buildAfterJoinsClause(GroupedQueries $grouped): string
    {
        return '';
    }

    /**
     * Hook called after GROUP BY and before HAVING. Override to emit
     * dialect-specific group-by modifiers (e.g. ClickHouse WITH TOTALS).
     */
    protected function buildAfterGroupByClause(): string
    {
        return '';
    }

    /**
     * Hook called after ORDER BY and before LIMIT. Override to emit
     * dialect-specific clauses that bind between ordering and pagination
     * (e.g. ClickHouse LIMIT BY).
     */
    protected function buildAfterOrderByClause(): string
    {
        return '';
    }

    /**
     * Hook called at the very end of the SELECT statement (just before any
     * UNION suffix). Override to emit dialect-specific settings fragments
     * (e.g. ClickHouse SETTINGS).
     */
    protected function buildSettingsClause(): string
    {
        return '';
    }

    #[\Override]
    public function from(string $table = '', string $alias = ''): static
    {
        $this->table = $table;
        $this->alias = $alias;
        $this->fromSubquery = null;
        $this->tableless = ($table === '');

        return $this;
    }

    public function fromNone(): static
    {
        return $this->from('');
    }

    #[\Override]
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
    #[\Override]
    public function set(array $row): static
    {
        $this->rows[] = $row;

        return $this;
    }

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

    /**
     * @param  string[]  $keys
     * @param  string[]  $updateColumns
     */
    #[\Override]
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

    #[\Override]
    public function countDistinct(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::countDistinct($attribute, $alias);

        return $this;
    }

    /**
     * @param  \Closure(JoinBuilder): void  $callback
     */
    #[\Override]
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
        $this->joins[$index] = $joinBuilder;

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

    public function explain(bool $analyze = false): Plan
    {
        $result = $this->build();
        $prefix = $analyze ? 'EXPLAIN ANALYZE ' : 'EXPLAIN ';

        return new Plan($prefix . $result->query, $result->bindings, readOnly: true, executor: $this->executor);
    }

    /**
     * @param  string|array<string>  $columns
     * @param  list<mixed>  $bindings
     */
    #[\Override]
    public function select(string|array $columns, array $bindings = []): static
    {
        if (\is_string($columns)) {
            $this->rawSelects[] = new Condition($columns, $bindings);
        } else {
            $this->pendingQueries[] = Query::select($columns);
        }

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function selectRaw(string $expression, array $bindings = []): static
    {
        return $this->select($expression, $bindings);
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function filter(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    #[\Override]
    public function sortAsc(string $attribute, ?NullsPosition $nulls = null): static
    {
        $this->pendingQueries[] = Query::orderAsc($attribute, $nulls);

        return $this;
    }

    #[\Override]
    public function sortDesc(string $attribute, ?NullsPosition $nulls = null): static
    {
        $this->pendingQueries[] = Query::orderDesc($attribute, $nulls);

        return $this;
    }

    #[\Override]
    public function sortRandom(): static
    {
        $this->pendingQueries[] = Query::orderRandom();

        return $this;
    }

    #[\Override]
    public function limit(int $value): static
    {
        $this->pendingQueries[] = Query::limit($value);

        return $this;
    }

    #[\Override]
    public function offset(int $value): static
    {
        $this->pendingQueries[] = Query::offset($value);

        return $this;
    }

    #[\Override]
    public function fetch(int $count, bool $withTies = false): static
    {
        $this->fetchCount = $count;
        $this->fetchWithTies = $withTies;

        return $this;
    }

    #[\Override]
    public function cursorAfter(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorAfter($value);

        return $this;
    }

    #[\Override]
    public function cursorBefore(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorBefore($value);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function queries(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    #[\Override]
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

    #[\Override]
    public function count(string $attribute = '*', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::count($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function sum(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::sum($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function avg(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::avg($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function min(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::min($attribute, $alias);

        return $this;
    }

    #[\Override]
    public function max(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::max($attribute, $alias);

        return $this;
    }

    /**
     * @param  array<string>  $columns
     */
    #[\Override]
    public function groupBy(array $columns): static
    {
        $this->pendingQueries[] = Query::groupBy($columns);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    #[\Override]
    public function having(array $queries): static
    {
        $this->pendingQueries[] = Query::having($queries);

        return $this;
    }

    #[\Override]
    public function distinct(): static
    {
        $this->pendingQueries[] = Query::distinct();

        return $this;
    }

    #[\Override]
    public function join(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::join($table, $left, $right, $operator, $alias);

        return $this;
    }

    #[\Override]
    public function leftJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::leftJoin($table, $left, $right, $operator, $alias);

        return $this;
    }

    #[\Override]
    public function rightJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::rightJoin($table, $left, $right, $operator, $alias);

        return $this;
    }

    #[\Override]
    public function crossJoin(string $table, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::crossJoin($table, $alias);

        return $this;
    }

    #[\Override]
    public function naturalJoin(string $table, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::naturalJoin($table, $alias);

        return $this;
    }

    #[\Override]
    public function union(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Union, $result->query, $result->bindings);

        return $this;
    }

    #[\Override]
    public function unionAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::UnionAll, $result->query, $result->bindings);

        return $this;
    }

    #[\Override]
    public function intersect(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Intersect, $result->query, $result->bindings);

        return $this;
    }

    #[\Override]
    public function intersectAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::IntersectAll, $result->query, $result->bindings);

        return $this;
    }

    #[\Override]
    public function except(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::Except, $result->query, $result->bindings);

        return $this;
    }

    #[\Override]
    public function exceptAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = new UnionClause(UnionType::ExceptAll, $result->query, $result->bindings);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function fromSelect(array $columns, self $source): static
    {
        $this->insertSelectColumns = $columns;
        $this->insertSelectSource = $source;

        return $this;
    }

    #[\Override]
    public function insertSelect(): Plan
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

        $this->addBindings($sourceResult->bindings);

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }

    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function with(string $name, self $query, array $columns = []): static
    {
        $result = $query->build();
        $this->ctes[] = new CteClause($name, $result->query, $result->bindings, false, $columns);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function withRecursive(string $name, self $query, array $columns = []): static
    {
        $result = $query->build();
        $this->ctes[] = new CteClause($name, $result->query, $result->bindings, true, $columns);

        return $this;
    }

    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function withRecursiveSeedStep(string $name, self $seed, self $step, array $columns = []): static
    {
        $seedResult = $seed->build();
        $stepResult = $step->build();
        $query = $seedResult->query . ' UNION ALL ' . $stepResult->query;
        $bindings = \array_merge($seedResult->bindings, $stepResult->bindings);
        $this->ctes[] = new CteClause($name, $query, $bindings, true, $columns);

        return $this;
    }

    #[\Override]
    public function selectCast(string $column, string $type, string $alias = ''): static
    {
        if (!\preg_match('/^[A-Za-z0-9_() ,]+$/', $type)) {
            throw new ValidationException('Invalid cast type: ' . $type);
        }

        $expr = 'CAST(' . $this->resolveAndWrap($column) . ' AS ' . $type . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }
        $this->rawSelects[] = new Condition($expr, []);

        return $this;
    }

    #[\Override]
    public function selectWindow(string $function, string $alias, ?array $partitionBy = null, ?array $orderBy = null, ?string $windowName = null, ?WindowFrame $frame = null): static
    {
        if (!\preg_match('/^[A-Za-z_][A-Za-z0-9_]*\s*\(.*\)$/', \trim($function))) {
            throw new ValidationException('Invalid window function: ' . $function);
        }

        $this->windowSelects[] = new WindowSelect($function, $alias, $partitionBy, $orderBy, $windowName, $frame);

        return $this;
    }

    #[\Override]
    public function window(string $name, ?array $partitionBy = null, ?array $orderBy = null, ?WindowFrame $frame = null): static
    {
        $this->windowDefinitions[] = new WindowDefinition($name, $partitionBy, $orderBy, $frame);

        return $this;
    }

    public function selectCase(CaseExpression $case): static
    {
        $this->cases[] = $case;

        return $this;
    }

    public function setCase(string $column, CaseExpression $case): static
    {
        $this->caseSets[$column] = $case;

        return $this;
    }

    /**
     * Compile a CASE expression to SQL, appending bindings to $this->bindings
     * in the order WHEN-value, THEN-value, ..., ELSE-value.
     */
    protected function compileCase(CaseExpression $case): string
    {
        $whens = $case->getWhens();

        if ($whens === []) {
            throw new ValidationException('CASE expression requires at least one WHEN clause.');
        }

        $sql = 'CASE';

        foreach ($whens as $when) {
            $sql .= ' WHEN ' . $this->compileWhenCondition($when) . ' THEN ?';
            $this->addBinding($when->then);
        }

        if ($case->hasElse()) {
            $sql .= ' ELSE ?';
            $this->addBinding($case->getElse());
        }

        $sql .= ' END';

        $alias = $case->getAlias();

        if ($alias !== '') {
            $sql .= ' AS ' . $this->quote($alias);
        }

        return $sql;
    }

    /**
     * Compile the predicate of a single WHEN clause, adding any operand
     * bindings to $this->bindings in left-to-right order.
     */
    private function compileWhenCondition(WhenClause $when): string
    {
        switch ($when->kind) {
            case CaseKind::Comparison:
                if ($when->column === null || $when->operator === null) {
                    throw new ValidationException('Comparison WHEN clause requires column and operator.');
                }

                if (! \in_array($when->operator, self::COMPARISON_OPERATORS, true)) {
                    throw new ValidationException(
                        'Unsupported CASE WHEN operator: ' . $when->operator
                    );
                }

                $this->addBinding($when->value);

                return $this->quote($when->column) . ' ' . $when->operator . ' ?';

            case CaseKind::Null:
                if ($when->column === null) {
                    throw new ValidationException('Null WHEN clause requires column.');
                }

                return $this->quote($when->column) . ' IS NULL';

            case CaseKind::NotNull:
                if ($when->column === null) {
                    throw new ValidationException('NotNull WHEN clause requires column.');
                }

                return $this->quote($when->column) . ' IS NOT NULL';

            case CaseKind::In:
                if ($when->column === null) {
                    throw new ValidationException('In WHEN clause requires column.');
                }

                if ($when->values === []) {
                    throw new ValidationException('In WHEN clause requires at least one value.');
                }

                $placeholders = \implode(', ', \array_fill(0, \count($when->values), '?'));

                foreach ($when->values as $value) {
                    $this->addBinding($value);
                }

                return $this->quote($when->column) . ' IN (' . $placeholders . ')';

            case CaseKind::Raw:
                if ($when->rawCondition === null) {
                    throw new ValidationException('Raw WHEN clause requires condition.');
                }

                foreach ($when->rawBindings as $binding) {
                    $this->addBinding($binding);
                }

                return $when->rawCondition;
        }
    }

    /**
     * Comparison operators accepted by CaseExpression::when().
     */
    public const array COMPARISON_OPERATORS = [
        '=',
        '!=',
        '<>',
        '<',
        '>',
        '<=',
        '>=',
        'LIKE',
        'NOT LIKE',
        'IS',
        'IS NOT',
    ];

    #[\Override]
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

    /**
     * @param  \Closure(Plan): (array<mixed>|int)  $executor
     */
    public function setExecutor(\Closure $executor): static
    {
        $this->executor = $executor;

        return $this;
    }

    #[\Override]
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

    #[\Override]
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

    public function forUpdate(): static
    {
        $this->lockMode = LockMode::ForUpdate;

        return $this;
    }

    /**
     * Build an INSERT ... ON CONFLICT/DUPLICATE KEY UPDATE statement.
     * Requires onConflict() to be called first to configure conflict keys and update columns.
     */
    public function upsert(): Plan
    {
        return $this->insert();
    }

    #[\Override]
    public function build(): Plan
    {
        $this->bindings = [];

        foreach ($this->beforeBuildCallbacks as $callback) {
            $callback($this);
        }

        $this->validateTable();

        $ctePrefix = $this->buildCtePrefix();

        $grouped = Query::groupByType($this->pendingQueries);

        $this->prepareAliasQualification($grouped);

        $parts = [];
        $parts[] = $this->buildSelectClause($grouped);

        $fromClause = $this->buildFromClause();
        if ($fromClause !== '') {
            $parts[] = $fromClause;
        }

        $joinFilterWhereClauses = [];
        $joinsClause = $this->buildJoinsClause($grouped, $joinFilterWhereClauses);
        if ($joinsClause !== '') {
            $parts[] = $joinsClause;
        }

        $afterJoins = $this->buildAfterJoinsClause($grouped);
        if ($afterJoins !== '') {
            $parts[] = $afterJoins;
        }

        $whereClause = $this->buildWhereClause($grouped, $joinFilterWhereClauses);
        if ($whereClause !== '') {
            $parts[] = $whereClause;
        }

        $groupByClause = $this->buildGroupByClause($grouped);
        if ($groupByClause !== '') {
            $parts[] = $groupByClause;
        }

        $afterGroupBy = $this->buildAfterGroupByClause();
        if ($afterGroupBy !== '') {
            $parts[] = $afterGroupBy;
        }

        $havingClause = $this->buildHavingClause($grouped);
        if ($havingClause !== '') {
            $parts[] = $havingClause;
        }

        $windowClause = $this->buildWindowClause();
        if ($windowClause !== '') {
            $parts[] = $windowClause;
        }

        $orderByClause = $this->buildOrderByClause();
        if ($orderByClause !== '') {
            $parts[] = $orderByClause;
        }

        $afterOrderBy = $this->buildAfterOrderByClause();
        if ($afterOrderBy !== '') {
            $parts[] = $afterOrderBy;
        }

        $limitClause = $this->buildLimitClause($grouped);
        if ($limitClause !== '') {
            $parts[] = $limitClause;
        }

        $lockingClause = $this->buildLockingClause();
        if ($lockingClause !== '') {
            $parts[] = $lockingClause;
        }

        $settings = $this->buildSettingsClause();
        if ($settings !== '') {
            $parts[] = $settings;
        }

        $sql = \implode(' ', $parts);

        $unionSuffix = $this->buildUnionSuffix();
        if ($unionSuffix !== '') {
            $sql = '(' . $sql . ')' . $unionSuffix;
        }

        $sql = $ctePrefix . $sql;

        $result = new Plan($sql, $this->bindings, readOnly: true, executor: $this->executor);

        foreach ($this->afterBuildCallbacks as $callback) {
            $result = $callback($result);
        }

        return $result;
    }

    /**
     * Build the optional WITH / WITH RECURSIVE prefix. Adds CTE bindings to
     * $this->bindings in document order. Returns an empty string when no
     * CTEs are registered.
     */
    private function buildCtePrefix(): string
    {
        if (empty($this->ctes)) {
            return '';
        }

        $hasRecursive = false;
        $cteParts = [];
        foreach ($this->ctes as $cte) {
            if ($cte->recursive) {
                $hasRecursive = true;
            }
            $this->addBindings($cte->bindings);
            $cteName = $this->quote($cte->name);
            if (! empty($cte->columns)) {
                $cteName .= '(' . \implode(', ', \array_map(fn (string $col): string => $this->quote($col), $cte->columns)) . ')';
            }
            $cteParts[] = $cteName . ' AS (' . $cte->query . ')';
        }

        $keyword = $hasRecursive ? 'WITH RECURSIVE' : 'WITH';

        return $keyword . ' ' . \implode(', ', $cteParts) . ' ';
    }

    /**
     * Configure alias-qualification state prior to emitting SELECT. When joins
     * are present and the base table has an alias, column references must be
     * fully qualified — except aggregation aliases, which are captured here
     * so they can be emitted bare.
     */
    private function prepareAliasQualification(GroupedQueries $grouped): void
    {
        $this->qualify = false;
        $this->aggregationAliases = [];

        if (empty($grouped->joins) || $this->alias === '') {
            return;
        }

        $this->qualify = true;
        foreach ($grouped->aggregations as $agg) {
            /** @var string $aggAlias */
            $aggAlias = $agg->getValue('');
            if ($aggAlias !== '') {
                $this->aggregationAliases[$aggAlias] = true;
            }
        }
    }

    /**
     * Compile the SELECT [DISTINCT] ... clause, including aggregations,
     * column selections, sub-selects, raw selects, window function selects,
     * and CASE selects. Always returns a non-empty fragment (falls back
     * to `SELECT *`).
     */
    private function buildSelectClause(GroupedQueries $grouped): string
    {
        $selectParts = [];

        foreach ($grouped->aggregations as $agg) {
            $selectParts[] = $this->compileAggregate($agg);
        }

        if (! empty($grouped->selections)) {
            $selectParts[] = $this->compileSelect($grouped->selections[0]);
        }

        foreach ($this->subSelects as $subSelect) {
            $subResult = $subSelect->subquery->build();
            $selectParts[] = '(' . $subResult->query . ') AS ' . $this->quote($subSelect->alias);
            $this->addBindings($subResult->bindings);
        }

        foreach ($this->rawSelects as $rawSelect) {
            $selectParts[] = $rawSelect->expression;
            $this->addBindings($rawSelect->bindings);
        }

        foreach ($this->windowSelects as $win) {
            $selectParts[] = $this->compileWindowSelect($win);
        }

        foreach ($this->cases as $caseSelect) {
            $selectParts[] = $this->compileCase($caseSelect);
        }

        $selectSQL = ! empty($selectParts) ? \implode(', ', $selectParts) : '*';
        $selectKeyword = $grouped->distinct ? 'SELECT DISTINCT' : 'SELECT';

        return $selectKeyword . ' ' . $selectSQL;
    }

    /**
     * Compile a single window-function SELECT item (inline or named window).
     */
    private function compileWindowSelect(WindowSelect $win): string
    {
        if ($win->windowName !== null) {
            return $win->function . ' OVER ' . $this->quote($win->windowName) . ' AS ' . $this->quote($win->alias);
        }

        $overParts = [];

        if ($win->partitionBy !== null && $win->partitionBy !== []) {
            $partCols = \array_map(
                fn (string $col): string => $this->resolveAndWrap($col),
                $win->partitionBy
            );
            $overParts[] = 'PARTITION BY ' . \implode(', ', $partCols);
        }

        if ($win->orderBy !== null && $win->orderBy !== []) {
            $overParts[] = 'ORDER BY ' . $this->compileOrderByList($win->orderBy);
        }

        if ($win->frame !== null) {
            $overParts[] = $win->frame->toSql();
        }

        return $win->function . ' OVER (' . \implode(' ', $overParts) . ') AS ' . $this->quote($win->alias);
    }

    /**
     * Compile a list of ORDER BY column tokens (prefixed with '-' for DESC)
     * into a comma-separated SQL fragment.
     *
     * @param  list<string>  $orderBy
     */
    private function compileOrderByList(array $orderBy): string
    {
        $orderCols = [];
        foreach ($orderBy as $col) {
            if (\str_starts_with($col, '-')) {
                $orderCols[] = $this->resolveAndWrap(\substr($col, 1)) . ' DESC';
            } else {
                $orderCols[] = $this->resolveAndWrap($col) . ' ASC';
            }
        }

        return \implode(', ', $orderCols);
    }

    /**
     * Compile the FROM clause. Delegates the table/subquery portion to
     * buildTableClause() so dialects can override it precisely.
     */
    private function buildFromClause(): string
    {
        return $this->buildTableClause();
    }

    /**
     * Compile the JOIN section, including any lateral joins. Deferred join
     * filter conditions that must land in WHERE are appended to the
     * $joinFilterWhereClauses out-parameter.
     *
     * @param  list<Condition>  $joinFilterWhereClauses
     */
    private function buildJoinsClause(GroupedQueries $grouped, array &$joinFilterWhereClauses): string
    {
        $joinParts = [];

        if (! empty($grouped->joins)) {
            $joinQueryIndices = [];
            foreach ($this->pendingQueries as $idx => $pq) {
                if ($pq->getMethod()->isJoin()) {
                    $joinQueryIndices[] = $idx;
                }
            }

            foreach ($grouped->joins as $joinIdx => $joinQuery) {
                $pendingIdx = $joinQueryIndices[$joinIdx] ?? -1;
                $joinBuilder = $this->joins[$pendingIdx] ?? null;

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
                        $this->addBindings($result->condition->bindings);
                    } else {
                        $joinFilterWhereClauses[] = $result->condition;
                    }
                }

                $joinParts[] = $joinSQL;
            }
        }

        foreach ($this->lateralJoins as $lateral) {
            $subResult = $lateral->subquery->build();
            $this->addBindings($subResult->bindings);
            $joinKeyword = match ($lateral->type) {
                JoinType::Left => 'LEFT JOIN',
                default => 'JOIN',
            };
            $joinParts[] = $joinKeyword . ' LATERAL (' . $subResult->query . ') AS ' . $this->quote($lateral->alias) . ' ON true';
        }

        return \implode(' ', $joinParts);
    }

    /**
     * Compile the WHERE clause from query filters, filter hooks, deferred
     * join-filter conditions, WHERE IN / NOT IN subqueries, EXISTS
     * subqueries, and cursor pagination.
     *
     * @param  list<Condition>  $joinFilterWhereClauses
     */
    private function buildWhereClause(GroupedQueries $grouped, array $joinFilterWhereClauses): string
    {
        $whereClauses = [];

        foreach ($grouped->filters as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->alias ?: $this->table);
            $whereClauses[] = $condition->expression;
            $this->addBindings($condition->bindings);
        }

        foreach ($joinFilterWhereClauses as $condition) {
            $whereClauses[] = $condition->expression;
            $this->addBindings($condition->bindings);
        }

        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub->column) . ' ' . $prefix . ' (' . $subResult->query . ')';
            $this->addBindings($subResult->bindings);
        }

        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT EXISTS' : 'EXISTS';
            $whereClauses[] = $prefix . ' (' . $subResult->query . ')';
            $this->addBindings($subResult->bindings);
        }

        if ($grouped->cursor !== null && $grouped->cursorDirection !== null) {
            $cursorQueries = Query::getCursorQueries($this->pendingQueries, false);
            if (! empty($cursorQueries)) {
                $cursorSQL = $this->compileCursor($cursorQueries[0]);
                if ($cursorSQL !== '') {
                    $whereClauses[] = $cursorSQL;
                }
            }
        }

        if (empty($whereClauses)) {
            return '';
        }

        return 'WHERE ' . \implode(' AND ', $whereClauses);
    }

    /**
     * Compile the GROUP BY clause, including any raw group expressions.
     */
    private function buildGroupByClause(GroupedQueries $grouped): string
    {
        $groupByParts = [];
        if (! empty($grouped->groupBy)) {
            foreach ($grouped->groupBy as $col) {
                $groupByParts[] = $this->resolveAndWrap($col);
            }
        }

        foreach ($this->rawGroups as $rawGroup) {
            $groupByParts[] = $rawGroup->expression;
            $this->addBindings($rawGroup->bindings);
        }

        if (empty($groupByParts)) {
            return '';
        }

        return 'GROUP BY ' . \implode(', ', $groupByParts);
    }

    /**
     * Compile the HAVING clause, resolving aggregation aliases to their
     * underlying expressions so filters against alias names work portably.
     */
    private function buildHavingClause(GroupedQueries $grouped): string
    {
        $aliasToExpr = $this->buildAggregationAliasMap($grouped);

        $havingClauses = [];
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
            $this->addBindings($rawHaving->bindings);
        }

        if (empty($havingClauses)) {
            return '';
        }

        return 'HAVING ' . \implode(' AND ', $havingClauses);
    }

    /**
     * Build a map of aggregation alias -> compiled aggregate expression so
     * HAVING can refer to aliases portably across dialects that don't allow
     * SELECT-list aliases in HAVING.
     *
     * @return array<string, string>
     */
    private function buildAggregationAliasMap(GroupedQueries $grouped): array
    {
        $aliasToExpr = [];
        foreach ($grouped->aggregations as $agg) {
            /** @var string $alias */
            $alias = $agg->getValue('');
            if ($alias === '') {
                continue;
            }

            $method = $agg->getMethod();
            $attr = $agg->getAttribute();
            $col = match (true) {
                $attr === '*', $attr === '' => '*',
                \is_numeric($attr) => $attr,
                default => $this->resolveAndWrap($attr),
            };

            if ($method === Method::CountDistinct) {
                $aliasToExpr[$alias] = 'COUNT(DISTINCT ' . $col . ')';

                continue;
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
                default => $method->value,
            };
            $aliasToExpr[$alias] = $func . '(' . $col . ')';
        }

        return $aliasToExpr;
    }

    /**
     * Compile the named-window (WINDOW w AS (...)) clause.
     */
    private function buildWindowClause(): string
    {
        if (empty($this->windowDefinitions)) {
            return '';
        }

        $windowParts = [];
        foreach ($this->windowDefinitions as $winDef) {
            $overParts = [];
            if ($winDef->partitionBy !== null && $winDef->partitionBy !== []) {
                $partCols = \array_map(fn (string $col): string => $this->resolveAndWrap($col), $winDef->partitionBy);
                $overParts[] = 'PARTITION BY ' . \implode(', ', $partCols);
            }
            if ($winDef->orderBy !== null && $winDef->orderBy !== []) {
                $overParts[] = 'ORDER BY ' . $this->compileOrderByList($winDef->orderBy);
            }
            if ($winDef->frame !== null) {
                $overParts[] = $winDef->frame->toSql();
            }
            $windowParts[] = $this->quote($winDef->name) . ' AS (' . \implode(' ', $overParts) . ')';
        }

        return 'WINDOW ' . \implode(', ', $windowParts);
    }

    /**
     * Compile the ORDER BY clause, including vector-distance ordering, raw
     * order expressions, and ordinary ORDER ASC/DESC/RANDOM queries.
     */
    private function buildOrderByClause(): string
    {
        $orderClauses = [];

        $vectorOrderExpr = $this->compileVectorOrderExpr();
        if ($vectorOrderExpr !== null) {
            $orderClauses[] = $vectorOrderExpr->expression;
            $this->addBindings($vectorOrderExpr->bindings);
        }

        foreach ($this->rawOrders as $rawOrder) {
            $orderClauses[] = $rawOrder->expression;
            $this->addBindings($rawOrder->bindings);
        }

        $orderQueries = Query::getByType($this->pendingQueries, [
            Method::OrderAsc,
            Method::OrderDesc,
            Method::OrderRandom,
        ], false);
        foreach ($orderQueries as $orderQuery) {
            $orderClauses[] = $this->compileOrder($orderQuery);
        }

        if (empty($orderClauses)) {
            return '';
        }

        return 'ORDER BY ' . \implode(', ', $orderClauses);
    }

    /**
     * Compile the LIMIT / OFFSET / FETCH FIRST pagination tail. Emitted as
     * a single space-joined fragment so bindings are added in document order.
     */
    private function buildLimitClause(GroupedQueries $grouped): string
    {
        $limitParts = [];

        if ($grouped->limit !== null) {
            $limitParts[] = 'LIMIT ?';
            $this->addBinding($grouped->limit);
        }

        if ($this->shouldEmitOffset($grouped->offset, $grouped->limit)) {
            $limitParts[] = 'OFFSET ?';
            $this->addBinding($grouped->offset);
        }

        if ($this->fetchCount !== null) {
            $this->addBinding($this->fetchCount);
            $limitParts[] = $this->fetchWithTies
                ? 'FETCH FIRST ? ROWS WITH TIES'
                : 'FETCH FIRST ? ROWS ONLY';
        }

        return \implode(' ', $limitParts);
    }

    /**
     * Compile the locking clause (FOR UPDATE / FOR SHARE / ...), optionally
     * scoped with OF <table>.
     */
    private function buildLockingClause(): string
    {
        if ($this->lockMode === null) {
            return '';
        }

        $lockSql = $this->lockMode->toSql();
        if ($this->lockOfTable !== null) {
            $lockSql .= ' OF ' . $this->quote($this->lockOfTable);
        }

        return $lockSql;
    }

    /**
     * Compile the trailing UNION chain. Returns the suffix to concatenate
     * after the parenthesized primary query (including the leading space),
     * or an empty string when no unions are registered.
     */
    private function buildUnionSuffix(): string
    {
        if (empty($this->unions)) {
            return '';
        }

        $suffix = '';
        foreach ($this->unions as $union) {
            $suffix .= ' ' . $union->type->value . ' (' . $union->query . ')';
            $this->addBindings($union->bindings);
        }

        return $suffix;
    }

    /**
     * @return array<mixed>|int
     */
    public function execute(): array|int
    {
        return $this->build()->execute();
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
        foreach ($this->rows as $row) {
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

    #[\Override]
    public function insert(): Plan
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        $this->addBindings($bindings);

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }

    #[\Override]
    public function insertDefaultValues(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $sql = 'INSERT INTO ' . $this->quote($this->table) . ' DEFAULT VALUES';

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }

    /**
     * @return list<string>
     */
    protected function compileAssignments(): array
    {
        $assignments = [];

        if (! empty($this->rows)) {
            foreach ($this->rows[0] as $col => $value) {
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
            $assignments[] = $this->resolveAndWrap($col) . ' = ' . $this->compileCase($caseData);
        }

        return $assignments;
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

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = ['UPDATE ' . $this->quote($this->table) . ' SET ' . \implode(', ', $assignments)];

        $this->compileWhereClauses($parts, $grouped);

        $this->compileOrderAndLimit($parts, $grouped);

        return new Plan(\implode(' ', $parts), $this->bindings, executor: $this->executor);
    }

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
            $condition = $hook->filter($this->alias ?: $this->table);
            $whereClauses[] = $condition->expression;
            $this->addBindings($condition->bindings);
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT IN' : 'IN';
            $whereClauses[] = $this->resolveAndWrap($sub->column) . ' ' . $prefix . ' (' . $subResult->query . ')';
            $this->addBindings($subResult->bindings);
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $sub) {
            $subResult = $sub->subquery->build();
            $prefix = $sub->not ? 'NOT EXISTS' : 'EXISTS';
            $whereClauses[] = $prefix . ' (' . $subResult->query . ')';
            $this->addBindings($subResult->bindings);
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
            $this->addBindings($rawOrder->bindings);
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
        if ($this->tableless) {
            return;
        }
        if ($this->table === '' && $this->fromSubquery === null) {
            throw new ValidationException('No table specified. Call from() or into() before building a query.');
        }
    }

    protected function validateRows(string $operation): void
    {
        if (empty($this->rows)) {
            throw new ValidationException("No rows to {$operation}. Call set() before {$operation}().");
        }

        foreach ($this->rows as $row) {
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
        $columns = \array_keys($this->rows[0]);

        foreach ($columns as $col) {
            if ($col === '') {
                throw new ValidationException('Column names must be non-empty strings.');
            }
        }

        if (\count($this->rows) > 1) {
            $expectedKeys = $columns;
            \sort($expectedKeys);

            foreach ($this->rows as $i => $row) {
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
    #[\Override]
    public function getBindings(): array
    {
        return $this->bindings;
    }

    #[\Override]
    public function reset(): static
    {
        $this->pendingQueries = [];
        $this->bindings = [];
        $this->table = '';
        $this->alias = '';
        $this->unions = [];
        $this->rows = [];
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
        $this->sample = null;
        $this->cases = [];
        $this->caseSets = [];
        $this->whereInSubqueries = [];
        $this->subSelects = [];
        $this->fromSubquery = null;
        $this->tableless = false;
        $this->rawOrders = [];
        $this->rawGroups = [];
        $this->rawHavings = [];
        $this->joins = [];
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
        $this->joins = \array_map(fn (JoinBuilder $j) => clone $j, $this->joins);
        $this->pendingQueries = \array_map(fn (Query $q) => clone $q, $this->pendingQueries);
        $this->lateralJoins = \array_map(fn (LateralJoin $l) => new LateralJoin(clone $l->subquery, $l->alias, $l->type), $this->lateralJoins);
    }

    #[\Override]
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

    #[\Override]
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

    #[\Override]
    public function compileLimit(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'LIMIT ?';
    }

    #[\Override]
    public function compileOffset(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'OFFSET ?';
    }

    #[\Override]
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

    #[\Override]
    public function compileCursor(Query $query): string
    {
        $value = $query->getValue();
        $this->addBinding($value);

        $operator = $query->getMethod() === Method::CursorAfter ? '>' : '<';

        return $this->quote('_cursor') . ' ' . $operator . ' ?';
    }

    #[\Override]
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

    #[\Override]
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

    #[\Override]
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
            $this->addBindings($where->bindings);
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

        if ($this->qualify
            && $resolved !== '*'
            && ! \str_contains($resolved, '.')
            && ! isset($this->aggregationAliases[$resolved])
        ) {
            $resolved = $this->alias . '.' . $resolved;
        }

        return $this->quote($resolved);
    }

    protected function addBinding(mixed $value): void
    {
        $this->bindings[] = $value;
    }

    /**
     * @param  array<mixed>  $bindings
     */
    protected function addBindings(array $bindings): void
    {
        \array_push($this->bindings, ...$bindings);
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

    public function toAst(): Select
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

        return new Select(
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
     * @return Expression[]
     */
    private function buildAstColumns(GroupedQueries $grouped): array
    {
        $columns = [];

        foreach ($grouped->aggregations as $agg) {
            $columns[] = $this->aggregateQueryToAstExpression($agg);
        }

        if (!empty($grouped->selections)) {
            /** @var array<string> $selectedCols */
            $selectedCols = $grouped->selections[0]->getValues();
            foreach ($selectedCols as $col) {
                $columns[] = $this->columnNameToAstExpression($col);
            }
        }

        if (empty($columns)) {
            $columns[] = new Star();
        }

        return $columns;
    }

    private function columnNameToAstExpression(string $col): Expression
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
                return new Column($parts[2], $parts[1], $parts[0]);
            }
            if ($parts[1] === '*') {
                return new Star($parts[0]);
            }
            return new Column($parts[1], $parts[0]);
        }

        return new Column($col);
    }

    private function aggregateQueryToAstExpression(Query $query): Expression
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

        $arg = ($attr === '*' || $attr === '') ? new Star() : new Column($attr);
        $distinct = $method === Method::CountDistinct;

        $funcCall = new Func($funcName, [$arg], $distinct);

        if ($alias !== '') {
            return new Aliased($funcCall, $alias);
        }

        return $funcCall;
    }

    private function buildAstFrom(): ?Table
    {
        if ($this->tableless) {
            return null;
        }

        if ($this->table === '') {
            return null;
        }

        $alias = $this->alias !== '' ? $this->alias : null;
        return new Table($this->table, $alias);
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
                $tableRef = new Table($table, $joinAlias !== '' ? $joinAlias : null);
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

                $tableRef = new Table($table, $joinAlias !== '' ? $joinAlias : null);

                $condition = null;
                if ($leftCol !== '' && $rightCol !== '') {
                    $condition = new Binary(
                        $this->columnNameToAstExpression($leftCol),
                        $operator,
                        $this->columnNameToAstExpression($rightCol),
                    );
                }

                $joins[] = new AstJoinClause($type, $tableRef, $condition);
            }
        }

        return $joins;
    }

    private function buildAstWhere(GroupedQueries $grouped): ?Expression
    {
        if (empty($grouped->filters)) {
            return null;
        }

        $exprs = [];
        foreach ($grouped->filters as $filter) {
            $exprs[] = $this->queryToAstExpression($filter);
        }

        return $this->combineAstExpressions($exprs, 'AND');
    }

    private function queryToAstExpression(Query $query): Expression
    {
        $method = $query->getMethod();
        $attr = $query->getAttribute();
        $values = $query->getValues();

        return match ($method) {
            Method::Equal => $this->buildEqualAstExpression($attr, $values),
            Method::NotEqual => $this->buildNotEqualAstExpression($attr, $values),
            Method::GreaterThan => new Binary(new Column($attr), '>', $this->toLiteral($values[0] ?? null)),
            Method::GreaterThanEqual => new Binary(new Column($attr), '>=', $this->toLiteral($values[0] ?? null)),
            Method::LessThan => new Binary(new Column($attr), '<', $this->toLiteral($values[0] ?? null)),
            Method::LessThanEqual => new Binary(new Column($attr), '<=', $this->toLiteral($values[0] ?? null)),
            Method::Between => new Between(new Column($attr), $this->toLiteral($values[0] ?? null), $this->toLiteral($values[1] ?? null)),
            Method::NotBetween => new Between(new Column($attr), $this->toLiteral($values[0] ?? null), $this->toLiteral($values[1] ?? null), true),
            Method::IsNull => new Unary('IS NULL', new Column($attr), false),
            Method::IsNotNull => new Unary('IS NOT NULL', new Column($attr), false),
            Method::Contains => $this->buildContainsAstExpression($attr, $values, false),
            Method::ContainsAny => $this->buildContainsAstExpression($attr, $values, false),
            Method::NotContains => $this->buildContainsAstExpression($attr, $values, true),
            Method::StartsWith => new Binary(new Column($attr), 'LIKE', new Literal($this->toScalar($values[0] ?? '') . '%')),
            Method::NotStartsWith => new Binary(new Column($attr), 'NOT LIKE', new Literal($this->toScalar($values[0] ?? '') . '%')),
            Method::EndsWith => new Binary(new Column($attr), 'LIKE', new Literal('%' . $this->toScalar($values[0] ?? ''))),
            Method::NotEndsWith => new Binary(new Column($attr), 'NOT LIKE', new Literal('%' . $this->toScalar($values[0] ?? ''))),
            Method::And => $this->buildLogicalAstExpression($query, 'AND'),
            Method::Or => $this->buildLogicalAstExpression($query, 'OR'),
            Method::Raw => new Raw($attr),
            default => new Raw($attr !== '' ? $attr : '1 = 1'),
        };
    }

    private function toLiteral(mixed $value): Literal
    {
        if ($value === null || \is_string($value) || \is_int($value) || \is_float($value) || \is_bool($value)) {
            return new Literal($value);
        }

        /** @var scalar $value */
        return new Literal((string) $value);
    }

    private function toScalar(mixed $value): string
    {
        if (\is_string($value)) {
            return $value;
        }

        if (\is_int($value) || \is_float($value) || \is_bool($value)) {
            return (string) $value;
        }

        return '';
    }

    /**
     * @param array<mixed> $values
     */
    private function buildEqualAstExpression(string $attr, array $values): Expression
    {
        if (\count($values) === 1) {
            if ($values[0] === null) {
                return new Unary('IS NULL', new Column($attr), false);
            }
            return new Binary(new Column($attr), '=', $this->toLiteral($values[0]));
        }

        $literals = \array_map(fn ($v) => $this->toLiteral($v), $values);
        return new In(new Column($attr), $literals);
    }

    /**
     * @param array<mixed> $values
     */
    private function buildNotEqualAstExpression(string $attr, array $values): Expression
    {
        if (\count($values) === 1) {
            if ($values[0] === null) {
                return new Unary('IS NOT NULL', new Column($attr), false);
            }
            return new Binary(new Column($attr), '!=', $this->toLiteral($values[0]));
        }

        $literals = \array_map(fn ($v) => $this->toLiteral($v), $values);
        return new In(new Column($attr), $literals, true);
    }

    /**
     * @param array<mixed> $values
     */
    private function buildContainsAstExpression(string $attr, array $values, bool $negated): Expression
    {
        if (\count($values) === 1) {
            $op = $negated ? 'NOT LIKE' : 'LIKE';
            return new Binary(new Column($attr), $op, new Literal('%' . $this->toScalar($values[0]) . '%'));
        }

        $parts = [];
        $op = $negated ? 'NOT LIKE' : 'LIKE';
        foreach ($values as $value) {
            $parts[] = new Binary(new Column($attr), $op, new Literal('%' . $this->toScalar($value) . '%'));
        }

        $combinator = $negated ? 'AND' : 'OR';
        return $this->combineAstExpressions($parts, $combinator);
    }

    private function buildLogicalAstExpression(Query $query, string $operator): Expression
    {
        $parts = [];
        foreach ($query->getValues() as $subQuery) {
            if ($subQuery instanceof Query) {
                $parts[] = $this->queryToAstExpression($subQuery);
            }
        }

        if (empty($parts)) {
            return new Literal($operator === 'OR' ? false : true);
        }

        return $this->combineAstExpressions($parts, $operator);
    }

    /**
     * @param Expression[] $expressions
     */
    private function combineAstExpressions(array $expressions, string $operator): Expression
    {
        if (\count($expressions) === 1) {
            return $expressions[0];
        }

        $result = $expressions[0];
        for ($i = 1; $i < \count($expressions); $i++) {
            $result = new Binary($result, $operator, $expressions[$i]);
        }

        return $result;
    }

    /**
     * @return Expression[]
     */
    private function buildAstGroupBy(GroupedQueries $grouped): array
    {
        $exprs = [];
        foreach ($grouped->groupBy as $col) {
            $exprs[] = $this->columnNameToAstExpression($col);
        }
        return $exprs;
    }

    private function buildAstHaving(GroupedQueries $grouped): ?Expression
    {
        if (empty($grouped->having)) {
            return null;
        }

        $parts = [];
        foreach ($grouped->having as $havingQuery) {
            foreach ($havingQuery->getValues() as $subQuery) {
                if ($subQuery instanceof Query) {
                    $parts[] = $this->queryToAstExpression($subQuery);
                }
            }
        }

        if (empty($parts)) {
            return null;
        }

        return $this->combineAstExpressions($parts, 'AND');
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
                $items[] = new OrderByItem(new Raw('RAND()'), OrderDirection::Asc);
                continue;
            }

            $direction = $method === Method::OrderAsc ? OrderDirection::Asc : OrderDirection::Desc;
            $attr = $orderQuery->getAttribute();
            $expr = $this->columnNameToAstExpression($attr);

            $nulls = null;
            $nullsVal = $orderQuery->getValue(null);
            if ($nullsVal instanceof NullsPosition) {
                $nulls = $nullsVal;
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

    private function parseSqlToAst(string $sql): Select
    {
        $tokenizer = new Tokenizer();
        $tokens = Tokenizer::filter($tokenizer->tokenize($sql));
        $parser = new Parser();
        return $parser->parse($tokens);
    }

    protected function createAstSerializer(): Serializer
    {
        return new Serializer();
    }

    public static function fromAst(Select $ast): static
    {
        $builder = new static(); // @phpstan-ignore new.static

        if ($ast->from instanceof Table) {
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

    private function applyAstColumns(Select $ast): void
    {
        $selectCols = [];
        $hasNonStar = false;

        foreach ($ast->columns as $col) {
            if ($col instanceof Star && $col->table === null) {
                continue;
            }

            if ($col instanceof Aliased && $col->expression instanceof Func) {
                $this->applyAstAggregateColumn($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof Func) {
                $this->applyAstUnaliasedFunctionColumn($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof Column) {
                $selectCols[] = $this->astColumnReferenceToString($col);
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof Star) {
                $selectCols[] = $col->table !== null ? $col->table . '.*' : '*';
                $hasNonStar = true;
                continue;
            }

            if ($col instanceof Aliased && $col->expression instanceof Column) {
                $colStr = $this->astColumnReferenceToString($col->expression);
                if ($col->alias !== '') {
                    $colStr .= ' AS ' . $col->alias;
                }
                $selectCols[] = $colStr;
                $hasNonStar = true;
                continue;
            }

            $serializer = $this->createAstSerializer();
            $this->select($serializer->serializeExpression($col));
            $hasNonStar = true;
        }

        if (!empty($selectCols)) {
            $this->select($selectCols);
        }
    }

    private function applyAstAggregateColumn(Aliased $aliased): void
    {
        $fn = $aliased->expression;
        if (!$fn instanceof Func) {
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

        $serializer = $this->createAstSerializer();
        $this->select($serializer->serializeExpression($aliased));
    }

    private function applyAstUnaliasedFunctionColumn(Func $fn): void
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

        $serializer = $this->createAstSerializer();
        $this->select($serializer->serializeExpression($fn));
    }

    private function astFuncArgToAttribute(Func $fn): string
    {
        if (empty($fn->arguments)) {
            return '*';
        }

        $firstArg = $fn->arguments[0];
        if ($firstArg instanceof Star) {
            return '*';
        }
        if ($firstArg instanceof Column) {
            return $this->astColumnReferenceToString($firstArg);
        }

        return '*';
    }

    private function astColumnReferenceToString(Column $reference): string
    {
        $parts = [];
        if ($reference->schema !== null) {
            $parts[] = $reference->schema;
        }
        if ($reference->table !== null) {
            $parts[] = $reference->table;
        }
        $parts[] = $reference->name;
        return \implode('.', $parts);
    }

    private function applyAstJoins(Select $ast): void
    {
        foreach ($ast->joins as $join) {
            if (!$join->table instanceof Table) {
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

            if ($join->condition instanceof Binary) {
                $leftCol = $this->astExpressionToColumnString($join->condition->left);
                $operator = $join->condition->operator;
                $rightCol = $this->astExpressionToColumnString($join->condition->right);
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

    private function astExpressionToColumnString(Expression $expression): string
    {
        if ($expression instanceof Column) {
            return $this->astColumnReferenceToString($expression);
        }

        $serializer = $this->createAstSerializer();
        return $serializer->serializeExpression($expression);
    }

    private function applyAstWhere(Select $ast): void
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
    private function astWhereToQueries(Expression $expression): array
    {
        if ($expression instanceof Binary && \strtoupper($expression->operator) === 'AND') {
            $left = $this->astWhereToQueries($expression->left);
            $right = $this->astWhereToQueries($expression->right);
            \array_push($left, ...$right);
            return $left;
        }

        $query = $this->astExpressionToSingleQuery($expression);
        if ($query !== null) {
            return [$query];
        }

        $serializer = $this->createAstSerializer();
        return [Query::raw($serializer->serializeExpression($expression))];
    }

    private function astExpressionToSingleQuery(Expression $expression): ?Query
    {
        if ($expression instanceof Binary) {
            $op = \strtoupper($expression->operator);

            if ($op === 'AND') {
                $leftQueries = $this->astWhereToQueries($expression->left);
                $rightQueries = $this->astWhereToQueries($expression->right);
                \array_push($leftQueries, ...$rightQueries);
                return Query::and($leftQueries);
            }

            if ($op === 'OR') {
                $leftQ = $this->astExpressionToSingleQuery($expression->left);
                $rightQ = $this->astExpressionToSingleQuery($expression->right);
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

            if ($expression->left instanceof Column && $expression->right instanceof Literal) {
                $attr = $this->astColumnReferenceToString($expression->left);
                /** @var string|int|float|bool|null $val */
                $val = $expression->right->value;

                return match ($op) {
                    '=' => Query::equal($attr, [$val]),
                    '!=' , '<>' => Query::notEqual($attr, \is_bool($val) ? (int) $val : $val),
                    '>' => Query::greaterThan($attr, \is_string($val) || \is_int($val) || \is_float($val) ? $val : (string) $val),
                    '>=' => Query::greaterThanEqual($attr, \is_string($val) || \is_int($val) || \is_float($val) ? $val : (string) $val),
                    '<' => Query::lessThan($attr, \is_string($val) || \is_int($val) || \is_float($val) ? $val : (string) $val),
                    '<=' => Query::lessThanEqual($attr, \is_string($val) || \is_int($val) || \is_float($val) ? $val : (string) $val),
                    'LIKE' => $this->likeToQuery($attr, (string) $val),
                    'NOT LIKE' => $this->notLikeToQuery($attr, (string) $val),
                    default => null,
                };
            }
        }

        if ($expression instanceof In && $expression->expression instanceof Column && \is_array($expression->list)) {
            $attr = $this->astColumnReferenceToString($expression->expression);
            $values = \array_map(fn (Expression $item) => $item instanceof Literal ? $item->value : null, $expression->list);
            if ($expression->negated) {
                return Query::notEqual($attr, $values);
            }
            return Query::equal($attr, $values);
        }

        if ($expression instanceof Between && $expression->expression instanceof Column) {
            $attr = $this->astColumnReferenceToString($expression->expression);
            $lowRaw = $expression->low instanceof Literal ? $expression->low->value : 0;
            $highRaw = $expression->high instanceof Literal ? $expression->high->value : 0;
            $low = \is_string($lowRaw) || \is_int($lowRaw) || \is_float($lowRaw) ? $lowRaw : (string) $lowRaw;
            $high = \is_string($highRaw) || \is_int($highRaw) || \is_float($highRaw) ? $highRaw : (string) $highRaw;
            if ($expression->negated) {
                return Query::notBetween($attr, $low, $high);
            }
            return Query::between($attr, $low, $high);
        }

        if ($expression instanceof Unary) {
            $op = \strtoupper($expression->operator);
            if ($expression->operand instanceof Column) {
                $attr = $this->astColumnReferenceToString($expression->operand);
                return match ($op) {
                    'IS NULL' => Query::isNull($attr),
                    'IS NOT NULL' => Query::isNotNull($attr),
                    default => null,
                };
            }
        }

        return null;
    }

    private function likeToQuery(string $attr, string $val): Query
    {
        $str = $val;
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

    private function notLikeToQuery(string $attr, string $val): Query
    {
        $str = $val;
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

    private function applyAstGroupBy(Select $ast): void
    {
        if (empty($ast->groupBy)) {
            return;
        }

        $cols = [];
        foreach ($ast->groupBy as $expression) {
            if ($expression instanceof Column) {
                $cols[] = $this->astColumnReferenceToString($expression);
            }
        }

        if (!empty($cols)) {
            $this->groupBy($cols);
        }
    }

    private function applyAstHaving(Select $ast): void
    {
        if ($ast->having === null) {
            return;
        }

        $queries = $this->astWhereToQueries($ast->having);
        if (!empty($queries)) {
            $this->having($queries);
        }
    }

    private function applyAstOrderBy(Select $ast): void
    {
        foreach ($ast->orderBy as $item) {
            if ($item->expression instanceof Column) {
                $attr = $this->astColumnReferenceToString($item->expression);

                if ($item->direction === OrderDirection::Desc) {
                    $this->sortDesc($attr, $item->nulls);
                } else {
                    $this->sortAsc($attr, $item->nulls);
                }
            } else {
                $serializer = $this->createAstSerializer();
                $rawExpr = $serializer->serializeExpression($item->expression);
                $dir = $item->direction === OrderDirection::Desc ? ' DESC' : ' ASC';
                $this->orderByRaw($rawExpr . $dir);
            }
        }
    }

    private function applyAstLimitOffset(Select $ast): void
    {
        if ($ast->limit instanceof Literal && ($ast->limit->value !== null)) {
            $this->limit((int) $ast->limit->value);
        }

        if ($ast->offset instanceof Literal && ($ast->offset->value !== null)) {
            $this->offset((int) $ast->offset->value);
        }
    }

    private function applyAstCtes(Select $ast): void
    {
        foreach ($ast->ctes as $cte) {
            $serializer = $this->createAstSerializer();
            $cteSql = $serializer->serialize($cte->query);

            $this->ctes[] = new CteClause(
                $cte->name,
                $cteSql,
                [],
                $cte->recursive,
                array_values($cte->columns),
            );
        }
    }
}
