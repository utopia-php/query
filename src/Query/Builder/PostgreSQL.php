<?php

namespace Utopia\Query\Builder;

use Utopia\Query\AST\Serializer;
use Utopia\Query\AST\Serializer\PostgreSQL as PostgreSQLSerializer;
use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Builder\Feature\GroupByModifiers;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\LateralJoins;
use Utopia\Query\Builder\Feature\PostgreSQL\AggregateFilter;
use Utopia\Query\Builder\Feature\PostgreSQL\DistinctOn;
use Utopia\Query\Builder\Feature\PostgreSQL\LockingOf;
use Utopia\Query\Builder\Feature\PostgreSQL\Merge;
use Utopia\Query\Builder\Feature\PostgreSQL\OrderedSetAggregates;
use Utopia\Query\Builder\Feature\PostgreSQL\Returning;
use Utopia\Query\Builder\Feature\PostgreSQL\VectorSearch;
use Utopia\Query\Builder\Feature\StringAggregates;
use Utopia\Query\Builder\Feature\TableSampling;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;

class PostgreSQL extends SQL implements VectorSearch, Json, Returning, LockingOf, ConditionalAggregates, Merge, LateralJoins, TableSampling, FullOuterJoins, StringAggregates, OrderedSetAggregates, DistinctOn, AggregateFilter, GroupByModifiers
{
    use Trait\FullOuterJoins;
    use Trait\LateralJoins;

    protected string $wrapChar = '"';

    #[\Override]
    protected function createAstSerializer(): Serializer
    {
        return new PostgreSQLSerializer();
    }

    /** @var list<string> */
    protected array $returningColumns = [];

    /** @var ?array{attribute: string, vector: array<float>, metric: VectorMetric} */
    protected ?array $vectorOrder = null;

    protected string $updateFromTable = '';

    protected string $updateFromAlias = '';

    protected string $updateFromCondition = '';

    /** @var list<mixed> */
    protected array $updateFromBindings = [];

    protected string $deleteUsingTable = '';

    protected string $deleteUsingCondition = '';

    /** @var list<mixed> */
    protected array $deleteUsingBindings = [];

    protected string $mergeTarget = '';

    protected ?BaseBuilder $mergeSource = null;

    protected string $mergeSourceAlias = '';

    protected string $mergeCondition = '';

    /** @var list<mixed> */
    protected array $mergeConditionBindings = [];

    /** @var list<MergeClause> */
    protected array $mergeClauses = [];

    /** @var list<string> */
    protected array $distinctOnColumns = [];

    protected ?string $groupByModifier = null;

    #[\Override]
    protected function compileRandom(): string
    {
        return 'RANDOM()';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' ~ ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSearchExpr(string $attribute, array $values, bool $not): string
    {
        /** @var string $term */
        $term = $values[0] ?? '';
        $exact = \str_ends_with($term, '"') && \str_starts_with($term, '"');

        $specialChars = ['@', '+', '-', '*', '.', "'", '"', ')', '(', '<', '>', '~'];
        $sanitized = \str_replace($specialChars, ' ', $term);
        $sanitized = \preg_replace('/\s+/', ' ', $sanitized) ?? '';
        $sanitized = \trim($sanitized);

        if ($sanitized === '') {
            return $not ? '1 = 1' : '1 = 0';
        }

        if ($exact) {
            $sanitized = '"' . $sanitized . '"';
        } else {
            $sanitized = \str_replace(' ', ' or ', $sanitized);
        }

        $this->addBinding($sanitized);
        $tsvector = "to_tsvector(regexp_replace(" . $attribute . ", '[^\\w]+', ' ', 'g'))";

        if ($not) {
            return 'NOT (' . $tsvector . ' @@ websearch_to_tsquery(?))';
        }

        return $tsvector . ' @@ websearch_to_tsquery(?)';
    }

    #[\Override]
    protected function compileConflictClause(): string
    {
        $wrappedKeys = \array_map(
            fn (string $key): string => $this->resolveAndWrap($key),
            $this->conflictKeys
        );

        $updates = [];
        foreach ($this->conflictUpdateColumns as $col) {
            $wrapped = $this->resolveAndWrap($col);
            if (isset($this->conflictRawSets[$col])) {
                $updates[] = $wrapped . ' = ' . $this->conflictRawSets[$col];
                foreach ($this->conflictRawSetBindings[$col] ?? [] as $binding) {
                    $this->addBinding($binding);
                }
            } else {
                $updates[] = $wrapped . ' = EXCLUDED.' . $wrapped;
            }
        }

        return 'ON CONFLICT (' . \implode(', ', $wrappedKeys) . ') DO UPDATE SET ' . \implode(', ', $updates);
    }

    #[\Override]
    protected function shouldEmitOffset(?int $offset, ?int $limit): bool
    {
        return $offset !== null;
    }

    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function returning(array $columns = ['*']): static
    {
        $this->returningColumns = $columns;

        return $this;
    }

    #[\Override]
    public function forUpdateOf(string $table): static
    {
        $this->lockMode = LockMode::ForUpdate;
        $this->lockOfTable = $table;

        return $this;
    }

    #[\Override]
    public function forShareOf(string $table): static
    {
        $this->lockMode = LockMode::ForShare;
        $this->lockOfTable = $table;

        return $this;
    }

    #[\Override]
    public function tablesample(float $percent, string $method = 'BERNOULLI'): static
    {
        $normalized = \strtoupper($method);
        if (! \in_array($normalized, ['BERNOULLI', 'SYSTEM'], true)) {
            throw new ValidationException('Invalid TABLESAMPLE method: ' . $method);
        }
        $this->sample = ['percent' => $percent, 'method' => $normalized];

        return $this;
    }

    #[\Override]
    public function insertOrIgnore(): Plan
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        $this->addBindings($bindings);

        $sql .= ' ON CONFLICT DO NOTHING';

        return $this->appendReturning(new Plan($sql, $this->bindings, executor: $this->executor));
    }

    #[\Override]
    public function insert(): Plan
    {
        $result = parent::insert();

        return $this->appendReturning($result);
    }

    public function updateFrom(string $table, string $alias = ''): static
    {
        $this->updateFromTable = $table;
        $this->updateFromAlias = $alias;

        return $this;
    }

    public function updateFromWhere(string $condition, mixed ...$bindings): static
    {
        $this->updateFromCondition = $condition;
        $this->updateFromBindings = \array_values($bindings);

        return $this;
    }

    #[\Override]
    public function update(): Plan
    {
        foreach ($this->jsonSets as $col => $condition) {
            $this->setRaw($col, $condition->expression, $condition->bindings);
        }

        if ($this->updateFromTable !== '') {
            $result = $this->buildUpdateFrom();
            $this->jsonSets = [];

            return $this->appendReturning($result);
        }

        $result = parent::update();
        $this->jsonSets = [];

        return $this->appendReturning($result);
    }

    private function buildUpdateFrom(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $assignments = $this->compileAssignments();

        if (empty($assignments)) {
            throw new ValidationException('No assignments for UPDATE. Call set() or setRaw() before update().');
        }

        $fromClause = $this->quote($this->updateFromTable);
        if ($this->updateFromAlias !== '') {
            $fromClause .= ' AS ' . $this->quote($this->updateFromAlias);
        }

        $sql = 'UPDATE ' . $this->quote($this->table)
            . ' SET ' . \implode(', ', $assignments)
            . ' FROM ' . $fromClause;

        $parts = [$sql];

        $updateFromWhereClauses = [];
        if ($this->updateFromCondition !== '') {
            $updateFromWhereClauses[] = $this->updateFromCondition;
            foreach ($this->updateFromBindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $this->compileWhereClauses($parts);

        if (! empty($updateFromWhereClauses)) {
            $lastPart = end($parts);
            if (\is_string($lastPart) && \str_starts_with($lastPart, 'WHERE ')) {
                $parts[\count($parts) - 1] = $lastPart . ' AND ' . \implode(' AND ', $updateFromWhereClauses);
            } else {
                $parts[] = 'WHERE ' . \implode(' AND ', $updateFromWhereClauses);
            }
        }

        return new Plan(\implode(' ', $parts), $this->bindings, executor: $this->executor);
    }

    public function deleteUsing(string $table, string $condition, mixed ...$bindings): static
    {
        $this->deleteUsingTable = $table;
        $this->deleteUsingCondition = $condition;
        $this->deleteUsingBindings = \array_values($bindings);

        return $this;
    }

    #[\Override]
    public function delete(): Plan
    {
        if ($this->deleteUsingTable !== '') {
            $result = $this->buildDeleteUsing();

            return $this->appendReturning($result);
        }

        $result = parent::delete();

        return $this->appendReturning($result);
    }

    private function buildDeleteUsing(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $sql = 'DELETE FROM ' . $this->quote($this->table)
            . ' USING ' . $this->quote($this->deleteUsingTable);

        $parts = [$sql];

        $deleteUsingWhereClauses = [];
        if ($this->deleteUsingCondition !== '') {
            $deleteUsingWhereClauses[] = $this->deleteUsingCondition;
            foreach ($this->deleteUsingBindings as $binding) {
                $this->addBinding($binding);
            }
        }

        $this->compileWhereClauses($parts);

        if (! empty($deleteUsingWhereClauses)) {
            $lastPart = end($parts);
            if (\is_string($lastPart) && \str_starts_with($lastPart, 'WHERE ')) {
                $parts[\count($parts) - 1] = $lastPart . ' AND ' . \implode(' AND ', $deleteUsingWhereClauses);
            } else {
                $parts[] = 'WHERE ' . \implode(' AND ', $deleteUsingWhereClauses);
            }
        }

        return new Plan(\implode(' ', $parts), $this->bindings, executor: $this->executor);
    }

    #[\Override]
    public function upsert(): Plan
    {
        $result = parent::upsert();

        return $this->appendReturning($result);
    }

    #[\Override]
    public function upsertSelect(): Plan
    {
        $result = parent::upsertSelect();

        return $this->appendReturning($result);
    }

    private function appendReturning(Plan $result): Plan
    {
        if (empty($this->returningColumns)) {
            return $result;
        }

        $columns = \array_map(
            fn (string $col): string => $col === '*' ? '*' : $this->resolveAndWrap($col),
            $this->returningColumns
        );

        return new Plan(
            $result->query . ' RETURNING ' . \implode(', ', $columns),
            $result->bindings,
            executor: $this->executor,
        );
    }

    #[\Override]
    public function orderByVectorDistance(string $attribute, array $vector, VectorMetric $metric = VectorMetric::Cosine): static
    {
        $this->vectorOrder = [
            'attribute' => $attribute,
            'vector' => $vector,
            'metric' => $metric,
        ];

        return $this;
    }

    #[\Override]
    public function setJsonAppend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            'COALESCE(' . $this->resolveAndWrap($column) . ', \'[]\'::jsonb) || ?::jsonb',
            [\json_encode($values)],
        );

        return $this;
    }

    #[\Override]
    public function setJsonPrepend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            '?::jsonb || COALESCE(' . $this->resolveAndWrap($column) . ', \'[]\'::jsonb)',
            [\json_encode($values)],
        );

        return $this;
    }

    #[\Override]
    public function setJsonInsert(string $column, int $index, mixed $value): static
    {
        $this->jsonSets[$column] = new Condition(
            'jsonb_insert(' . $this->resolveAndWrap($column) . ', \'{' . $index . '}\', ?::jsonb)',
            [\json_encode($value)],
        );

        return $this;
    }

    #[\Override]
    public function setJsonRemove(string $column, mixed $value): static
    {
        $this->jsonSets[$column] = new Condition(
            $this->resolveAndWrap($column) . ' - ?',
            [\json_encode($value)],
        );

        return $this;
    }

    #[\Override]
    public function setJsonIntersect(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT jsonb_agg(elem) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem WHERE elem <@ ?::jsonb)', [\json_encode($values)]);

        return $this;
    }

    #[\Override]
    public function setJsonDiff(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT COALESCE(jsonb_agg(elem), \'[]\'::jsonb) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem WHERE NOT elem <@ ?::jsonb)', [\json_encode($values)]);

        return $this;
    }

    #[\Override]
    public function setJsonUnique(string $column): static
    {
        $this->setRaw($column, '(SELECT jsonb_agg(DISTINCT elem) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem)');

        return $this;
    }

    #[\Override]
    public function explain(bool $analyze = false, bool $verbose = false, bool $buffers = false, string $format = ''): Plan
    {
        $normalizedFormat = \strtoupper($format);
        if (! \in_array($normalizedFormat, ['', 'TEXT', 'XML', 'JSON', 'YAML'], true)) {
            throw new ValidationException('Invalid EXPLAIN format: ' . $format);
        }
        $result = $this->build();
        $options = [];
        if ($analyze) {
            $options[] = 'ANALYZE';
        }
        if ($verbose) {
            $options[] = 'VERBOSE';
        }
        if ($buffers) {
            $options[] = 'BUFFERS';
        }
        if ($normalizedFormat !== '') {
            $options[] = 'FORMAT ' . $normalizedFormat;
        }
        $prefix = empty($options) ? 'EXPLAIN' : 'EXPLAIN (' . \implode(', ', $options) . ')';

        return new Plan($prefix . ' ' . $result->query, $result->bindings, readOnly: true, executor: $this->executor);
    }

    #[\Override]
    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();

        if ($method->isVector()) {
            $attribute = $this->resolveAndWrap($query->getAttribute());

            return $this->compileVectorFilter($method, $attribute, $query);
        }

        if ($query->getAttributeType() === ColumnType::Object->value) {
            return $this->compileObjectFilter($query);
        }

        return parent::compileFilter($query);
    }

    protected function compileObjectFilter(Query $query): string
    {
        $method = $query->getMethod();
        $rawAttr = $query->getAttribute();
        $isNested = \str_contains($rawAttr, '.');

        if ($isNested) {
            $attribute = $this->buildJsonbPath($rawAttr);

            return match ($method) {
                Method::Equal => $this->compileIn($attribute, $query->getValues()),
                Method::NotEqual => $this->compileNotIn($attribute, $query->getValues()),
                Method::LessThan => $this->compileComparison($attribute, '<', $query->getValues()),
                Method::LessThanEqual => $this->compileComparison($attribute, '<=', $query->getValues()),
                Method::GreaterThan => $this->compileComparison($attribute, '>', $query->getValues()),
                Method::GreaterThanEqual => $this->compileComparison($attribute, '>=', $query->getValues()),
                Method::StartsWith => $this->compileLike($attribute, $query->getValues(), '', '%', false),
                Method::NotStartsWith => $this->compileLike($attribute, $query->getValues(), '', '%', true),
                Method::EndsWith => $this->compileLike($attribute, $query->getValues(), '%', '', false),
                Method::NotEndsWith => $this->compileLike($attribute, $query->getValues(), '%', '', true),
                Method::Contains => $this->compileLike($attribute, $query->getValues(), '%', '%', false),
                Method::NotContains => $this->compileLike($attribute, $query->getValues(), '%', '%', true),
                Method::IsNull => $attribute . ' IS NULL',
                Method::IsNotNull => $attribute . ' IS NOT NULL',
                default => parent::compileFilter($query),
            };
        }

        $attribute = $this->resolveAndWrap($rawAttr);

        return match ($method) {
            Method::Equal, Method::NotEqual => $this->compileJsonbContainment($attribute, $query->getValues(), $method === Method::NotEqual, false),
            Method::Contains, Method::ContainsAny, Method::ContainsAll, Method::NotContains => $this->compileJsonbContainment($attribute, $query->getValues(), $method === Method::NotContains, true),
            Method::StartsWith => $this->compileLike($attribute . '::text', $query->getValues(), '', '%', false),
            Method::NotStartsWith => $this->compileLike($attribute . '::text', $query->getValues(), '', '%', true),
            Method::EndsWith => $this->compileLike($attribute . '::text', $query->getValues(), '%', '', false),
            Method::NotEndsWith => $this->compileLike($attribute . '::text', $query->getValues(), '%', '', true),
            Method::IsNull => $attribute . ' IS NULL',
            Method::IsNotNull => $attribute . ' IS NOT NULL',
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param array<mixed> $values
     */
    protected function compileJsonbContainment(string $attribute, array $values, bool $not, bool $wrapScalars): string
    {
        $conditions = [];
        foreach ($values as $value) {
            if ($wrapScalars && \is_array($value) && \count($value) === 1) {
                $jsonKey = \array_key_first($value);
                $jsonValue = $value[$jsonKey];
                if (!\is_array($jsonValue)) {
                    $value[$jsonKey] = [$jsonValue];
                }
            }
            $this->addBinding(\json_encode($value));
            $fragment = $attribute . ' @> ?::jsonb';
            $conditions[] = $not ? 'NOT (' . $fragment . ')' : $fragment;
        }
        $separator = $not ? ' AND ' : ' OR ';

        return '(' . \implode($separator, $conditions) . ')';
    }

    #[\Override]
    protected function getLikeKeyword(): string
    {
        return 'ILIKE';
    }

    protected function buildJsonbPath(string $path): string
    {
        $parts = \explode('.', $path);
        if (\count($parts) === 1) {
            return $this->resolveAndWrap($parts[0]);
        }

        $base = $this->quote($this->resolveAttribute($parts[0]));
        $lastKey = \array_pop($parts);
        \array_shift($parts);

        $chain = $base;
        foreach ($parts as $key) {
            $chain .= "->'" . $key . "'";
        }

        return $chain . "->>'" . $lastKey . "'";
    }

    #[\Override]
    protected function compileVectorOrderExpr(): ?Condition
    {
        if ($this->vectorOrder === null) {
            return null;
        }

        $attr = $this->resolveAndWrap($this->vectorOrder['attribute']);
        $operator = $this->vectorOrder['metric']->toOperator();
        $vectorJson = \json_encode($this->vectorOrder['vector']);

        return new Condition(
            '(' . $attr . ' ' . $operator . ' ?::vector) ASC',
            [$vectorJson],
        );
    }

    #[\Override]
    public function countWhen(string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'COUNT(*) FILTER (WHERE ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function sumWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'SUM(' . $this->resolveAndWrap($column) . ') FILTER (WHERE ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function avgWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'AVG(' . $this->resolveAndWrap($column) . ') FILTER (WHERE ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function minWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MIN(' . $this->resolveAndWrap($column) . ') FILTER (WHERE ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

    #[\Override]
    public function maxWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MAX(' . $this->resolveAndWrap($column) . ') FILTER (WHERE ' . $condition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, \array_values($bindings));
    }

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

    #[\Override]
    public function groupConcat(string $column, string $separator = ',', string $alias = '', ?array $orderBy = null): static
    {
        $col = $this->resolveAndWrap($column);
        $expr = 'STRING_AGG(' . $col . ', ?';
        if ($orderBy !== null && $orderBy !== []) {
            $orderCols = [];
            foreach ($orderBy as $orderCol) {
                if (\str_starts_with($orderCol, '-')) {
                    $orderCols[] = $this->resolveAndWrap(\substr($orderCol, 1)) . ' DESC';
                } else {
                    $orderCols[] = $this->resolveAndWrap($orderCol) . ' ASC';
                }
            }
            $expr .= ' ORDER BY ' . \implode(', ', $orderCols);
        }
        $expr .= ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, [$separator]);
    }

    #[\Override]
    public function jsonArrayAgg(string $column, string $alias = ''): static
    {
        $expr = 'JSON_AGG(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function jsonObjectAgg(string $keyColumn, string $valueColumn, string $alias = ''): static
    {
        $expr = 'JSON_OBJECT_AGG(' . $this->resolveAndWrap($keyColumn) . ', ' . $this->resolveAndWrap($valueColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function arrayAgg(string $column, string $alias = ''): static
    {
        $expr = 'ARRAY_AGG(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function boolAnd(string $column, string $alias = ''): static
    {
        $expr = 'BOOL_AND(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function boolOr(string $column, string $alias = ''): static
    {
        $expr = 'BOOL_OR(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function every(string $column, string $alias = ''): static
    {
        $expr = 'EVERY(' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function mode(string $column, string $alias = ''): static
    {
        $expr = 'MODE() WITHIN GROUP (ORDER BY ' . $this->resolveAndWrap($column) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr);
    }

    #[\Override]
    public function percentileCont(float $fraction, string $orderColumn, string $alias = ''): static
    {
        $expr = 'PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY ' . $this->resolveAndWrap($orderColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, [$fraction]);
    }

    #[\Override]
    public function percentileDisc(float $fraction, string $orderColumn, string $alias = ''): static
    {
        $expr = 'PERCENTILE_DISC(?) WITHIN GROUP (ORDER BY ' . $this->resolveAndWrap($orderColumn) . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, [$fraction]);
    }

    #[\Override]
    public function distinctOn(array $columns): static
    {
        $this->distinctOnColumns = $columns;

        return $this;
    }

    #[\Override]
    public function selectAggregateFilter(string $aggregateExpr, string $filterCondition, string $alias = '', array $bindings = []): static
    {
        $expr = $aggregateExpr . ' FILTER (WHERE ' . $filterCondition . ')';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->select($expr, $bindings);
    }

    #[\Override]
    public function insertDefaultValues(): Plan
    {
        $result = parent::insertDefaultValues();

        return $this->appendReturning($result);
    }

    #[\Override]
    public function withTotals(): static
    {
        throw new UnsupportedException('WITH TOTALS is not supported by PostgreSQL.');
    }

    #[\Override]
    public function withRollup(): static
    {
        $this->groupByModifier = 'ROLLUP';

        return $this;
    }

    #[\Override]
    public function withCube(): static
    {
        $this->groupByModifier = 'CUBE';

        return $this;
    }

    #[\Override]
    public function build(): Plan
    {
        $result = parent::build();
        $query = $result->query;
        $modified = false;

        if (! empty($this->distinctOnColumns)) {
            $cols = \array_map(
                fn (string $col): string => $this->resolveAndWrap($col),
                $this->distinctOnColumns
            );
            $distinctOnClause = 'SELECT DISTINCT ON (' . \implode(', ', $cols) . ')';
            $query = \preg_replace('/^SELECT(\s+DISTINCT)?/', $distinctOnClause, $query, 1) ?? $query;
            $modified = true;
        }

        if ($this->groupByModifier !== null) {
            $groupByPos = \strpos($query, 'GROUP BY ');
            if ($groupByPos !== false) {
                $afterGroupBy = $groupByPos + 9;
                $endPos = null;
                foreach (['HAVING ', 'WINDOW ', 'ORDER BY ', 'LIMIT ', 'OFFSET ', 'FETCH ', 'FOR '] as $keyword) {
                    $pos = \strpos($query, $keyword, $afterGroupBy);
                    if ($pos !== false && ($endPos === null || $pos < $endPos)) {
                        $endPos = $pos;
                    }
                }
                $columns = $endPos !== null
                    ? \rtrim(\substr($query, $afterGroupBy, $endPos - $afterGroupBy))
                    : \substr($query, $afterGroupBy);
                $replacement = 'GROUP BY ' . $this->groupByModifier . '(' . $columns . ')';
                $query = \substr($query, 0, $groupByPos) . $replacement . ($endPos !== null ? ' ' . \substr($query, $endPos) : '');
            }
            $modified = true;
        }

        if ($modified) {
            return new Plan($query, $result->bindings, $result->readOnly, $this->executor);
        }

        return $result;
    }

    #[\Override]
    public function reset(): static
    {
        parent::reset();
        $this->jsonSets = [];
        $this->vectorOrder = null;
        $this->returningColumns = [];
        $this->updateFromTable = '';
        $this->updateFromAlias = '';
        $this->updateFromCondition = '';
        $this->updateFromBindings = [];
        $this->deleteUsingTable = '';
        $this->deleteUsingCondition = '';
        $this->deleteUsingBindings = [];
        $this->mergeTarget = '';
        $this->mergeSource = null;
        $this->mergeSourceAlias = '';
        $this->mergeCondition = '';
        $this->mergeConditionBindings = [];
        $this->mergeClauses = [];
        $this->distinctOnColumns = [];
        $this->groupByModifier = null;

        return $this;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSpatialDistance(Method $method, string $attribute, array $values): string
    {
        /** @var array{0: string|array<mixed>, 1: float, 2: bool} $tuple */
        $tuple = $values[0];
        $filter = SpatialDistanceFilter::fromTuple($tuple);
        $wkt = \is_array($filter->geometry) ? $this->geometryToWkt($filter->geometry) : $filter->geometry;

        $operator = match ($method) {
            Method::DistanceLessThan => '<',
            Method::DistanceGreaterThan => '>',
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            default => '<',
        };

        $this->addBinding($wkt);
        $this->addBinding($filter->distance);

        if ($filter->meters) {
            return 'ST_Distance((' . $attribute . '::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) ' . $operator . ' ?';
        }

        return 'ST_Distance(' . $attribute . ', ST_GeomFromText(?, 4326)) ' . $operator . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string
    {
        /** @var array<mixed> $geometry */
        $geometry = $values[0];
        $wkt = $this->geometryToWkt($geometry);
        $this->addBinding($wkt);

        $expr = $function . '(' . $attribute . ', ST_GeomFromText(?, 4326))';

        return $not ? 'NOT ' . $expr : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSpatialCoversPredicate(string $attribute, array $values, bool $not): string
    {
        return $this->compileSpatialPredicate('ST_Covers', $attribute, $values, $not);
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string
    {
        $this->addBinding(\json_encode($values[0]));
        $expr = $attribute . ' @> ?::jsonb';

        return $not ? 'NOT (' . $expr . ')' : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileJsonOverlapsExpr(string $attribute, array $values): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];

        $conditions = [];
        foreach ($arr as $value) {
            $this->addBinding(\json_encode($value));
            $conditions[] = $attribute . ' @> ?::jsonb';
        }

        return '(' . \implode(' OR ', $conditions) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileJsonPathExpr(string $attribute, array $values): string
    {
        /** @var string $path */
        $path = $values[0];
        /** @var string $operator */
        $operator = $values[1];
        $value = $values[2];

        if (!\preg_match('/^[a-zA-Z0-9_.\[\]]+$/', $path)) {
            throw new ValidationException('Invalid JSON path: ' . $path);
        }

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        if (!\in_array($operator, $allowedOperators, true)) {
            throw new ValidationException('Invalid JSON path operator: ' . $operator);
        }

        $this->addBinding($value);

        return $attribute . '->>\''. $path . '\' ' . $operator . ' ?';
    }

    private function compileVectorFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();
        /** @var array<float> $vector */
        $vector = $values[0];

        $operator = match ($method) {
            Method::VectorCosine => '<=>',
            Method::VectorEuclidean => '<->',
            Method::VectorDot => '<#>',
            default => '<=>',
        };

        $this->addBinding(\json_encode($vector));

        return '(' . $attribute . ' ' . $operator . ' ?::vector)';
    }

}
