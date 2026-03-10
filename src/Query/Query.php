<?php

namespace Utopia\Query;

use JsonException;
use Utopia\Query\Builder\GroupedQueries;
use Utopia\Query\Exception as QueryException;

/** @phpstan-consistent-constructor */
class Query
{
    public const DEFAULT_ALIAS = 'main';

    protected Method $method;

    protected string $attribute = '';

    protected string $attributeType = '';

    protected bool $onArray = false;

    /**
     * @var array<mixed>
     */
    protected array $values = [];

    /**
     * Construct a new query object
     *
     * @param  array<mixed>  $values
     */
    public function __construct(Method|string $method, string $attribute = '', array $values = [])
    {
        $this->method = $method instanceof Method ? $method : Method::from($method);
        $this->attribute = $attribute;
        $this->values = $values;
    }

    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    public function getMethod(): Method
    {
        return $this->method;
    }

    public function getAttribute(): string
    {
        return $this->attribute;
    }

    /**
     * @return array<mixed>
     */
    public function getValues(): array
    {
        return $this->values;
    }

    public function getValue(mixed $default = null): mixed
    {
        return $this->values[0] ?? $default;
    }

    /**
     * Sets method
     */
    public function setMethod(Method|string $method): static
    {
        $this->method = $method instanceof Method ? $method : Method::from($method);

        return $this;
    }

    /**
     * Sets attribute
     */
    public function setAttribute(string $attribute): static
    {
        $this->attribute = $attribute;

        return $this;
    }

    /**
     * Sets values
     *
     * @param  array<mixed>  $values
     */
    public function setValues(array $values): static
    {
        $this->values = $values;

        return $this;
    }

    /**
     * Sets value
     */
    public function setValue(mixed $value): static
    {
        $this->values = [$value];

        return $this;
    }

    /**
     * Check if method is supported
     */
    public static function isMethod(string $value): bool
    {
        return Method::tryFrom($value) !== null;
    }

    /**
     * Check if method is a spatial-only query method
     */
    public function isSpatialQuery(): bool
    {
        return $this->method->isSpatial();
    }

    /**
     * Parse query
     *
     * @throws QueryException
     */
    public static function parse(string $query): static
    {
        try {
            $query = \json_decode($query, true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new QueryException('Invalid query: '.$e->getMessage());
        }

        if (! \is_array($query)) {
            throw new QueryException('Invalid query. Must be an array, got '.\gettype($query));
        }

        /** @var array<string, mixed> $query */
        return static::parseQuery($query);
    }

    /**
     * Parse query
     *
     * @param  array<string, mixed>  $query
     *
     * @throws QueryException
     */
    public static function parseQuery(array $query): static
    {
        $method = $query['method'] ?? '';
        $attribute = $query['attribute'] ?? '';
        $values = $query['values'] ?? [];

        if (! \is_string($method)) {
            throw new QueryException('Invalid query method. Must be a string, got '.\gettype($method));
        }

        if (! static::isMethod($method)) {
            throw new QueryException('Invalid query method: '.$method);
        }

        if (! \is_string($attribute)) {
            throw new QueryException('Invalid query attribute. Must be a string, got '.\gettype($attribute));
        }

        if (! \is_array($values)) {
            throw new QueryException('Invalid query values. Must be an array, got '.\gettype($values));
        }

        $methodEnum = Method::from($method);

        if ($methodEnum->isNested()) {
            foreach ($values as $index => $value) {
                /** @var array<string, mixed> $value */
                $values[$index] = static::parseQuery($value);
            }
        }

        return new static($methodEnum, $attribute, $values);
    }

    /**
     * Parse an array of queries
     *
     * @param  array<string>  $queries
     * @return array<static>
     *
     * @throws QueryException
     */
    public static function parseQueries(array $queries): array
    {
        $parsed = [];

        foreach ($queries as $query) {
            $parsed[] = static::parse($query);
        }

        return $parsed;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $array = ['method' => $this->method->value];

        if (! empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if ($this->method->isNested()) {
            foreach ($this->values as $index => $value) {
                /** @var Query $value */
                $array['values'][$index] = $value->toArray();
            }
        } else {
            $array['values'] = [];
            foreach ($this->values as $value) {
                $array['values'][] = $value;
            }
        }

        return $array;
    }

    /**
     * Compile this query using the given compiler
     */
    public function compile(Compiler $compiler): string
    {
        return match ($this->method) {
            Method::OrderAsc,
            Method::OrderDesc,
            Method::OrderRandom => $compiler->compileOrder($this),
            Method::Limit => $compiler->compileLimit($this),
            Method::Offset => $compiler->compileOffset($this),
            Method::CursorAfter,
            Method::CursorBefore => $compiler->compileCursor($this),
            Method::Select => $compiler->compileSelect($this),
            Method::Count,
            Method::CountDistinct,
            Method::Sum,
            Method::Avg,
            Method::Min,
            Method::Max => $compiler->compileAggregate($this),
            Method::GroupBy => $compiler->compileGroupBy($this),
            Method::Join,
            Method::LeftJoin,
            Method::RightJoin,
            Method::CrossJoin => $compiler->compileJoin($this),
            Method::Having => $compiler->compileFilter($this),
            default => $compiler->compileFilter($this),
        };
    }

    /**
     * @throws QueryException
     */
    public function toString(): string
    {
        try {
            return \json_encode($this->toArray(), flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new QueryException('Invalid Json: '.$e->getMessage());
        }
    }

    /**
     * Helper method to create Query with equal method
     *
     * @param  array<string|int|float|bool|null|array<mixed,mixed>>  $values
     */
    public static function equal(string $attribute, array $values): static
    {
        return new static(Method::Equal, $attribute, $values);
    }

    /**
     * Helper method to create Query with notEqual method
     *
     * @param  string|int|float|bool|null|array<mixed,mixed>  $value
     */
    public static function notEqual(string $attribute, string|int|float|bool|array|null $value): static
    {
        // maps or not an array
        if ((is_array($value) && ! array_is_list($value)) || ! is_array($value)) {
            $value = [$value];
        }

        return new static(Method::NotEqual, $attribute, $value);
    }

    /**
     * Helper method to create Query with lessThan method
     */
    public static function lessThan(string $attribute, string|int|float|bool $value): static
    {
        return new static(Method::LessThan, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThanEqual method
     */
    public static function lessThanEqual(string $attribute, string|int|float|bool $value): static
    {
        return new static(Method::LessThanEqual, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThan method
     */
    public static function greaterThan(string $attribute, string|int|float|bool $value): static
    {
        return new static(Method::GreaterThan, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThanEqual method
     */
    public static function greaterThanEqual(string $attribute, string|int|float|bool $value): static
    {
        return new static(Method::GreaterThanEqual, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with contains method
     *
     * @param  array<mixed>  $values
     */
    #[\Deprecated('Use containsAny() for array attributes, or keep using contains() for string substring matching.')]
    public static function contains(string $attribute, array $values): static
    {
        return new static(Method::Contains, $attribute, $values);
    }

    /**
     * Helper method to create Query with containsAny method.
     * For array and relationship attributes, matches documents where the attribute contains ANY of the given values.
     *
     * @param  array<mixed>  $values
     */
    public static function containsAny(string $attribute, array $values): static
    {
        return new static(Method::ContainsAny, $attribute, $values);
    }

    /**
     * Helper method to create Query with notContains method
     *
     * @param  array<mixed>  $values
     */
    public static function notContains(string $attribute, array $values): static
    {
        return new static(Method::NotContains, $attribute, $values);
    }

    /**
     * Helper method to create Query with between method
     */
    public static function between(string $attribute, string|int|float|bool $start, string|int|float|bool $end): static
    {
        return new static(Method::Between, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with notBetween method
     */
    public static function notBetween(string $attribute, string|int|float|bool $start, string|int|float|bool $end): static
    {
        return new static(Method::NotBetween, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with search method
     */
    public static function search(string $attribute, string $value): static
    {
        return new static(Method::Search, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with notSearch method
     */
    public static function notSearch(string $attribute, string $value): static
    {
        return new static(Method::NotSearch, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with select method
     *
     * @param  array<string>  $attributes
     */
    public static function select(array $attributes): static
    {
        return new static(Method::Select, values: $attributes);
    }

    /**
     * Helper method to create Query with orderDesc method
     */
    public static function orderDesc(string $attribute = ''): static
    {
        return new static(Method::OrderDesc, $attribute);
    }

    /**
     * Helper method to create Query with orderAsc method
     */
    public static function orderAsc(string $attribute = ''): static
    {
        return new static(Method::OrderAsc, $attribute);
    }

    /**
     * Helper method to create Query with orderRandom method
     */
    public static function orderRandom(): static
    {
        return new static(Method::OrderRandom);
    }

    /**
     * Helper method to create Query with limit method
     */
    public static function limit(int $value): static
    {
        return new static(Method::Limit, values: [$value]);
    }

    /**
     * Helper method to create Query with offset method
     */
    public static function offset(int $value): static
    {
        return new static(Method::Offset, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorAfter method
     */
    public static function cursorAfter(mixed $value): static
    {
        return new static(Method::CursorAfter, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     */
    public static function cursorBefore(mixed $value): static
    {
        return new static(Method::CursorBefore, values: [$value]);
    }

    /**
     * Helper method to create Query with isNull method
     */
    public static function isNull(string $attribute): static
    {
        return new static(Method::IsNull, $attribute);
    }

    /**
     * Helper method to create Query with isNotNull method
     */
    public static function isNotNull(string $attribute): static
    {
        return new static(Method::IsNotNull, $attribute);
    }

    public static function startsWith(string $attribute, string $value): static
    {
        return new static(Method::StartsWith, $attribute, [$value]);
    }

    public static function notStartsWith(string $attribute, string $value): static
    {
        return new static(Method::NotStartsWith, $attribute, [$value]);
    }

    public static function endsWith(string $attribute, string $value): static
    {
        return new static(Method::EndsWith, $attribute, [$value]);
    }

    public static function notEndsWith(string $attribute, string $value): static
    {
        return new static(Method::NotEndsWith, $attribute, [$value]);
    }

    /**
     * Helper method to create Query for documents created before a specific date
     */
    public static function createdBefore(string $value): static
    {
        return static::lessThan('$createdAt', $value);
    }

    /**
     * Helper method to create Query for documents created after a specific date
     */
    public static function createdAfter(string $value): static
    {
        return static::greaterThan('$createdAt', $value);
    }

    /**
     * Helper method to create Query for documents updated before a specific date
     */
    public static function updatedBefore(string $value): static
    {
        return static::lessThan('$updatedAt', $value);
    }

    /**
     * Helper method to create Query for documents updated after a specific date
     */
    public static function updatedAfter(string $value): static
    {
        return static::greaterThan('$updatedAt', $value);
    }

    /**
     * Helper method to create Query for documents created between two dates
     */
    public static function createdBetween(string $start, string $end): static
    {
        return static::between('$createdAt', $start, $end);
    }

    /**
     * Helper method to create Query for documents updated between two dates
     */
    public static function updatedBetween(string $start, string $end): static
    {
        return static::between('$updatedAt', $start, $end);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function or(array $queries): static
    {
        return new static(Method::Or, '', $queries);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function and(array $queries): static
    {
        return new static(Method::And, '', $queries);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function containsAll(string $attribute, array $values): static
    {
        return new static(Method::ContainsAll, $attribute, $values);
    }

    /**
     * Filters $queries for $types
     *
     * @param  array<static>  $queries
     * @param  array<Method>  $types
     * @return array<static>
     */
    public static function getByType(array $queries, array $types, bool $clone = true): array
    {
        $filtered = [];

        foreach ($queries as $query) {
            if (\in_array($query->getMethod(), $types, true)) {
                $filtered[] = $clone ? clone $query : $query;
            }
        }

        return $filtered;
    }

    /**
     * @param  array<static>  $queries
     * @return array<static>
     */
    public static function getCursorQueries(array $queries, bool $clone = true): array
    {
        return self::getByType(
            $queries,
            [
                Method::CursorAfter,
                Method::CursorBefore,
            ],
            $clone
        );
    }

    /**
     * Iterates through queries and groups them by type
     *
     * @param  array<mixed>  $queries
     */
    public static function groupByType(array $queries): GroupedQueries
    {
        $filters = [];
        $selections = [];
        $aggregations = [];
        $groupBy = [];
        $having = [];
        $distinct = false;
        $joins = [];
        $unions = [];
        $limit = null;
        $offset = null;
        $orderAttributes = [];
        $orderTypes = [];
        $cursor = null;
        $cursorDirection = null;

        foreach ($queries as $query) {
            if (! $query instanceof Query) {
                continue;
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();
            $values = $query->getValues();

            switch ($method) {
                case Method::OrderAsc:
                case Method::OrderDesc:
                case Method::OrderRandom:
                    if (! empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = match ($method) {
                        Method::OrderAsc => OrderDirection::Asc,
                        Method::OrderDesc => OrderDirection::Desc,
                        Method::OrderRandom => OrderDirection::Random,
                    };

                    break;
                case Method::Limit:
                    // Keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) {
                        break;
                    }

                    $limit = isset($values[0]) && \is_numeric($values[0]) ? \intval($values[0]) : $limit;
                    break;
                case Method::Offset:
                    // Keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) {
                        break;
                    }

                    $offset = isset($values[0]) && \is_numeric($values[0]) ? \intval($values[0]) : $offset;
                    break;
                case Method::CursorAfter:
                case Method::CursorBefore:
                    // Keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) {
                        break;
                    }

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === Method::CursorAfter ? CursorDirection::After : CursorDirection::Before;
                    break;

                case Method::Select:
                    $selections[] = clone $query;
                    break;

                case Method::Count:
                case Method::CountDistinct:
                case Method::Sum:
                case Method::Avg:
                case Method::Min:
                case Method::Max:
                    $aggregations[] = clone $query;
                    break;

                case Method::GroupBy:
                    /** @var array<string> $values */
                    foreach ($values as $col) {
                        $groupBy[] = $col;
                    }
                    break;

                case Method::Having:
                    $having[] = clone $query;
                    break;

                case Method::Distinct:
                    $distinct = true;
                    break;

                case Method::Join:
                case Method::LeftJoin:
                case Method::RightJoin:
                case Method::CrossJoin:
                    $joins[] = clone $query;
                    break;

                case Method::Union:
                case Method::UnionAll:
                    $unions[] = clone $query;
                    break;

                default:
                    $filters[] = clone $query;
                    break;
            }
        }

        return new GroupedQueries(
            filters: $filters,
            selections: $selections,
            aggregations: $aggregations,
            groupBy: $groupBy,
            having: $having,
            distinct: $distinct,
            joins: $joins,
            unions: $unions,
            limit: $limit,
            offset: $offset,
            orderAttributes: $orderAttributes,
            orderTypes: $orderTypes,
            cursor: $cursor,
            cursorDirection: $cursorDirection,
        );
    }

    /**
     * Is this query able to contain other queries
     */
    public function isNested(): bool
    {
        return $this->method->isNested();
    }

    public function onArray(): bool
    {
        return $this->onArray;
    }

    public function setOnArray(bool $bool): void
    {
        $this->onArray = $bool;
    }

    public function setAttributeType(string $type): void
    {
        $this->attributeType = $type;
    }

    public function getAttributeType(): string
    {
        return $this->attributeType;
    }

    // Spatial query methods

    /**
     * Helper method to create Query with distanceEqual method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceEqual(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(Method::DistanceEqual, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceNotEqual method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceNotEqual(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(Method::DistanceNotEqual, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceGreaterThan method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceGreaterThan(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(Method::DistanceGreaterThan, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceLessThan method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceLessThan(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(Method::DistanceLessThan, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with intersects method
     *
     * @param  array<mixed>  $values
     */
    public static function intersects(string $attribute, array $values): static
    {
        return new static(Method::Intersects, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notIntersects method
     *
     * @param  array<mixed>  $values
     */
    public static function notIntersects(string $attribute, array $values): static
    {
        return new static(Method::NotIntersects, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with crosses method
     *
     * @param  array<mixed>  $values
     */
    public static function crosses(string $attribute, array $values): static
    {
        return new static(Method::Crosses, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notCrosses method
     *
     * @param  array<mixed>  $values
     */
    public static function notCrosses(string $attribute, array $values): static
    {
        return new static(Method::NotCrosses, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with overlaps method
     *
     * @param  array<mixed>  $values
     */
    public static function overlaps(string $attribute, array $values): static
    {
        return new static(Method::Overlaps, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notOverlaps method
     *
     * @param  array<mixed>  $values
     */
    public static function notOverlaps(string $attribute, array $values): static
    {
        return new static(Method::NotOverlaps, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with touches method
     *
     * @param  array<mixed>  $values
     */
    public static function touches(string $attribute, array $values): static
    {
        return new static(Method::Touches, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notTouches method
     *
     * @param  array<mixed>  $values
     */
    public static function notTouches(string $attribute, array $values): static
    {
        return new static(Method::NotTouches, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with vectorDot method
     *
     * @param  array<float>  $vector
     */
    public static function vectorDot(string $attribute, array $vector): static
    {
        return new static(Method::VectorDot, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorCosine method
     *
     * @param  array<float>  $vector
     */
    public static function vectorCosine(string $attribute, array $vector): static
    {
        return new static(Method::VectorCosine, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorEuclidean method
     *
     * @param  array<float>  $vector
     */
    public static function vectorEuclidean(string $attribute, array $vector): static
    {
        return new static(Method::VectorEuclidean, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with regex method
     */
    public static function regex(string $attribute, string $pattern): static
    {
        return new static(Method::Regex, $attribute, [$pattern]);
    }

    /**
     * Helper method to create Query with exists method
     *
     * @param  array<string>  $attributes
     */
    public static function exists(array $attributes): static
    {
        return new static(Method::Exists, '', $attributes);
    }

    /**
     * Helper method to create Query with notExists method
     *
     * @param  string|int|float|bool|array<mixed,mixed>  $attribute
     */
    public static function notExists(string|int|float|bool|array $attribute): static
    {
        return new static(Method::NotExists, '', is_array($attribute) ? $attribute : [$attribute]);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function elemMatch(string $attribute, array $queries): static
    {
        return new static(Method::ElemMatch, $attribute, $queries);
    }

    // Aggregation factory methods

    public static function count(string $attribute = '*', string $alias = ''): static
    {
        return new static(Method::Count, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function countDistinct(string $attribute, string $alias = ''): static
    {
        return new static(Method::CountDistinct, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function sum(string $attribute, string $alias = ''): static
    {
        return new static(Method::Sum, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function avg(string $attribute, string $alias = ''): static
    {
        return new static(Method::Avg, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function min(string $attribute, string $alias = ''): static
    {
        return new static(Method::Min, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function max(string $attribute, string $alias = ''): static
    {
        return new static(Method::Max, $attribute, $alias !== '' ? [$alias] : []);
    }

    /**
     * @param  array<string>  $attributes
     */
    public static function groupBy(array $attributes): static
    {
        return new static(Method::GroupBy, '', $attributes);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function having(array $queries): static
    {
        return new static(Method::Having, '', $queries);
    }

    public static function distinct(): static
    {
        return new static(Method::Distinct);
    }

    // Join factory methods

    public static function join(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $values = [$left, $operator, $right];
        if ($alias !== '') {
            $values[] = $alias;
        }

        return new static(Method::Join, $table, $values);
    }

    public static function leftJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $values = [$left, $operator, $right];
        if ($alias !== '') {
            $values[] = $alias;
        }

        return new static(Method::LeftJoin, $table, $values);
    }

    public static function rightJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static
    {
        $values = [$left, $operator, $right];
        if ($alias !== '') {
            $values[] = $alias;
        }

        return new static(Method::RightJoin, $table, $values);
    }

    public static function crossJoin(string $table, string $alias = ''): static
    {
        return new static(Method::CrossJoin, $table, $alias !== '' ? [$alias] : []);
    }

    // Union factory methods

    /**
     * @param  array<Query>  $queries
     */
    public static function union(array $queries): static
    {
        return new static(Method::Union, '', $queries);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function unionAll(array $queries): static
    {
        return new static(Method::UnionAll, '', $queries);
    }

    // JSON factory methods

    public static function jsonContains(string $attribute, mixed $value): static
    {
        return new static(Method::JsonContains, $attribute, [$value]);
    }

    public static function jsonNotContains(string $attribute, mixed $value): static
    {
        return new static(Method::JsonNotContains, $attribute, [$value]);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function jsonOverlaps(string $attribute, array $values): static
    {
        return new static(Method::JsonOverlaps, $attribute, [$values]);
    }

    public static function jsonPath(string $attribute, string $path, string $operator, mixed $value): static
    {
        return new static(Method::JsonPath, $attribute, [$path, $operator, $value]);
    }

    // Spatial predicate extras

    /**
     * @param  array<mixed>  $values
     */
    public static function covers(string $attribute, array $values): static
    {
        return new static(Method::Covers, $attribute, [$values]);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function notCovers(string $attribute, array $values): static
    {
        return new static(Method::NotCovers, $attribute, [$values]);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function spatialEquals(string $attribute, array $values): static
    {
        return new static(Method::SpatialEquals, $attribute, [$values]);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function notSpatialEquals(string $attribute, array $values): static
    {
        return new static(Method::NotSpatialEquals, $attribute, [$values]);
    }

    // Raw factory method

    /**
     * @param  array<mixed>  $bindings
     */
    public static function raw(string $sql, array $bindings = []): static
    {
        return new static(Method::Raw, $sql, $bindings);
    }

    // Convenience: page

    /**
     * Returns an array of limit and offset queries for page-based pagination
     *
     * @return array{0: static, 1: static}
     */
    public static function page(int $page, int $perPage = 25): array
    {
        if ($page < 1) {
            throw new \Utopia\Query\Exception\ValidationException('Page must be >= 1, got ' . $page);
        }
        if ($perPage < 1) {
            throw new \Utopia\Query\Exception\ValidationException('Per page must be >= 1, got ' . $perPage);
        }

        return [
            static::limit($perPage),
            static::offset(($page - 1) * $perPage),
        ];
    }

    // Static helpers

    /**
     * Merge two query arrays. For limit/offset/cursor, values from $queriesB override $queriesA.
     *
     * @param  array<static>  $queriesA
     * @param  array<static>  $queriesB
     * @return array<static>
     */
    public static function merge(array $queriesA, array $queriesB): array
    {
        $singularTypes = [
            Method::Limit,
            Method::Offset,
            Method::CursorAfter,
            Method::CursorBefore,
        ];

        $result = $queriesA;

        foreach ($queriesB as $queryB) {
            $method = $queryB->getMethod();

            if (\in_array($method, $singularTypes, true)) {
                // Remove existing queries of the same type from result
                $result = \array_values(\array_filter(
                    $result,
                    fn (Query $q): bool => $q->getMethod() !== $method
                ));
            }

            $result[] = $queryB;
        }

        return $result;
    }

    /**
     * Returns queries in A that are not in B (compared by toArray())
     *
     * @param  array<static>  $queriesA
     * @param  array<static>  $queriesB
     * @return array<static>
     */
    public static function diff(array $queriesA, array $queriesB): array
    {
        $bArrays = \array_map(fn (Query $q): array => $q->toArray(), $queriesB);

        $result = [];
        foreach ($queriesA as $queryA) {
            $aArray = $queryA->toArray();
            if (! array_any($bArrays, fn (array $b): bool => $aArray === $b)) {
                $result[] = $queryA;
            }
        }

        return $result;
    }

    /**
     * Validate queries against allowed attributes
     *
     * @param  array<static>  $queries
     * @param  array<string>  $allowedAttributes
     * @return array<string>  Error messages
     */
    public static function validate(array $queries, array $allowedAttributes): array
    {
        $errors = [];
        $skipTypes = [
            Method::Limit,
            Method::Offset,
            Method::CursorAfter,
            Method::CursorBefore,
            Method::OrderRandom,
            Method::Distinct,
            Method::Select,
            Method::Exists,
            Method::NotExists,
        ];

        foreach ($queries as $query) {
            $method = $query->getMethod();

            // Recursively validate nested queries
            if ($method->isNested()) {
                /** @var array<static> $nested */
                $nested = $query->getValues();
                $errors = \array_merge($errors, static::validate($nested, $allowedAttributes));

                continue;
            }

            if (\in_array($method, $skipTypes, true)) {
                continue;
            }

            // GROUP_BY stores attributes in values
            if ($method === Method::GroupBy) {
                /** @var array<string> $columns */
                $columns = $query->getValues();
                foreach ($columns as $col) {
                    if (! \in_array($col, $allowedAttributes, true)) {
                        $errors[] = "Invalid attribute \"{$col}\" used in {$method->value}";
                    }
                }

                continue;
            }

            $attribute = $query->getAttribute();

            if ($attribute === '' || $attribute === '*') {
                continue;
            }

            if (! \in_array($attribute, $allowedAttributes, true)) {
                $errors[] = "Invalid attribute \"{$attribute}\" used in {$method->value}";
            }
        }

        return $errors;
    }
}
