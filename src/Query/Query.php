<?php

namespace Utopia\Query;

use JsonException;
use Utopia\Query\Exception as QueryException;

/** @phpstan-consistent-constructor */
class Query
{
    // Filter methods
    public const TYPE_EQUAL = 'equal';

    public const TYPE_NOT_EQUAL = 'notEqual';

    public const TYPE_LESSER = 'lessThan';

    public const TYPE_LESSER_EQUAL = 'lessThanEqual';

    public const TYPE_GREATER = 'greaterThan';

    public const TYPE_GREATER_EQUAL = 'greaterThanEqual';

    public const TYPE_CONTAINS = 'contains';

    public const TYPE_CONTAINS_ANY = 'containsAny';

    public const TYPE_NOT_CONTAINS = 'notContains';

    public const TYPE_SEARCH = 'search';

    public const TYPE_NOT_SEARCH = 'notSearch';

    public const TYPE_IS_NULL = 'isNull';

    public const TYPE_IS_NOT_NULL = 'isNotNull';

    public const TYPE_BETWEEN = 'between';

    public const TYPE_NOT_BETWEEN = 'notBetween';

    public const TYPE_STARTS_WITH = 'startsWith';

    public const TYPE_NOT_STARTS_WITH = 'notStartsWith';

    public const TYPE_ENDS_WITH = 'endsWith';

    public const TYPE_NOT_ENDS_WITH = 'notEndsWith';

    public const TYPE_REGEX = 'regex';

    public const TYPE_EXISTS = 'exists';

    public const TYPE_NOT_EXISTS = 'notExists';

    // Spatial methods
    public const TYPE_CROSSES = 'crosses';

    public const TYPE_NOT_CROSSES = 'notCrosses';

    public const TYPE_DISTANCE_EQUAL = 'distanceEqual';

    public const TYPE_DISTANCE_NOT_EQUAL = 'distanceNotEqual';

    public const TYPE_DISTANCE_GREATER_THAN = 'distanceGreaterThan';

    public const TYPE_DISTANCE_LESS_THAN = 'distanceLessThan';

    public const TYPE_INTERSECTS = 'intersects';

    public const TYPE_NOT_INTERSECTS = 'notIntersects';

    public const TYPE_OVERLAPS = 'overlaps';

    public const TYPE_NOT_OVERLAPS = 'notOverlaps';

    public const TYPE_TOUCHES = 'touches';

    public const TYPE_NOT_TOUCHES = 'notTouches';

    // Vector query methods
    public const TYPE_VECTOR_DOT = 'vectorDot';

    public const TYPE_VECTOR_COSINE = 'vectorCosine';

    public const TYPE_VECTOR_EUCLIDEAN = 'vectorEuclidean';

    public const TYPE_SELECT = 'select';

    // Order methods
    public const TYPE_ORDER_DESC = 'orderDesc';

    public const TYPE_ORDER_ASC = 'orderAsc';

    public const TYPE_ORDER_RANDOM = 'orderRandom';

    // Pagination methods
    public const TYPE_LIMIT = 'limit';

    public const TYPE_OFFSET = 'offset';

    public const TYPE_CURSOR_AFTER = 'cursorAfter';

    public const TYPE_CURSOR_BEFORE = 'cursorBefore';

    // Logical methods
    public const TYPE_AND = 'and';

    public const TYPE_OR = 'or';

    public const TYPE_CONTAINS_ALL = 'containsAll';

    public const TYPE_ELEM_MATCH = 'elemMatch';

    // Aggregation methods
    public const TYPE_COUNT = 'count';

    public const TYPE_SUM = 'sum';

    public const TYPE_AVG = 'avg';

    public const TYPE_MIN = 'min';

    public const TYPE_MAX = 'max';

    public const TYPE_GROUP_BY = 'groupBy';

    public const TYPE_HAVING = 'having';

    // Distinct
    public const TYPE_DISTINCT = 'distinct';

    // Join methods
    public const TYPE_JOIN = 'join';

    public const TYPE_LEFT_JOIN = 'leftJoin';

    public const TYPE_RIGHT_JOIN = 'rightJoin';

    public const TYPE_CROSS_JOIN = 'crossJoin';

    // Union
    public const TYPE_UNION = 'union';

    public const TYPE_UNION_ALL = 'unionAll';

    // Raw
    public const TYPE_RAW = 'raw';

    public const DEFAULT_ALIAS = 'main';

    // Order direction constants (inlined from Database)
    public const ORDER_ASC = 'ASC';

    public const ORDER_DESC = 'DESC';

    public const ORDER_RANDOM = 'RANDOM';

    // Cursor direction constants (inlined from Database)
    public const CURSOR_AFTER = 'after';

    public const CURSOR_BEFORE = 'before';

    public const TYPES = [
        self::TYPE_EQUAL,
        self::TYPE_NOT_EQUAL,
        self::TYPE_LESSER,
        self::TYPE_LESSER_EQUAL,
        self::TYPE_GREATER,
        self::TYPE_GREATER_EQUAL,
        self::TYPE_CONTAINS,
        self::TYPE_CONTAINS_ANY,
        self::TYPE_NOT_CONTAINS,
        self::TYPE_SEARCH,
        self::TYPE_NOT_SEARCH,
        self::TYPE_IS_NULL,
        self::TYPE_IS_NOT_NULL,
        self::TYPE_BETWEEN,
        self::TYPE_NOT_BETWEEN,
        self::TYPE_STARTS_WITH,
        self::TYPE_NOT_STARTS_WITH,
        self::TYPE_ENDS_WITH,
        self::TYPE_NOT_ENDS_WITH,
        self::TYPE_CROSSES,
        self::TYPE_NOT_CROSSES,
        self::TYPE_DISTANCE_EQUAL,
        self::TYPE_DISTANCE_NOT_EQUAL,
        self::TYPE_DISTANCE_GREATER_THAN,
        self::TYPE_DISTANCE_LESS_THAN,
        self::TYPE_INTERSECTS,
        self::TYPE_NOT_INTERSECTS,
        self::TYPE_OVERLAPS,
        self::TYPE_NOT_OVERLAPS,
        self::TYPE_TOUCHES,
        self::TYPE_NOT_TOUCHES,
        self::TYPE_VECTOR_DOT,
        self::TYPE_VECTOR_COSINE,
        self::TYPE_VECTOR_EUCLIDEAN,
        self::TYPE_EXISTS,
        self::TYPE_NOT_EXISTS,
        self::TYPE_SELECT,
        self::TYPE_ORDER_DESC,
        self::TYPE_ORDER_ASC,
        self::TYPE_ORDER_RANDOM,
        self::TYPE_LIMIT,
        self::TYPE_OFFSET,
        self::TYPE_CURSOR_AFTER,
        self::TYPE_CURSOR_BEFORE,
        self::TYPE_AND,
        self::TYPE_OR,
        self::TYPE_CONTAINS_ALL,
        self::TYPE_ELEM_MATCH,
        self::TYPE_REGEX,
        self::TYPE_COUNT,
        self::TYPE_SUM,
        self::TYPE_AVG,
        self::TYPE_MIN,
        self::TYPE_MAX,
        self::TYPE_GROUP_BY,
        self::TYPE_HAVING,
        self::TYPE_DISTINCT,
        self::TYPE_JOIN,
        self::TYPE_LEFT_JOIN,
        self::TYPE_RIGHT_JOIN,
        self::TYPE_CROSS_JOIN,
        self::TYPE_UNION,
        self::TYPE_UNION_ALL,
        self::TYPE_RAW,
    ];

    public const AGGREGATE_TYPES = [
        self::TYPE_COUNT,
        self::TYPE_SUM,
        self::TYPE_AVG,
        self::TYPE_MIN,
        self::TYPE_MAX,
    ];

    public const JOIN_TYPES = [
        self::TYPE_JOIN,
        self::TYPE_LEFT_JOIN,
        self::TYPE_RIGHT_JOIN,
        self::TYPE_CROSS_JOIN,
    ];

    public const VECTOR_TYPES = [
        self::TYPE_VECTOR_DOT,
        self::TYPE_VECTOR_COSINE,
        self::TYPE_VECTOR_EUCLIDEAN,
    ];

    protected const LOGICAL_TYPES = [
        self::TYPE_AND,
        self::TYPE_OR,
        self::TYPE_ELEM_MATCH,
        self::TYPE_HAVING,
        self::TYPE_UNION,
        self::TYPE_UNION_ALL,
    ];

    protected string $method = '';

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
    public function __construct(string $method, string $attribute = '', array $values = [])
    {
        $this->method = $method;
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

    public function getMethod(): string
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
    public function setMethod(string $method): static
    {
        $this->method = $method;

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
        return match ($value) {
            self::TYPE_EQUAL,
            self::TYPE_NOT_EQUAL,
            self::TYPE_LESSER,
            self::TYPE_LESSER_EQUAL,
            self::TYPE_GREATER,
            self::TYPE_GREATER_EQUAL,
            self::TYPE_CONTAINS,
            self::TYPE_CONTAINS_ANY,
            self::TYPE_NOT_CONTAINS,
            self::TYPE_SEARCH,
            self::TYPE_NOT_SEARCH,
            self::TYPE_ORDER_ASC,
            self::TYPE_ORDER_DESC,
            self::TYPE_ORDER_RANDOM,
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE,
            self::TYPE_IS_NULL,
            self::TYPE_IS_NOT_NULL,
            self::TYPE_BETWEEN,
            self::TYPE_NOT_BETWEEN,
            self::TYPE_STARTS_WITH,
            self::TYPE_NOT_STARTS_WITH,
            self::TYPE_ENDS_WITH,
            self::TYPE_NOT_ENDS_WITH,
            self::TYPE_CROSSES,
            self::TYPE_NOT_CROSSES,
            self::TYPE_DISTANCE_EQUAL,
            self::TYPE_DISTANCE_NOT_EQUAL,
            self::TYPE_DISTANCE_GREATER_THAN,
            self::TYPE_DISTANCE_LESS_THAN,
            self::TYPE_INTERSECTS,
            self::TYPE_NOT_INTERSECTS,
            self::TYPE_OVERLAPS,
            self::TYPE_NOT_OVERLAPS,
            self::TYPE_TOUCHES,
            self::TYPE_NOT_TOUCHES,
            self::TYPE_OR,
            self::TYPE_AND,
            self::TYPE_CONTAINS_ALL,
            self::TYPE_ELEM_MATCH,
            self::TYPE_SELECT,
            self::TYPE_VECTOR_DOT,
            self::TYPE_VECTOR_COSINE,
            self::TYPE_VECTOR_EUCLIDEAN,
            self::TYPE_EXISTS,
            self::TYPE_NOT_EXISTS,
            self::TYPE_REGEX,
            self::TYPE_COUNT,
            self::TYPE_SUM,
            self::TYPE_AVG,
            self::TYPE_MIN,
            self::TYPE_MAX,
            self::TYPE_GROUP_BY,
            self::TYPE_HAVING,
            self::TYPE_DISTINCT,
            self::TYPE_JOIN,
            self::TYPE_LEFT_JOIN,
            self::TYPE_RIGHT_JOIN,
            self::TYPE_CROSS_JOIN,
            self::TYPE_UNION,
            self::TYPE_UNION_ALL,
            self::TYPE_RAW => true,
            default => false,
        };
    }

    /**
     * Check if method is a spatial-only query method
     */
    public function isSpatialQuery(): bool
    {
        return match ($this->method) {
            self::TYPE_CROSSES,
            self::TYPE_NOT_CROSSES,
            self::TYPE_DISTANCE_EQUAL,
            self::TYPE_DISTANCE_NOT_EQUAL,
            self::TYPE_DISTANCE_GREATER_THAN,
            self::TYPE_DISTANCE_LESS_THAN,
            self::TYPE_INTERSECTS,
            self::TYPE_NOT_INTERSECTS,
            self::TYPE_OVERLAPS,
            self::TYPE_NOT_OVERLAPS,
            self::TYPE_TOUCHES,
            self::TYPE_NOT_TOUCHES => true,
            default => false,
        };
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

        if (\in_array($method, self::LOGICAL_TYPES, true)) {
            foreach ($values as $index => $value) {
                /** @var array<string, mixed> $value */
                $values[$index] = static::parseQuery($value);
            }
        }

        return new static($method, $attribute, $values);
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
        $array = ['method' => $this->method];

        if (! empty($this->attribute)) {
            $array['attribute'] = $this->attribute;
        }

        if (\in_array($this->method, self::LOGICAL_TYPES, true)) {
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
            self::TYPE_ORDER_ASC,
            self::TYPE_ORDER_DESC,
            self::TYPE_ORDER_RANDOM => $compiler->compileOrder($this),

            self::TYPE_LIMIT => $compiler->compileLimit($this),

            self::TYPE_OFFSET => $compiler->compileOffset($this),

            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE => $compiler->compileCursor($this),

            self::TYPE_SELECT => $compiler->compileSelect($this),

            self::TYPE_COUNT,
            self::TYPE_SUM,
            self::TYPE_AVG,
            self::TYPE_MIN,
            self::TYPE_MAX => $compiler->compileAggregate($this),

            self::TYPE_GROUP_BY => $compiler->compileGroupBy($this),

            self::TYPE_JOIN,
            self::TYPE_LEFT_JOIN,
            self::TYPE_RIGHT_JOIN,
            self::TYPE_CROSS_JOIN => $compiler->compileJoin($this),

            self::TYPE_HAVING => $compiler->compileFilter($this),

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
        return new static(self::TYPE_EQUAL, $attribute, $values);
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

        return new static(self::TYPE_NOT_EQUAL, $attribute, $value);
    }

    /**
     * Helper method to create Query with lessThan method
     */
    public static function lessThan(string $attribute, string|int|float|bool $value): static
    {
        return new static(self::TYPE_LESSER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with lessThanEqual method
     */
    public static function lessThanEqual(string $attribute, string|int|float|bool $value): static
    {
        return new static(self::TYPE_LESSER_EQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThan method
     */
    public static function greaterThan(string $attribute, string|int|float|bool $value): static
    {
        return new static(self::TYPE_GREATER, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with greaterThanEqual method
     */
    public static function greaterThanEqual(string $attribute, string|int|float|bool $value): static
    {
        return new static(self::TYPE_GREATER_EQUAL, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with contains method
     *
     * @deprecated Use containsAny() for array attributes, or keep using contains() for string substring matching.
     *
     * @param  array<mixed>  $values
     */
    public static function contains(string $attribute, array $values): static
    {
        return new static(self::TYPE_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with containsAny method.
     * For array and relationship attributes, matches documents where the attribute contains ANY of the given values.
     *
     * @param  array<mixed>  $values
     */
    public static function containsAny(string $attribute, array $values): static
    {
        return new static(self::TYPE_CONTAINS_ANY, $attribute, $values);
    }

    /**
     * Helper method to create Query with notContains method
     *
     * @param  array<mixed>  $values
     */
    public static function notContains(string $attribute, array $values): static
    {
        return new static(self::TYPE_NOT_CONTAINS, $attribute, $values);
    }

    /**
     * Helper method to create Query with between method
     */
    public static function between(string $attribute, string|int|float|bool $start, string|int|float|bool $end): static
    {
        return new static(self::TYPE_BETWEEN, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with notBetween method
     */
    public static function notBetween(string $attribute, string|int|float|bool $start, string|int|float|bool $end): static
    {
        return new static(self::TYPE_NOT_BETWEEN, $attribute, [$start, $end]);
    }

    /**
     * Helper method to create Query with search method
     */
    public static function search(string $attribute, string $value): static
    {
        return new static(self::TYPE_SEARCH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with notSearch method
     */
    public static function notSearch(string $attribute, string $value): static
    {
        return new static(self::TYPE_NOT_SEARCH, $attribute, [$value]);
    }

    /**
     * Helper method to create Query with select method
     *
     * @param  array<string>  $attributes
     */
    public static function select(array $attributes): static
    {
        return new static(self::TYPE_SELECT, values: $attributes);
    }

    /**
     * Helper method to create Query with orderDesc method
     */
    public static function orderDesc(string $attribute = ''): static
    {
        return new static(self::TYPE_ORDER_DESC, $attribute);
    }

    /**
     * Helper method to create Query with orderAsc method
     */
    public static function orderAsc(string $attribute = ''): static
    {
        return new static(self::TYPE_ORDER_ASC, $attribute);
    }

    /**
     * Helper method to create Query with orderRandom method
     */
    public static function orderRandom(): static
    {
        return new static(self::TYPE_ORDER_RANDOM);
    }

    /**
     * Helper method to create Query with limit method
     */
    public static function limit(int $value): static
    {
        return new static(self::TYPE_LIMIT, values: [$value]);
    }

    /**
     * Helper method to create Query with offset method
     */
    public static function offset(int $value): static
    {
        return new static(self::TYPE_OFFSET, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorAfter method
     */
    public static function cursorAfter(mixed $value): static
    {
        return new static(self::TYPE_CURSOR_AFTER, values: [$value]);
    }

    /**
     * Helper method to create Query with cursorBefore method
     */
    public static function cursorBefore(mixed $value): static
    {
        return new static(self::TYPE_CURSOR_BEFORE, values: [$value]);
    }

    /**
     * Helper method to create Query with isNull method
     */
    public static function isNull(string $attribute): static
    {
        return new static(self::TYPE_IS_NULL, $attribute);
    }

    /**
     * Helper method to create Query with isNotNull method
     */
    public static function isNotNull(string $attribute): static
    {
        return new static(self::TYPE_IS_NOT_NULL, $attribute);
    }

    public static function startsWith(string $attribute, string $value): static
    {
        return new static(self::TYPE_STARTS_WITH, $attribute, [$value]);
    }

    public static function notStartsWith(string $attribute, string $value): static
    {
        return new static(self::TYPE_NOT_STARTS_WITH, $attribute, [$value]);
    }

    public static function endsWith(string $attribute, string $value): static
    {
        return new static(self::TYPE_ENDS_WITH, $attribute, [$value]);
    }

    public static function notEndsWith(string $attribute, string $value): static
    {
        return new static(self::TYPE_NOT_ENDS_WITH, $attribute, [$value]);
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
        return new static(self::TYPE_OR, '', $queries);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function and(array $queries): static
    {
        return new static(self::TYPE_AND, '', $queries);
    }

    /**
     * @param  array<mixed>  $values
     */
    public static function containsAll(string $attribute, array $values): static
    {
        return new static(self::TYPE_CONTAINS_ALL, $attribute, $values);
    }

    /**
     * Filters $queries for $types
     *
     * @param  array<static>  $queries
     * @param  array<string>  $types
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
                Query::TYPE_CURSOR_AFTER,
                Query::TYPE_CURSOR_BEFORE,
            ],
            $clone
        );
    }

    /**
     * Iterates through queries and groups them by type
     *
     * @param  array<mixed>  $queries
     * @return array{
     *     filters: list<Query>,
     *     selections: list<Query>,
     *     aggregations: list<Query>,
     *     groupBy: list<string>,
     *     having: list<Query>,
     *     distinct: bool,
     *     joins: list<Query>,
     *     unions: list<Query>,
     *     limit: int|null,
     *     offset: int|null,
     *     orderAttributes: array<string>,
     *     orderTypes: array<string>,
     *     cursor: mixed,
     *     cursorDirection: string|null
     * }
     */
    public static function groupByType(array $queries): array
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
                case Query::TYPE_ORDER_ASC:
                case Query::TYPE_ORDER_DESC:
                case Query::TYPE_ORDER_RANDOM:
                    if (! empty($attribute)) {
                        $orderAttributes[] = $attribute;
                    }

                    $orderTypes[] = match ($method) {
                        Query::TYPE_ORDER_ASC => self::ORDER_ASC,
                        Query::TYPE_ORDER_DESC => self::ORDER_DESC,
                        Query::TYPE_ORDER_RANDOM => self::ORDER_RANDOM,
                    };

                    break;
                case Query::TYPE_LIMIT:
                    // Keep the 1st limit encountered and ignore the rest
                    if ($limit !== null) {
                        break;
                    }

                    $limit = isset($values[0]) && \is_numeric($values[0]) ? \intval($values[0]) : $limit;
                    break;
                case Query::TYPE_OFFSET:
                    // Keep the 1st offset encountered and ignore the rest
                    if ($offset !== null) {
                        break;
                    }

                    $offset = isset($values[0]) && \is_numeric($values[0]) ? \intval($values[0]) : $offset;
                    break;
                case Query::TYPE_CURSOR_AFTER:
                case Query::TYPE_CURSOR_BEFORE:
                    // Keep the 1st cursor encountered and ignore the rest
                    if ($cursor !== null) {
                        break;
                    }

                    $cursor = $values[0] ?? $limit;
                    $cursorDirection = $method === Query::TYPE_CURSOR_AFTER ? self::CURSOR_AFTER : self::CURSOR_BEFORE;
                    break;

                case Query::TYPE_SELECT:
                    $selections[] = clone $query;
                    break;

                case Query::TYPE_COUNT:
                case Query::TYPE_SUM:
                case Query::TYPE_AVG:
                case Query::TYPE_MIN:
                case Query::TYPE_MAX:
                    $aggregations[] = clone $query;
                    break;

                case Query::TYPE_GROUP_BY:
                    /** @var array<string> $values */
                    foreach ($values as $col) {
                        $groupBy[] = $col;
                    }
                    break;

                case Query::TYPE_HAVING:
                    $having[] = clone $query;
                    break;

                case Query::TYPE_DISTINCT:
                    $distinct = true;
                    break;

                case Query::TYPE_JOIN:
                case Query::TYPE_LEFT_JOIN:
                case Query::TYPE_RIGHT_JOIN:
                case Query::TYPE_CROSS_JOIN:
                    $joins[] = clone $query;
                    break;

                case Query::TYPE_UNION:
                case Query::TYPE_UNION_ALL:
                    $unions[] = clone $query;
                    break;

                default:
                    $filters[] = clone $query;
                    break;
            }
        }

        return [
            'filters' => $filters,
            'selections' => $selections,
            'aggregations' => $aggregations,
            'groupBy' => $groupBy,
            'having' => $having,
            'distinct' => $distinct,
            'joins' => $joins,
            'unions' => $unions,
            'limit' => $limit,
            'offset' => $offset,
            'orderAttributes' => $orderAttributes,
            'orderTypes' => $orderTypes,
            'cursor' => $cursor,
            'cursorDirection' => $cursorDirection,
        ];
    }

    /**
     * Is this query able to contain other queries
     */
    public function isNested(): bool
    {
        if (\in_array($this->getMethod(), self::LOGICAL_TYPES, true)) {
            return true;
        }

        return false;
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
        return new static(self::TYPE_DISTANCE_EQUAL, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceNotEqual method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceNotEqual(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(self::TYPE_DISTANCE_NOT_EQUAL, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceGreaterThan method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceGreaterThan(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(self::TYPE_DISTANCE_GREATER_THAN, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with distanceLessThan method
     *
     * @param  array<mixed>  $values
     */
    public static function distanceLessThan(string $attribute, array $values, int|float $distance, bool $meters = false): static
    {
        return new static(self::TYPE_DISTANCE_LESS_THAN, $attribute, [[$values, $distance, $meters]]);
    }

    /**
     * Helper method to create Query with intersects method
     *
     * @param  array<mixed>  $values
     */
    public static function intersects(string $attribute, array $values): static
    {
        return new static(self::TYPE_INTERSECTS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notIntersects method
     *
     * @param  array<mixed>  $values
     */
    public static function notIntersects(string $attribute, array $values): static
    {
        return new static(self::TYPE_NOT_INTERSECTS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with crosses method
     *
     * @param  array<mixed>  $values
     */
    public static function crosses(string $attribute, array $values): static
    {
        return new static(self::TYPE_CROSSES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notCrosses method
     *
     * @param  array<mixed>  $values
     */
    public static function notCrosses(string $attribute, array $values): static
    {
        return new static(self::TYPE_NOT_CROSSES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with overlaps method
     *
     * @param  array<mixed>  $values
     */
    public static function overlaps(string $attribute, array $values): static
    {
        return new static(self::TYPE_OVERLAPS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notOverlaps method
     *
     * @param  array<mixed>  $values
     */
    public static function notOverlaps(string $attribute, array $values): static
    {
        return new static(self::TYPE_NOT_OVERLAPS, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with touches method
     *
     * @param  array<mixed>  $values
     */
    public static function touches(string $attribute, array $values): static
    {
        return new static(self::TYPE_TOUCHES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with notTouches method
     *
     * @param  array<mixed>  $values
     */
    public static function notTouches(string $attribute, array $values): static
    {
        return new static(self::TYPE_NOT_TOUCHES, $attribute, [$values]);
    }

    /**
     * Helper method to create Query with vectorDot method
     *
     * @param  array<float>  $vector
     */
    public static function vectorDot(string $attribute, array $vector): static
    {
        return new static(self::TYPE_VECTOR_DOT, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorCosine method
     *
     * @param  array<float>  $vector
     */
    public static function vectorCosine(string $attribute, array $vector): static
    {
        return new static(self::TYPE_VECTOR_COSINE, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with vectorEuclidean method
     *
     * @param  array<float>  $vector
     */
    public static function vectorEuclidean(string $attribute, array $vector): static
    {
        return new static(self::TYPE_VECTOR_EUCLIDEAN, $attribute, [$vector]);
    }

    /**
     * Helper method to create Query with regex method
     */
    public static function regex(string $attribute, string $pattern): static
    {
        return new static(self::TYPE_REGEX, $attribute, [$pattern]);
    }

    /**
     * Helper method to create Query with exists method
     *
     * @param  array<string>  $attributes
     */
    public static function exists(array $attributes): static
    {
        return new static(self::TYPE_EXISTS, '', $attributes);
    }

    /**
     * Helper method to create Query with notExists method
     *
     * @param  string|int|float|bool|array<mixed,mixed>  $attribute
     */
    public static function notExists(string|int|float|bool|array $attribute): static
    {
        return new static(self::TYPE_NOT_EXISTS, '', is_array($attribute) ? $attribute : [$attribute]);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function elemMatch(string $attribute, array $queries): static
    {
        return new static(self::TYPE_ELEM_MATCH, $attribute, $queries);
    }

    // Aggregation factory methods

    public static function count(string $attribute = '*', string $alias = ''): static
    {
        return new static(self::TYPE_COUNT, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function sum(string $attribute, string $alias = ''): static
    {
        return new static(self::TYPE_SUM, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function avg(string $attribute, string $alias = ''): static
    {
        return new static(self::TYPE_AVG, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function min(string $attribute, string $alias = ''): static
    {
        return new static(self::TYPE_MIN, $attribute, $alias !== '' ? [$alias] : []);
    }

    public static function max(string $attribute, string $alias = ''): static
    {
        return new static(self::TYPE_MAX, $attribute, $alias !== '' ? [$alias] : []);
    }

    /**
     * @param  array<string>  $attributes
     */
    public static function groupBy(array $attributes): static
    {
        return new static(self::TYPE_GROUP_BY, '', $attributes);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function having(array $queries): static
    {
        return new static(self::TYPE_HAVING, '', $queries);
    }

    public static function distinct(): static
    {
        return new static(self::TYPE_DISTINCT);
    }

    // Join factory methods

    public static function join(string $table, string $left, string $right, string $operator = '='): static
    {
        return new static(self::TYPE_JOIN, $table, [$left, $operator, $right]);
    }

    public static function leftJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        return new static(self::TYPE_LEFT_JOIN, $table, [$left, $operator, $right]);
    }

    public static function rightJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        return new static(self::TYPE_RIGHT_JOIN, $table, [$left, $operator, $right]);
    }

    public static function crossJoin(string $table): static
    {
        return new static(self::TYPE_CROSS_JOIN, $table);
    }

    // Union factory methods

    /**
     * @param  array<Query>  $queries
     */
    public static function union(array $queries): static
    {
        return new static(self::TYPE_UNION, '', $queries);
    }

    /**
     * @param  array<Query>  $queries
     */
    public static function unionAll(array $queries): static
    {
        return new static(self::TYPE_UNION_ALL, '', $queries);
    }

    // Raw factory method

    /**
     * @param  array<mixed>  $bindings
     */
    public static function raw(string $sql, array $bindings = []): static
    {
        return new static(self::TYPE_RAW, $sql, $bindings);
    }

    // Convenience: page

    /**
     * Returns an array of limit and offset queries for page-based pagination
     *
     * @return array{0: static, 1: static}
     */
    public static function page(int $page, int $perPage = 25): array
    {
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
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE,
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
            $found = false;

            foreach ($bArrays as $bArray) {
                if ($aArray === $bArray) {
                    $found = true;
                    break;
                }
            }

            if (! $found) {
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
            self::TYPE_LIMIT,
            self::TYPE_OFFSET,
            self::TYPE_CURSOR_AFTER,
            self::TYPE_CURSOR_BEFORE,
            self::TYPE_ORDER_RANDOM,
            self::TYPE_DISTINCT,
            self::TYPE_SELECT,
            self::TYPE_EXISTS,
            self::TYPE_NOT_EXISTS,
        ];

        foreach ($queries as $query) {
            $method = $query->getMethod();

            // Recursively validate nested queries
            if (\in_array($method, self::LOGICAL_TYPES, true)) {
                /** @var array<static> $nested */
                $nested = $query->getValues();
                $errors = \array_merge($errors, static::validate($nested, $allowedAttributes));

                continue;
            }

            if (\in_array($method, $skipTypes, true)) {
                continue;
            }

            // GROUP_BY stores attributes in values
            if ($method === self::TYPE_GROUP_BY) {
                /** @var array<string> $columns */
                $columns = $query->getValues();
                foreach ($columns as $col) {
                    if (! \in_array($col, $allowedAttributes, true)) {
                        $errors[] = "Invalid attribute \"{$col}\" used in {$method}";
                    }
                }

                continue;
            }

            $attribute = $query->getAttribute();

            if ($attribute === '' || $attribute === '*') {
                continue;
            }

            if (! \in_array($attribute, $allowedAttributes, true)) {
                $errors[] = "Invalid attribute \"{$attribute}\" used in {$method}";
            }
        }

        return $errors;
    }
}
