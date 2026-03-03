# Utopia Query

[![Tests](https://github.com/utopia-php/query/actions/workflows/tests.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/tests.yml)
[![Linter](https://github.com/utopia-php/query/actions/workflows/linter.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/linter.yml)
[![Static Analysis](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml)

A simple PHP library providing a query abstraction for filtering, ordering, and pagination. It offers a fluent, type-safe API for building queries that can be serialized to JSON and parsed back, making it easy to pass query definitions between client and server or between services.

## Installation

```bash
composer require utopia-php/query
```

## System Requirements

- PHP 8.4+

## Usage

```php
use Utopia\Query\Query;
```

### Filter Queries

```php
// Equality
$query = Query::equal('status', ['active', 'pending']);
$query = Query::notEqual('role', 'guest');

// Comparison
$query = Query::greaterThan('age', 18);
$query = Query::greaterThanEqual('score', 90);
$query = Query::lessThan('price', 100);
$query = Query::lessThanEqual('quantity', 0);

// Range
$query = Query::between('createdAt', '2024-01-01', '2024-12-31');
$query = Query::notBetween('priority', 1, 3);

// String matching
$query = Query::startsWith('email', 'admin');
$query = Query::endsWith('filename', '.pdf');
$query = Query::search('content', 'hello world');
$query = Query::regex('slug', '^[a-z0-9-]+$');

// Array / contains
$query = Query::contains('tags', ['php', 'utopia']);
$query = Query::containsAny('categories', ['news', 'blog']);
$query = Query::containsAll('permissions', ['read', 'write']);
$query = Query::notContains('labels', ['deprecated']);

// Null checks
$query = Query::isNull('deletedAt');
$query = Query::isNotNull('verifiedAt');

// Existence
$query = Query::exists(['name', 'email']);
$query = Query::notExists('legacyField');

// Date helpers
$query = Query::createdAfter('2024-01-01');
$query = Query::updatedBetween('2024-01-01', '2024-06-30');
```

### Ordering and Pagination

```php
$query = Query::orderAsc('createdAt');
$query = Query::orderDesc('score');
$query = Query::orderRandom();

$query = Query::limit(25);
$query = Query::offset(50);

$query = Query::cursorAfter('doc_abc123');
$query = Query::cursorBefore('doc_xyz789');
```

### Logical Combinations

```php
$query = Query::and([
    Query::greaterThan('age', 18),
    Query::equal('status', ['active']),
]);

$query = Query::or([
    Query::equal('role', ['admin']),
    Query::equal('role', ['moderator']),
]);
```

### Spatial Queries

```php
$query = Query::distanceLessThan('location', [40.7128, -74.0060], 5000, meters: true);
$query = Query::distanceGreaterThan('location', [51.5074, -0.1278], 100);

$query = Query::intersects('area', [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]);
$query = Query::overlaps('region', [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]);
$query = Query::touches('boundary', [[0, 0], [1, 1]]);
$query = Query::crosses('path', [[0, 0], [5, 5]]);
```

### Vector Similarity

```php
$query = Query::vectorDot('embedding', [0.1, 0.2, 0.3, 0.4]);
$query = Query::vectorCosine('embedding', [0.1, 0.2, 0.3, 0.4]);
$query = Query::vectorEuclidean('embedding', [0.1, 0.2, 0.3, 0.4]);
```

### Selection

```php
$query = Query::select(['name', 'email', 'createdAt']);
```

### Serialization

Queries serialize to JSON and can be parsed back:

```php
$query = Query::equal('status', ['active']);

// Serialize to JSON string
$json = $query->toString();
// '{"method":"equal","attribute":"status","values":["active"]}'

// Parse back from JSON string
$parsed = Query::parse($json);

// Parse multiple queries
$queries = Query::parseQueries([$json1, $json2, $json3]);
```

### Grouping Helpers

`groupByType` splits an array of queries into categorized buckets:

```php
$queries = [
    Query::equal('status', ['active']),
    Query::greaterThan('age', 18),
    Query::orderAsc('name'),
    Query::limit(25),
    Query::offset(10),
    Query::select(['name', 'email']),
    Query::cursorAfter('abc123'),
];

$grouped = Query::groupByType($queries);

// $grouped['filters']         — filter Query objects
// $grouped['selections']      — select Query objects
// $grouped['limit']           — int|null
// $grouped['offset']          — int|null
// $grouped['orderAttributes'] — ['name']
// $grouped['orderTypes']      — ['ASC']
// $grouped['cursor']          — 'abc123'
// $grouped['cursorDirection'] — 'after'
```

`getByType` filters queries by one or more method types:

```php
$cursors = Query::getByType($queries, [Query::TYPE_CURSOR_AFTER, Query::TYPE_CURSOR_BEFORE]);
```

### Building an Adapter

The `Query` object is backend-agnostic — your library decides how to translate it. Use `groupByType` to break queries apart, then map each piece to your target syntax:

```php
use Utopia\Query\Query;

class SQLAdapter
{
    /**
     * @param array<Query> $queries
     */
    public function find(string $table, array $queries): array
    {
        $grouped = Query::groupByType($queries);

        // SELECT
        $columns = '*';
        if (!empty($grouped['selections'])) {
            $columns = implode(', ', $grouped['selections'][0]->getValues());
        }

        $sql = "SELECT {$columns} FROM {$table}";

        // WHERE
        $conditions = [];
        foreach ($grouped['filters'] as $filter) {
            $conditions[] = match ($filter->getMethod()) {
                Query::TYPE_EQUAL        => $filter->getAttribute() . ' IN (' . $this->placeholders($filter->getValues()) . ')',
                Query::TYPE_NOT_EQUAL    => $filter->getAttribute() . ' != ?',
                Query::TYPE_GREATER      => $filter->getAttribute() . ' > ?',
                Query::TYPE_LESSER       => $filter->getAttribute() . ' < ?',
                Query::TYPE_BETWEEN      => $filter->getAttribute() . ' BETWEEN ? AND ?',
                Query::TYPE_IS_NULL      => $filter->getAttribute() . ' IS NULL',
                Query::TYPE_IS_NOT_NULL  => $filter->getAttribute() . ' IS NOT NULL',
                Query::TYPE_STARTS_WITH  => $filter->getAttribute() . " LIKE CONCAT(?, '%')",
                // ... handle other types
            };
        }

        if (!empty($conditions)) {
            $sql .= ' WHERE ' . implode(' AND ', $conditions);
        }

        // ORDER BY
        foreach ($grouped['orderAttributes'] as $i => $attr) {
            $sql .= ($i === 0 ? ' ORDER BY ' : ', ') . $attr . ' ' . $grouped['orderTypes'][$i];
        }

        // LIMIT / OFFSET
        if ($grouped['limit'] !== null) {
            $sql .= ' LIMIT ' . $grouped['limit'];
        }
        if ($grouped['offset'] !== null) {
            $sql .= ' OFFSET ' . $grouped['offset'];
        }

        // Execute $sql with bound parameters ...
    }
}
```

The same pattern works for any backend. A Redis adapter might map filters to sorted-set range commands, an Elasticsearch adapter might build a `bool` query, or a MongoDB adapter might produce a `find()` filter document — the Query objects stay the same regardless:

```php
class RedisAdapter
{
    /**
     * @param array<Query> $queries
     */
    public function find(string $key, array $queries): array
    {
        $grouped = Query::groupByType($queries);

        foreach ($grouped['filters'] as $filter) {
            match ($filter->getMethod()) {
                Query::TYPE_BETWEEN => $this->redis->zRangeByScore(
                    $key,
                    $filter->getValues()[0],
                    $filter->getValues()[1],
                ),
                Query::TYPE_GREATER => $this->redis->zRangeByScore(
                    $key,
                    '(' . $filter->getValue(),
                    '+inf',
                ),
                // ... handle other types
            };
        }

        // ...
    }
}
```

This keeps your application code decoupled from any particular storage engine — swap adapters without changing a single query.

## Contributing

All code contributions should go through a pull request and be approved by a core developer before being merged. This is to ensure a proper review of all the code.

```bash
# Install dependencies
composer install

# Run tests
composer test

# Run linter
composer lint

# Auto-format code
composer format

# Run static analysis
composer check
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
