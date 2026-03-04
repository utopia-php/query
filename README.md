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

### Building a Compiler

This library ships with a `Compiler` interface so you can translate queries into any backend syntax. Each query delegates to the correct compiler method via `$query->compile($compiler)`:

```php
use Utopia\Query\Compiler;
use Utopia\Query\Query;

class SQLCompiler implements Compiler
{
    public function compileFilter(Query $query): string
    {
        return match ($query->getMethod()) {
            Query::TYPE_EQUAL       => $query->getAttribute() . ' IN (' . $this->placeholders($query->getValues()) . ')',
            Query::TYPE_NOT_EQUAL   => $query->getAttribute() . ' != ?',
            Query::TYPE_GREATER     => $query->getAttribute() . ' > ?',
            Query::TYPE_LESSER      => $query->getAttribute() . ' < ?',
            Query::TYPE_BETWEEN     => $query->getAttribute() . ' BETWEEN ? AND ?',
            Query::TYPE_IS_NULL     => $query->getAttribute() . ' IS NULL',
            Query::TYPE_IS_NOT_NULL => $query->getAttribute() . ' IS NOT NULL',
            Query::TYPE_STARTS_WITH => $query->getAttribute() . " LIKE CONCAT(?, '%')",
            // ... handle remaining types
        };
    }

    public function compileOrder(Query $query): string
    {
        return match ($query->getMethod()) {
            Query::TYPE_ORDER_ASC    => $query->getAttribute() . ' ASC',
            Query::TYPE_ORDER_DESC   => $query->getAttribute() . ' DESC',
            Query::TYPE_ORDER_RANDOM => 'RAND()',
        };
    }

    public function compileLimit(Query $query): string
    {
        return 'LIMIT ' . $query->getValue();
    }

    public function compileOffset(Query $query): string
    {
        return 'OFFSET ' . $query->getValue();
    }

    public function compileSelect(Query $query): string
    {
        return implode(', ', $query->getValues());
    }

    public function compileCursor(Query $query): string
    {
        // Cursor-based pagination is adapter-specific
        return '';
    }
}
```

Then calling `compile()` on any query routes to the right method automatically:

```php
$compiler = new SQLCompiler();

$filter = Query::greaterThan('age', 18);
echo $filter->compile($compiler); // "age > ?"

$order = Query::orderAsc('name');
echo $order->compile($compiler); // "name ASC"

$limit = Query::limit(25);
echo $limit->compile($compiler); // "LIMIT 25"
```

The same interface works for any backend — implement `Compiler` for Redis, MongoDB, Elasticsearch, etc. and every query compiles without changes:

```php
class RedisCompiler implements Compiler
{
    public function compileFilter(Query $query): string
    {
        return match ($query->getMethod()) {
            Query::TYPE_BETWEEN => $query->getValues()[0] . ' ' . $query->getValues()[1],
            Query::TYPE_GREATER => '(' . $query->getValue() . ' +inf',
            // ... handle remaining types
        };
    }

    // ... implement remaining methods
}
```

This is the pattern used by [utopia-php/database](https://github.com/utopia-php/database) — it implements `Compiler` for each supported database engine, keeping application code fully decoupled from any particular storage backend.

### SQL Builder

The library includes a built-in `Builder` class that implements `Compiler` and provides a fluent API for building parameterized SQL queries:

```php
use Utopia\Query\Builder;
use Utopia\Query\Query;

// Fluent API
$result = (new Builder())
    ->select(['name', 'email'])
    ->from('users')
    ->filter([
        Query::equal('status', ['active']),
        Query::greaterThan('age', 18),
    ])
    ->sortAsc('name')
    ->limit(25)
    ->offset(0)
    ->build();

$result['query'];    // SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?
$result['bindings']; // ['active', 18, 25, 0]
```

**Batch mode** — pass all queries at once:

```php
$result = (new Builder())
    ->from('users')
    ->queries([
        Query::select(['name', 'email']),
        Query::equal('status', ['active']),
        Query::orderAsc('name'),
        Query::limit(25),
    ])
    ->build();
```

**Using with PDO:**

```php
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['active'])])
    ->limit(10)
    ->build();

$stmt = $pdo->prepare($result['query']);
$stmt->execute($result['bindings']);
$rows = $stmt->fetchAll();
```

**Pluggable extensions** — customize attribute mapping, identifier wrapping, and inject extra conditions:

```php
$result = (new Builder())
    ->from('users')
    ->setAttributeResolver(fn(string $a) => match($a) {
        '$id' => '_uid', '$createdAt' => '_createdAt', default => $a
    })
    ->setWrapChar('"') // PostgreSQL
    ->addConditionProvider(fn(string $table) => [
        "_uid IN (SELECT _document FROM {$table}_perms WHERE _type = 'read')",
        [],
    ])
    ->filter([Query::equal('status', ['active'])])
    ->build();
```

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
