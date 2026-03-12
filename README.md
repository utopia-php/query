# Utopia Query

[![Tests](https://github.com/utopia-php/query/actions/workflows/tests.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/tests.yml)
[![Integration Tests](https://github.com/utopia-php/query/actions/workflows/integration.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/integration.yml)
[![Linter](https://github.com/utopia-php/query/actions/workflows/linter.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/linter.yml)
[![Static Analysis](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml)

A PHP library for building type-safe, dialect-aware SQL queries and DDL statements. Provides a fluent builder API with parameterized output for MySQL, MariaDB, PostgreSQL, SQLite, and ClickHouse, plus a serializable `Query` value object for passing query definitions between services.

## Installation

```bash
composer require utopia-php/query
```

**Requires PHP 8.4+**

## Table of Contents

- [Query Object](#query-object)
  - [Filters](#filters)
  - [Ordering and Pagination](#ordering-and-pagination)
  - [Logical Combinations](#logical-combinations)
  - [Spatial Queries](#spatial-queries)
  - [Vector Similarity](#vector-similarity)
  - [JSON Queries](#json-queries)
  - [Selection](#selection)
  - [Raw Expressions](#raw-expressions)
  - [Serialization](#serialization)
  - [Helpers](#helpers)
- [Query Builder](#query-builder)
  - [Basic Usage](#basic-usage)
  - [Aggregations](#aggregations)
  - [Conditional Aggregates](#conditional-aggregates)
  - [Joins](#joins)
  - [Unions and Set Operations](#unions-and-set-operations)
  - [CTEs (Common Table Expressions)](#ctes-common-table-expressions)
  - [Window Functions](#window-functions)
  - [CASE Expressions](#case-expressions)
  - [Inserts](#inserts)
  - [Updates](#updates)
  - [Deletes](#deletes)
  - [Upsert](#upsert)
  - [Locking](#locking)
  - [Transactions](#transactions)
  - [EXPLAIN](#explain)
  - [Conditional Building](#conditional-building)
  - [Builder Cloning and Callbacks](#builder-cloning-and-callbacks)
  - [Debugging](#debugging)
  - [Hooks](#hooks)
- [Dialect-Specific Features](#dialect-specific-features)
  - [MySQL](#mysql)
  - [MariaDB](#mariadb)
  - [PostgreSQL](#postgresql)
  - [SQLite](#sqlite)
  - [ClickHouse](#clickhouse)
  - [Feature Matrix](#feature-matrix)
- [Schema Builder](#schema-builder)
  - [Creating Tables](#creating-tables)
  - [Altering Tables](#altering-tables)
  - [Indexes](#indexes)
  - [Foreign Keys](#foreign-keys)
  - [Partitions](#partitions)
  - [Comments](#comments)
  - [Views](#views)
  - [Procedures and Triggers](#procedures-and-triggers)
  - [PostgreSQL Schema Extensions](#postgresql-schema-extensions)
  - [ClickHouse Schema](#clickhouse-schema)
  - [SQLite Schema](#sqlite-schema)
- [Compiler Interface](#compiler-interface)
- [Contributing](#contributing)
- [License](#license)

## Query Object

The `Query` class is a serializable value object representing a single query predicate. It serves as the input to the builder's `filter()`, `having()`, and other methods.

```php
use Utopia\Query\Query;
```

### Filters

```php
// Equality
Query::equal('status', ['active', 'pending']);
Query::notEqual('role', 'guest');

// Comparison
Query::greaterThan('age', 18);
Query::greaterThanEqual('score', 90);
Query::lessThan('price', 100);
Query::lessThanEqual('quantity', 0);

// Range
Query::between('createdAt', '2024-01-01', '2024-12-31');
Query::notBetween('priority', 1, 3);

// String matching
Query::startsWith('email', 'admin');
Query::endsWith('filename', '.pdf');
Query::search('content', 'hello world');
Query::regex('slug', '^[a-z0-9-]+$');

// Array / contains
Query::contains('tags', ['php', 'utopia']);
Query::containsAny('categories', ['news', 'blog']);
Query::containsAll('permissions', ['read', 'write']);
Query::notContains('labels', ['deprecated']);

// Null checks
Query::isNull('deletedAt');
Query::isNotNull('verifiedAt');

// Existence (compiles to IS NOT NULL / IS NULL)
Query::exists(['name', 'email']);
Query::notExists('legacyField');

// Date helpers
Query::createdAfter('2024-01-01');
Query::updatedBetween('2024-01-01', '2024-06-30');
```

### Ordering and Pagination

```php
Query::orderAsc('createdAt');
Query::orderDesc('score');
Query::orderRandom();

Query::limit(25);
Query::offset(50);

Query::cursorAfter('doc_abc123');
Query::cursorBefore('doc_xyz789');
```

### Logical Combinations

```php
Query::and([
    Query::greaterThan('age', 18),
    Query::equal('status', ['active']),
]);

Query::or([
    Query::equal('role', ['admin']),
    Query::equal('role', ['moderator']),
]);
```

### Spatial Queries

```php
Query::distanceLessThan('location', [40.7128, -74.0060], 5000, meters: true);
Query::distanceGreaterThan('location', [51.5074, -0.1278], 100);

Query::intersects('area', [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]);
Query::overlaps('region', [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]);
Query::touches('boundary', [[0, 0], [1, 1]]);
Query::crosses('path', [[0, 0], [5, 5]]);
Query::covers('zone', [1.0, 2.0]);
Query::spatialEquals('geom', [3.0, 4.0]);
```

### Vector Similarity

```php
Query::vectorDot('embedding', [0.1, 0.2, 0.3, 0.4]);
Query::vectorCosine('embedding', [0.1, 0.2, 0.3, 0.4]);
Query::vectorEuclidean('embedding', [0.1, 0.2, 0.3, 0.4]);
```

### JSON Queries

```php
Query::jsonContains('tags', 'php');
Query::jsonNotContains('tags', 'legacy');
Query::jsonOverlaps('categories', ['news', 'blog']);
Query::jsonPath('metadata', 'address.city', '=', 'London');
```

### Selection

```php
Query::select(['name', 'email', 'createdAt']);
```

### Raw Expressions

```php
Query::raw('score > ? AND score < ?', [10, 100]);
```

### Serialization

Queries serialize to JSON and can be parsed back:

```php
$query = Query::equal('status', ['active']);

// Serialize
$json = $query->toString();
// '{"method":"equal","attribute":"status","values":["active"]}'

// Parse back
$parsed = Query::parse($json);

// Parse multiple
$queries = Query::parseQueries([$json1, $json2]);
```

### Helpers

```php
// Group queries by type
$grouped = Query::groupByType($queries);
// $grouped->filters, $grouped->limit, $grouped->orderAttributes, etc.

// Filter by method type
$cursors = Query::getByType($queries, [Method::CursorAfter, Method::CursorBefore]);

// Merge (later limit/offset/cursor overrides earlier)
$merged = Query::merge($defaultQueries, $userQueries);

// Diff — queries in A not in B
$unique = Query::diff($queriesA, $queriesB);

// Validate attributes against an allow-list
$errors = Query::validate($queries, ['name', 'age', 'status']);

// Page helper — returns [limit, offset] queries
[$limit, $offset] = Query::page(3, 10);
```

## Query Builder

The builder generates parameterized SQL from the fluent API. Every `build()`, `insert()`, `update()`, and `delete()` call returns a `BuildResult` with `->query` (the SQL string), `->bindings` (the parameter array), and `->readOnly` (whether the query is read-only).

Five dialect implementations are provided:

- `Utopia\Query\Builder\MySQL` — MySQL
- `Utopia\Query\Builder\MariaDB` — MariaDB (extends MySQL with dialect-specific spatial handling)
- `Utopia\Query\Builder\PostgreSQL` — PostgreSQL
- `Utopia\Query\Builder\SQLite` — SQLite
- `Utopia\Query\Builder\ClickHouse` — ClickHouse

MySQL, MariaDB, PostgreSQL, and SQLite extend `Builder\SQL` which adds locking, transactions, upsert, spatial queries, and full-text search. ClickHouse extends `Builder` directly with its own `ALTER TABLE` mutation syntax.

### Basic Usage

```php
use Utopia\Query\Builder\MySQL as Builder;
use Utopia\Query\Query;

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

$result->query;    // SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?
$result->bindings; // ['active', 18, 25, 0]
$result->readOnly; // true
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

$stmt = $pdo->prepare($result->query);
$stmt->execute($result->bindings);
$rows = $stmt->fetchAll();
```

### Aggregations

```php
$result = (new Builder())
    ->from('orders')
    ->count('*', 'total')
    ->sum('price', 'total_price')
    ->select(['status'])
    ->groupBy(['status'])
    ->having([Query::greaterThan('total', 5)])
    ->build();

// SELECT COUNT(*) AS `total`, SUM(`price`) AS `total_price`, `status`
//   FROM `orders` GROUP BY `status` HAVING `total` > ?
```

**Distinct:**

```php
$result = (new Builder())
    ->from('users')
    ->distinct()
    ->select(['country'])
    ->build();

// SELECT DISTINCT `country` FROM `users`
```

### Conditional Aggregates

Available on MySQL, PostgreSQL, SQLite, and ClickHouse via the `ConditionalAggregates` interface:

```php
use Utopia\Query\Builder\PostgreSQL as Builder;

$result = (new Builder())
    ->from('orders')
    ->countWhen('status = ?', 'active_count', 'active')
    ->sumWhen('amount', 'status = ?', 'active_total', 'active')
    ->build();

// PostgreSQL: COUNT(*) FILTER (WHERE status = ?) AS "active_count", SUM("amount") FILTER (WHERE status = ?) AS "active_total"
// MySQL:      COUNT(CASE WHEN status = ? THEN 1 END) AS `active_count`, SUM(CASE WHEN status = ? THEN `amount` END) AS `active_total`
// ClickHouse: countIf(status = ?) AS `active_count`, sumIf(`amount`, status = ?) AS `active_total`
```

Also available: `avgWhen()`, `minWhen()`, `maxWhen()`.

### Joins

```php
$result = (new Builder())
    ->from('users')
    ->join('orders', 'users.id', 'orders.user_id')
    ->leftJoin('profiles', 'users.id', 'profiles.user_id')
    ->rightJoin('notes', 'users.id', 'notes.user_id')
    ->crossJoin('colors')
    ->naturalJoin('defaults')
    ->build();

// SELECT * FROM `users`
//   JOIN `orders` ON `users`.`id` = `orders`.`user_id`
//   LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id`
//   RIGHT JOIN `notes` ON `users`.`id` = `notes`.`user_id`
//   CROSS JOIN `colors`
//   NATURAL JOIN `defaults`
```

**Complex join conditions** with `joinWhere()`:

```php
use Utopia\Query\Builder\JoinType;

$result = (new Builder())
    ->from('users')
    ->joinWhere('orders', function ($join) {
        $join->on('users.id', 'orders.user_id')
            ->where('orders.status', '=', 'active');
    }, JoinType::Left)
    ->build();
```

**Full outer joins** (PostgreSQL, ClickHouse):

```php
$result = (new \Utopia\Query\Builder\PostgreSQL())
    ->from('left_table')
    ->fullOuterJoin('right_table', 'left_table.id', 'right_table.id')
    ->build();

// SELECT * FROM "left_table" FULL OUTER JOIN "right_table" ON "left_table"."id" = "right_table"."id"
```

**Lateral joins** (MySQL, PostgreSQL):

```php
$sub = (new Builder())->from('orders')->filter([Query::raw('orders.user_id = users.id')])->limit(3);

$result = (new Builder())
    ->from('users')
    ->joinLateral($sub, 'recent_orders')
    ->build();
```

### Unions and Set Operations

```php
$admins = (new Builder())->from('admins')->filter([Query::equal('role', ['admin'])]);

$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['active'])])
    ->union($admins)
    ->build();

// SELECT * FROM `users` WHERE `status` IN (?)
//   UNION SELECT * FROM `admins` WHERE `role` IN (?)
```

Also available: `unionAll()`, `intersect()`, `intersectAll()`, `except()`, `exceptAll()`.

### CTEs (Common Table Expressions)

```php
$activeUsers = (new Builder())->from('users')->filter([Query::equal('status', ['active'])]);

$result = (new Builder())
    ->with('active_users', $activeUsers)
    ->from('active_users')
    ->select(['name'])
    ->build();

// WITH `active_users` AS (SELECT * FROM `users` WHERE `status` IN (?))
//   SELECT `name` FROM `active_users`
```

Use `withRecursive()` for recursive CTEs, or `withRecursiveSeedStep()` to construct a recursive CTE from separate seed and step builders:

```php
$seed = (new Builder())->from('employees')->filter([Query::isNull('manager_id')]);
$step = (new Builder())->from('employees')->join('org', 'employees.manager_id', 'org.id');

$result = (new Builder())
    ->withRecursiveSeedStep('org', $seed, $step)
    ->from('org')
    ->build();
```

### Window Functions

```php
$result = (new Builder())
    ->from('sales')
    ->select(['employee', 'amount'])
    ->selectWindow('ROW_NUMBER()', 'row_num', partitionBy: ['department'], orderBy: ['amount'])
    ->selectWindow('SUM(amount)', 'running_total', partitionBy: ['department'], orderBy: ['date'])
    ->build();

// SELECT `employee`, `amount`,
//   ROW_NUMBER() OVER (PARTITION BY `department` ORDER BY `amount` ASC) AS `row_num`,
//   SUM(amount) OVER (PARTITION BY `department` ORDER BY `date` ASC) AS `running_total`
//   FROM `sales`
```

Prefix an `orderBy` column with `-` for descending order (e.g., `['-amount']`).

**Named window definitions** allow reusing the same window across multiple expressions:

```php
$result = (new Builder())
    ->from('sales')
    ->select(['employee', 'amount'])
    ->window('w', partitionBy: ['department'], orderBy: ['date'])
    ->selectWindow('ROW_NUMBER()', 'row_num', windowName: 'w')
    ->selectWindow('SUM(amount)', 'running_total', windowName: 'w')
    ->build();

// SELECT `employee`, `amount`,
//   ROW_NUMBER() OVER `w` AS `row_num`,
//   SUM(amount) OVER `w` AS `running_total`
//   FROM `sales`
//   WINDOW `w` AS (PARTITION BY `department` ORDER BY `date` ASC)
```

### CASE Expressions

```php
$result = (new Builder())
    ->from('orders')
    ->select(['id'])
    ->selectCase(
        (new Builder())->case()
            ->when('amount > ?', 'high', conditionBindings: [1000])
            ->when('amount > ?', 'medium', conditionBindings: [100])
            ->elseResult('low')
            ->alias('priority')
            ->build()
    )
    ->build();

// SELECT `id`, CASE WHEN amount > ? THEN ? WHEN amount > ? THEN ? ELSE ? END AS `priority`
//   FROM `orders`
```

### Inserts

```php
// Single row
$result = (new Builder())
    ->into('users')
    ->set('name', 'Alice')
    ->set('email', 'alice@example.com')
    ->insert();

// Batch insert
$result = (new Builder())
    ->into('users')
    ->set('name', 'Alice')->set('email', 'alice@example.com')
    ->addRow()
    ->set('name', 'Bob')->set('email', 'bob@example.com')
    ->insert();

// INSERT ... SELECT
$source = (new Builder())->from('archived_users')->filter([Query::equal('status', ['active'])]);

$result = (new Builder())
    ->into('users')
    ->fromSelect($source, ['name', 'email'])
    ->insertSelect();
```

### Updates

```php
$result = (new Builder())
    ->from('users')
    ->set('status', 'inactive')
    ->setRaw('updated_at', 'NOW()')
    ->filter([Query::equal('id', [42])])
    ->update();

// UPDATE `users` SET `status` = ?, `updated_at` = NOW() WHERE `id` IN (?)
```

### Deletes

```php
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['deleted'])])
    ->delete();

// DELETE FROM `users` WHERE `status` IN (?)
```

### Upsert

Available on MySQL, PostgreSQL, and SQLite builders (`Builder\SQL` subclasses):

```php
// MySQL — ON DUPLICATE KEY UPDATE
$result = (new Builder())
    ->into('counters')
    ->set('key', 'visits')
    ->set('value', 1)
    ->onConflict(['key'])
    ->upsert();

// PostgreSQL — ON CONFLICT (...) DO UPDATE SET
$result = (new \Utopia\Query\Builder\PostgreSQL())
    ->into('counters')
    ->set('key', 'visits')
    ->set('value', 1)
    ->onConflict(['key'])
    ->upsert();
```

**Insert or ignore** — skip rows that conflict instead of updating:

```php
$result = (new Builder())
    ->into('counters')
    ->set('key', 'visits')
    ->set('value', 1)
    ->onConflict(['key'])
    ->insertOrIgnore();

// MySQL:      INSERT IGNORE INTO `counters` ...
// PostgreSQL: INSERT INTO "counters" ... ON CONFLICT ("key") DO NOTHING
// SQLite:     INSERT OR IGNORE INTO `counters` ...
```

**Upsert from SELECT** — insert from a subquery with conflict resolution:

```php
$source = (new Builder())->from('staging')->select(['key', 'value']);

$result = (new Builder())
    ->into('counters')
    ->fromSelect($source, ['key', 'value'])
    ->onConflict(['key'])
    ->upsertSelect();
```

### Locking

Available on MySQL, PostgreSQL, and SQLite builders:

```php
$result = (new Builder())
    ->from('accounts')
    ->filter([Query::equal('id', [1])])
    ->forUpdate()
    ->build();

// SELECT * FROM `accounts` WHERE `id` IN (?) FOR UPDATE
```

Also available: `forShare()`, `forUpdateSkipLocked()`, `forUpdateNoWait()`, `forShareSkipLocked()`, `forShareNoWait()`.

PostgreSQL also supports table-specific locking: `forUpdateOf('accounts')`, `forShareOf('accounts')`.

### Transactions

Available on MySQL, PostgreSQL, and SQLite builders:

```php
$builder = new Builder();

$builder->begin();            // BEGIN
$builder->savepoint('sp1');   // SAVEPOINT `sp1`
$builder->rollbackToSavepoint('sp1');
$builder->commit();           // COMMIT
$builder->rollback();         // ROLLBACK
```

### EXPLAIN

Available on all builders. MySQL and PostgreSQL provide extended options:

```php
// Basic explain
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['active'])])
    ->explain();

// MySQL — with format
$result = (new \Utopia\Query\Builder\MySQL())
    ->from('users')
    ->explain(analyze: true, format: 'JSON');

// PostgreSQL — with analyze, verbose, buffers, format
$result = (new \Utopia\Query\Builder\PostgreSQL())
    ->from('users')
    ->explain(analyze: true, verbose: true, buffers: true, format: 'JSON');
```

### Conditional Building

`when()` applies a callback only when the condition is true:

```php
$result = (new Builder())
    ->from('users')
    ->when($filterActive, fn(Builder $b) => $b->filter([Query::equal('status', ['active'])]))
    ->build();
```

### Builder Cloning and Callbacks

**Cloning** creates a deep copy of the builder, useful for branching from a shared base:

```php
$base = (new Builder())->from('users')->filter([Query::equal('status', ['active'])]);
$withLimit = $base->clone()->limit(10);
$withSort = $base->clone()->sortAsc('name');
```

**Build callbacks** run before or after building:

```php
$result = (new Builder())
    ->from('users')
    ->beforeBuild(fn(Builder $b) => $b->filter([Query::isNotNull('email')]))
    ->afterBuild(fn(BuildResult $r) => new BuildResult("/* traced */ {$r->query}", $r->bindings, $r->readOnly))
    ->build();
```

### Debugging

`toRawSql()` inlines bindings for inspection (not for execution):

```php
$sql = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['active'])])
    ->limit(10)
    ->toRawSql();

// SELECT * FROM `users` WHERE `status` IN ('active') LIMIT 10
```

### Hooks

Hooks extend the builder with reusable, testable classes for attribute resolution and condition injection.

**Attribute hooks** map virtual field names to real column names:

```php
use Utopia\Query\Hook\Attribute\Map;

$result = (new Builder())
    ->from('users')
    ->addHook(new Map([
        '$id' => '_uid',
        '$createdAt' => '_createdAt',
    ]))
    ->filter([Query::equal('$id', ['abc'])])
    ->build();

// SELECT * FROM `users` WHERE `_uid` IN (?)
```

**Filter hooks** inject conditions into every query:

```php
use Utopia\Query\Hook\Filter\Tenant;

$result = (new Builder())
    ->from('users')
    ->addHook(new Tenant(['tenant_abc']))
    ->filter([Query::equal('status', ['active'])])
    ->build();

// SELECT * FROM `users`
//   WHERE `status` IN (?) AND `tenant_id` IN (?)
```

**Custom filter hooks** implement `Hook\Filter`:

```php
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

class SoftDeleteHook implements Filter
{
    public function filter(string $table): Condition
    {
        return new Condition('deleted_at IS NULL');
    }
}
```

**Join filter hooks** inject per-join conditions with placement control (ON vs WHERE):

```php
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

class ActiveJoinFilter implements JoinFilter
{
    public function filterJoin(string $table, JoinType $joinType): ?Condition
    {
        return new Condition(
            'active = ?',
            [1],
            match ($joinType) {
                JoinType::Left, JoinType::Right => Placement::On,
                default => Placement::Where,
            },
        );
    }
}
```

The built-in `Tenant` hook implements both `Filter` and `JoinFilter` — it automatically applies ON placement for LEFT/RIGHT joins and WHERE placement for INNER/CROSS joins.

**Write hooks** decorate rows before writes and run callbacks after create/update/delete operations:

```php
use Utopia\Query\Hook\Write;

class AuditHook implements Write
{
    public function decorateRow(array $row, array $metadata = []): array { /* ... */ }
    public function afterCreate(string $table, array $metadata, mixed $context): void { /* ... */ }
    public function afterUpdate(string $table, array $metadata, mixed $context): void { /* ... */ }
    public function afterBatchUpdate(string $table, array $updateData, array $metadata, mixed $context): void { /* ... */ }
    public function afterDelete(string $table, array $ids, mixed $context): void { /* ... */ }
}
```

## Dialect-Specific Features

### MySQL

```php
use Utopia\Query\Builder\MySQL as Builder;
```

**Spatial queries** — uses `ST_Distance()`, `ST_Intersects()`, `ST_Contains()`, etc.:

```php
$result = (new Builder())
    ->from('stores')
    ->filterDistance('location', [40.7128, -74.0060], '<', 5000, meters: true)
    ->build();

// WHERE ST_Distance(ST_SRID(`location`, 4326), ST_GeomFromText(?, 4326, 'axis-order=long-lat'), 'metre') < ?
```

All spatial predicates: `filterDistance`, `filterIntersects`, `filterNotIntersects`, `filterCrosses`, `filterNotCrosses`, `filterOverlaps`, `filterNotOverlaps`, `filterTouches`, `filterNotTouches`, `filterCovers`, `filterNotCovers`, `filterSpatialEquals`, `filterNotSpatialEquals`.

**JSON operations:**

```php
// Filtering
$result = (new Builder())
    ->from('products')
    ->filterJsonContains('tags', 'sale')
    ->filterJsonPath('metadata', 'color', '=', 'red')
    ->build();

// WHERE JSON_CONTAINS(`tags`, ?) AND JSON_EXTRACT(`metadata`, '$.color') = ?

// Mutations (in UPDATE)
$result = (new Builder())
    ->from('products')
    ->filter([Query::equal('id', [1])])
    ->setJsonAppend('tags', ['new-tag'])
    ->update();
```

JSON mutation methods: `setJsonAppend`, `setJsonPrepend`, `setJsonInsert`, `setJsonRemove`, `setJsonIntersect`, `setJsonDiff`, `setJsonUnique`.

**Query hints:**

```php
$result = (new Builder())
    ->from('users')
    ->hint('NO_INDEX_MERGE(users)')
    ->maxExecutionTime(5000)
    ->build();

// SELECT /*+ NO_INDEX_MERGE(users) max_execution_time(5000) */ * FROM `users`
```

**Full-text search** — `MATCH() AGAINST(? IN BOOLEAN MODE)`:

```php
$result = (new Builder())
    ->from('articles')
    ->filter([Query::search('content', 'hello world')])
    ->build();

// WHERE MATCH(`content`) AGAINST(? IN BOOLEAN MODE)
```

**UPDATE with JOIN:**

```php
$result = (new Builder())
    ->from('users')
    ->set('status', 'premium')
    ->updateJoin('orders', 'users.id', 'orders.user_id')
    ->filter([Query::greaterThan('orders.total', 1000)])
    ->update();
```

**DELETE with JOIN:**

```php
$result = (new Builder())
    ->from('users')
    ->deleteUsing('u', 'orders', 'u.id', 'orders.user_id')
    ->filter([Query::equal('orders.status', ['cancelled'])])
    ->delete();
```

### MariaDB

```php
use Utopia\Query\Builder\MariaDB as Builder;
```

Extends MySQL with MariaDB-specific spatial handling:
- Uses `ST_DISTANCE_SPHERE()` for meter-based distance calculations
- Uses `ST_GeomFromText()` without the `axis-order` parameter
- Validates that distance-in-meters only works between POINT types

All other MySQL features (JSON, hints, lateral joins, etc.) are inherited.

### PostgreSQL

```php
use Utopia\Query\Builder\PostgreSQL as Builder;
```

**Spatial queries** — uses PostGIS functions with geography casting for meter-based distance:

```php
$result = (new Builder())
    ->from('stores')
    ->filterDistance('location', [40.7128, -74.0060], '<', 5000, meters: true)
    ->build();

// WHERE ST_Distance(("location"::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) < ?
```

**Vector search** — uses pgvector operators (`<=>`, `<->`, `<#>`):

```php
use Utopia\Query\Builder\VectorMetric;

$result = (new Builder())
    ->from('documents')
    ->select(['title'])
    ->orderByVectorDistance('embedding', [0.1, 0.2, 0.3], VectorMetric::Cosine)
    ->limit(10)
    ->build();

// SELECT "title" FROM "documents" ORDER BY ("embedding" <=> ?::vector) ASC LIMIT ?
```

Metrics: `VectorMetric::Cosine` (`<=>`), `VectorMetric::Euclidean` (`<->`), `VectorMetric::Dot` (`<#>`).

**JSON operations** — uses native JSONB operators:

```php
$result = (new Builder())
    ->from('products')
    ->filterJsonContains('tags', 'sale')
    ->build();

// WHERE "tags" @> ?::jsonb
```

**Full-text search** — `to_tsvector() @@ websearch_to_tsquery()`:

```php
$result = (new Builder())
    ->from('articles')
    ->filter([Query::search('content', 'hello world')])
    ->build();

// WHERE to_tsvector("content") @@ websearch_to_tsquery(?)
```

**Regex** — uses PostgreSQL `~` operator instead of `REGEXP`. String matching uses `ILIKE` for case-insensitive comparison.

**RETURNING** — get affected rows back from INSERT/UPDATE/DELETE:

```php
$result = (new Builder())
    ->into('users')
    ->set('name', 'Alice')
    ->returning(['id', 'created_at'])
    ->insert();

// INSERT INTO "users" ("name") VALUES (?) RETURNING "id", "created_at"
```

**MERGE** — SQL standard MERGE statement:

```php
$source = (new Builder())->from('staging');

$result = (new Builder())
    ->mergeInto('target')
    ->using($source, 's')
    ->on('"target"."id" = "s"."id"')
    ->whenMatched('UPDATE SET "name" = "s"."name"')
    ->whenNotMatched('INSERT ("id", "name") VALUES ("s"."id", "s"."name")')
    ->executeMerge();
```

**UPDATE FROM / DELETE USING:**

```php
// UPDATE ... FROM
$result = (new Builder())
    ->from('users')
    ->set('status', 'premium')
    ->updateFrom('orders', 'o')
    ->updateFromWhere('"users"."id" = "o"."user_id"')
    ->update();

// DELETE ... USING
$result = (new Builder())
    ->from('users')
    ->deleteUsing('old_users', '"users"."id" = "old_users"."id"')
    ->delete();
```

**Table sampling:**

```php
$result = (new Builder())
    ->from('large_table')
    ->tablesample(10.0, 'BERNOULLI')
    ->count('*', 'approx')
    ->build();

// SELECT COUNT(*) AS "approx" FROM "large_table" TABLESAMPLE BERNOULLI (10)
```

### SQLite

```php
use Utopia\Query\Builder\SQLite as Builder;
```

Extends `Builder\SQL` with SQLite-specific behavior:
- JSON support via `json_each()` and `json_extract()`
- Conditional aggregates using `CASE WHEN` syntax
- `INSERT OR IGNORE` for insertOrIgnore
- Regex and full-text search throw `UnsupportedException`
- Spatial queries throw `UnsupportedException`

### ClickHouse

```php
use Utopia\Query\Builder\ClickHouse as Builder;
```

**FINAL** — force merging of data parts:

```php
$result = (new Builder())
    ->from('events')
    ->final()
    ->build();

// SELECT * FROM `events` FINAL
```

**SAMPLE** — approximate query processing:

```php
$result = (new Builder())
    ->from('events')
    ->sample(0.1)
    ->count('*', 'approx_total')
    ->build();

// SELECT COUNT(*) AS `approx_total` FROM `events` SAMPLE 0.1
```

**PREWHERE** — filter before reading columns (optimization for wide tables):

```php
$result = (new Builder())
    ->from('events')
    ->prewhere([Query::equal('event_type', ['click'])])
    ->filter([Query::greaterThan('count', 5)])
    ->build();

// SELECT * FROM `events` PREWHERE `event_type` IN (?) WHERE `count` > ?
```

**SETTINGS:**

```php
$result = (new Builder())
    ->from('events')
    ->settings(['max_threads' => '4', 'optimize_read_in_order' => '1'])
    ->build();

// SELECT * FROM `events` SETTINGS max_threads=4, optimize_read_in_order=1
```

**String matching** — uses native ClickHouse functions instead of LIKE:

```php
// startsWith/endsWith → native functions
Query::startsWith('name', 'Al');   // startsWith(`name`, ?)
Query::endsWith('file', '.pdf');   // endsWith(`file`, ?)

// contains/notContains → position()
Query::contains('tags', ['php']);   // position(`tags`, ?) > 0
```

**Regex** — uses `match()` function instead of `REGEXP`.

**UPDATE/DELETE** — compiles to `ALTER TABLE ... UPDATE/DELETE` with mandatory WHERE:

```php
$result = (new Builder())
    ->from('events')
    ->set('status', 'archived')
    ->filter([Query::lessThan('created_at', '2024-01-01')])
    ->update();

// ALTER TABLE `events` UPDATE `status` = ? WHERE `created_at` < ?
```

> **Note:** Full-text search (`Query::search()`) is not supported in ClickHouse and throws `UnsupportedException`. The ClickHouse builder also forces all join filter hook conditions to WHERE placement, since ClickHouse does not support subqueries in JOIN ON.

### Feature Matrix

Unsupported features are not on the class — consumers type-hint the interface to check capability (e.g., `if ($builder instanceof Spatial)`).

| Feature | Builder | SQL | MySQL | MariaDB | PostgreSQL | SQLite | ClickHouse |
|---------|:-------:|:---:|:-----:|:-------:|:----------:|:------:|:----------:|
| Selects, Filters, Aggregates, Joins, Unions, CTEs, Inserts, Updates, Deletes, Hooks | x | | | | | | |
| Windows | x | | | | | | |
| Locking, Transactions, Upsert | | x | | | | | |
| Spatial, Full-Text Search | | x | | | | | |
| Conditional Aggregates | | | x | x | x | x | x |
| JSON | | | x | x | x | x | |
| Hints | | | x | x | | | x |
| Lateral Joins | | | x | x | x | | |
| Full Outer Joins | | | | | x | | x |
| Table Sampling | | | | | x | | x |
| Merge | | | | | x | | |
| Returning | | | | | x | | |
| Vector Search | | | | | x | | |
| PREWHERE, FINAL, SAMPLE | | | | | | | x |

## Schema Builder

The schema builder generates DDL statements for table creation, alteration, indexes, views, and more.

```php
use Utopia\Query\Schema\MySQL as Schema;
// or: PostgreSQL, ClickHouse, SQLite
```

### Creating Tables

```php
$schema = new Schema();

$result = $schema->create('users', function ($table) {
    $table->id();
    $table->string('name', 255);
    $table->string('email', 255)->unique();
    $table->integer('age')->nullable();
    $table->boolean('active')->default(true);
    $table->json('metadata');
    $table->timestamps();
});

$result->query; // CREATE TABLE `users` (...)
```

Use `createIfNotExists()` to add `IF NOT EXISTS`:

```php
$result = $schema->createIfNotExists('users', function ($table) {
    $table->id();
    $table->string('name', 255);
});
```

Available column types: `id`, `string`, `text`, `mediumText`, `longText`, `integer`, `bigInteger`, `float`, `boolean`, `datetime`, `timestamp`, `json`, `binary`, `enum`, `point`, `linestring`, `polygon`, `vector` (PostgreSQL only), `timestamps`.

Column modifiers: `nullable()`, `default($value)`, `unsigned()`, `unique()`, `primary()`, `autoIncrement()`, `after($column)`, `comment($text)`, `collation($collation)`.

### Altering Tables

```php
$result = $schema->alter('users', function ($table) {
    $table->string('phone', 20)->nullable();
    $table->modifyColumn('name', 'string', 500);
    $table->renameColumn('email', 'email_address');
    $table->dropColumn('legacy_field');
});
```

### Indexes

```php
$result = $schema->createIndex('users', 'idx_email', ['email'], unique: true);
$result = $schema->dropIndex('users', 'idx_email');
```

PostgreSQL supports index methods, operator classes, and concurrent creation:

```php
$schema = new \Utopia\Query\Schema\PostgreSQL();

// GIN trigram index
$result = $schema->createIndex('users', 'idx_name_trgm', ['name'],
    method: 'gin', operatorClass: 'gin_trgm_ops');

// HNSW vector index
$result = $schema->createIndex('documents', 'idx_embedding', ['embedding'],
    method: 'hnsw', operatorClass: 'vector_cosine_ops');

// Concurrent index creation (non-blocking)
$result = $schema->createIndex('users', 'idx_email', ['email'], concurrently: true);

// Concurrent index drop
$result = $schema->dropIndexConcurrently('idx_email');
```

### Foreign Keys

```php
use Utopia\Query\Schema\ForeignKeyAction;

$result = $schema->addForeignKey('orders', 'fk_user', 'user_id',
    'users', 'id', onDelete: ForeignKeyAction::Cascade);

$result = $schema->dropForeignKey('orders', 'fk_user');
```

Available actions: `ForeignKeyAction::Cascade`, `SetNull`, `SetDefault`, `Restrict`, `NoAction`.

### Partitions

Available on MySQL, PostgreSQL, and ClickHouse:

```php
// Define partition strategy in table creation
$result = $schema->create('events', function ($table) {
    $table->id();
    $table->datetime('created_at');
    $table->partitionByRange('created_at');
});

// Create a child partition (MySQL, PostgreSQL)
$result = $schema->createPartition('events', 'events_2024', "VALUES LESS THAN ('2025-01-01')");

// Drop a partition
$result = $schema->dropPartition('events', 'events_2024');
```

Partition strategies: `partitionByRange()`, `partitionByList()`, `partitionByHash()`.

### Comments

Table and column comments are available via the `TableComments` and `ColumnComments` interfaces:

```php
// Table comments (MySQL, PostgreSQL, ClickHouse)
$result = $schema->commentOnTable('users', 'Main user accounts table');

// Column comments (PostgreSQL, ClickHouse)
$result = $schema->commentOnColumn('users', 'email', 'Primary contact email');
```

### Views

```php
$query = (new Builder())->from('users')->filter([Query::equal('active', [true])]);

$result = $schema->createView('active_users', $query);
$result = $schema->createOrReplaceView('active_users', $query);
$result = $schema->dropView('active_users');
```

### Procedures and Triggers

```php
use Utopia\Query\Schema\ParameterDirection;
use Utopia\Query\Schema\TriggerTiming;
use Utopia\Query\Schema\TriggerEvent;

// Procedure
$result = $schema->createProcedure('update_stats', [
    [ParameterDirection::In, 'user_id', 'INT'],
], 'UPDATE stats SET count = count + 1 WHERE id = user_id;');

// Trigger
$result = $schema->createTrigger('before_insert_users', 'users',
    TriggerTiming::Before, TriggerEvent::Insert,
    'SET NEW.created_at = NOW();');
```

### PostgreSQL Schema Extensions

```php
$schema = new \Utopia\Query\Schema\PostgreSQL();

// Extensions (e.g., pgvector, pg_trgm)
$result = $schema->createExtension('vector');
// CREATE EXTENSION IF NOT EXISTS "vector"

// Procedures → CREATE FUNCTION ... LANGUAGE plpgsql
$result = $schema->createProcedure('increment', [
    [ParameterDirection::In, 'p_id', 'INTEGER'],
], '
BEGIN
    UPDATE counters SET value = value + 1 WHERE id = p_id;
END;
');

// Custom types
$result = $schema->createType('status_type', ['active', 'inactive', 'banned']);
$result = $schema->dropType('status_type');

// Sequences
$result = $schema->createSequence('order_seq', start: 1000, incrementBy: 1);
$result = $schema->dropSequence('order_seq');
$result = $schema->nextVal('order_seq');

// Collations
$result = $schema->createCollation('custom_collation', ['locale' => 'en-US-u-ks-level2']);

// Alter column type with optional USING expression
$result = $schema->alterColumnType('users', 'age', 'BIGINT', using: '"age"::BIGINT');

// DROP CONSTRAINT instead of DROP FOREIGN KEY
$result = $schema->dropForeignKey('orders', 'fk_user');
// ALTER TABLE "orders" DROP CONSTRAINT "fk_user"

// DROP INDEX without table name
$result = $schema->dropIndex('orders', 'idx_status');
// DROP INDEX "idx_status"
```

Type differences from MySQL: `INTEGER` (not `INT`), `DOUBLE PRECISION` (not `DOUBLE`), `BOOLEAN` (not `TINYINT(1)`), `JSONB` (not `JSON`), `BYTEA` (not `BLOB`), `VECTOR(n)` for pgvector, `GEOMETRY(type, srid)` for PostGIS. Enums use `TEXT CHECK (col IN (...))`. Auto-increment uses `GENERATED BY DEFAULT AS IDENTITY`.

### ClickHouse Schema

```php
$schema = new \Utopia\Query\Schema\ClickHouse();

$result = $schema->create('events', function ($table) {
    $table->string('event_id', 36)->primary();
    $table->string('event_type', 50);
    $table->integer('count');
    $table->datetime('created_at');
});

// CREATE TABLE `events` (...) ENGINE = MergeTree() ORDER BY (...)
```

ClickHouse uses `Nullable(type)` wrapping for nullable columns, `Enum8(...)` for enums, `Tuple(Float64, Float64)` for points, and `TYPE minmax GRANULARITY 3` for indexes. Foreign keys, stored procedures, and triggers throw `UnsupportedException`.

Supports `TableComments`, `ColumnComments`, and `DropPartition` interfaces.

### SQLite Schema

```php
$schema = new \Utopia\Query\Schema\SQLite();
```

SQLite uses simplified type mappings: `INTEGER` for booleans, `TEXT` for datetimes/JSON, `REAL` for floats, `BLOB` for binary. Auto-increment uses `AUTOINCREMENT`. Vector and spatial types are not supported. Foreign keys, stored procedures, and triggers throw `UnsupportedException`.

## Compiler Interface

The `Compiler` interface lets you build custom backends. Each `Query` dispatches to the correct compiler method via `$query->compile($compiler)`:

```php
use Utopia\Query\Compiler;
use Utopia\Query\Query;
use Utopia\Query\Method;

class MyCompiler implements Compiler
{
    public function compileFilter(Query $query): string { /* ... */ }
    public function compileOrder(Query $query): string { /* ... */ }
    public function compileLimit(Query $query): string { /* ... */ }
    public function compileOffset(Query $query): string { /* ... */ }
    public function compileSelect(Query $query): string { /* ... */ }
    public function compileCursor(Query $query): string { /* ... */ }
    public function compileAggregate(Query $query): string { /* ... */ }
    public function compileGroupBy(Query $query): string { /* ... */ }
    public function compileJoin(Query $query): string { /* ... */ }
}
```

This is the pattern used by [utopia-php/database](https://github.com/utopia-php/database) — it implements `Compiler` for each supported database engine, keeping application code decoupled from storage backends.

## Contributing

All code contributions should go through a pull request and be approved by a core developer before being merged.

```bash
composer install       # Install dependencies
composer test          # Run tests
composer lint          # Check formatting
composer format        # Auto-format code
composer check         # Run static analysis (PHPStan level max)
```

**Integration tests** require Docker:

```bash
docker compose -f docker-compose.test.yml up -d   # Start MySQL, PostgreSQL, ClickHouse
composer test:integration                          # Run integration tests
docker compose -f docker-compose.test.yml down     # Stop containers
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
