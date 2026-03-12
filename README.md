# Utopia Query

[![Tests](https://github.com/utopia-php/query/actions/workflows/tests.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/tests.yml)
[![Linter](https://github.com/utopia-php/query/actions/workflows/linter.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/linter.yml)
[![Static Analysis](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml)

A PHP library for building type-safe, dialect-aware SQL queries and DDL statements. Provides a fluent builder API with parameterized output for MySQL, PostgreSQL, and ClickHouse, plus a serializable `Query` value object for passing query definitions between services.

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
  - [Conditional Building](#conditional-building)
  - [Debugging](#debugging)
  - [Hooks](#hooks)
- [Dialect-Specific Features](#dialect-specific-features)
  - [MySQL](#mysql)
  - [PostgreSQL](#postgresql)
  - [ClickHouse](#clickhouse)
  - [Feature Matrix](#feature-matrix)
- [Schema Builder](#schema-builder)
  - [Creating Tables](#creating-tables)
  - [Altering Tables](#altering-tables)
  - [Indexes](#indexes)
  - [Foreign Keys](#foreign-keys)
  - [Views](#views)
  - [Procedures and Triggers](#procedures-and-triggers)
  - [PostgreSQL Schema Extensions](#postgresql-schema-extensions)
  - [ClickHouse Schema](#clickhouse-schema)
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

The builder generates parameterized SQL from the fluent API. Every `build()`, `insert()`, `update()`, and `delete()` call returns a `BuildResult` with `->query` (the SQL string) and `->bindings` (the parameter array).

Three dialect implementations are provided:

- `Utopia\Query\Builder\MySQL` — MySQL/MariaDB
- `Utopia\Query\Builder\PostgreSQL` — PostgreSQL
- `Utopia\Query\Builder\ClickHouse` — ClickHouse

MySQL and PostgreSQL extend `Builder\SQL` which adds locking, transactions, and upsert. ClickHouse extends `Builder` directly with its own `ALTER TABLE` mutation syntax.

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

### Joins

```php
$result = (new Builder())
    ->from('users')
    ->join('orders', 'users.id', 'orders.user_id')
    ->leftJoin('profiles', 'users.id', 'profiles.user_id')
    ->crossJoin('colors')
    ->build();

// SELECT * FROM `users`
//   JOIN `orders` ON `users`.`id` = `orders`.`user_id`
//   LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id`
//   CROSS JOIN `colors`
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

Use `withRecursive()` for recursive CTEs.

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

Available on MySQL and PostgreSQL builders (`Builder\SQL` subclasses):

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

### Locking

Available on MySQL and PostgreSQL builders:

```php
$result = (new Builder())
    ->from('accounts')
    ->filter([Query::equal('id', [1])])
    ->forUpdate()
    ->build();

// SELECT * FROM `accounts` WHERE `id` IN (?) FOR UPDATE
```

Also available: `forShare()`.

### Transactions

Available on MySQL and PostgreSQL builders:

```php
$builder = new Builder();

$builder->begin();            // BEGIN
$builder->savepoint('sp1');   // SAVEPOINT `sp1`
$builder->rollbackToSavepoint('sp1');
$builder->commit();           // COMMIT
$builder->rollback();         // ROLLBACK
```

### Conditional Building

`when()` applies a callback only when the condition is true:

```php
$result = (new Builder())
    ->from('users')
    ->when($filterActive, fn(Builder $b) => $b->filter([Query::equal('status', ['active'])]))
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
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Placement;

class ActiveJoinFilter implements JoinFilter
{
    public function filterJoin(string $table, string $joinType): ?JoinCondition
    {
        return new JoinCondition(
            new Condition('active = ?', [1]),
            $joinType === 'LEFT JOIN' ? Placement::On : Placement::Where,
        );
    }
}
```

Built-in `Tenant` and `Permission` hooks implement both `Filter` and `JoinFilter` — they automatically apply ON placement for LEFT/RIGHT joins and WHERE placement for INNER/CROSS joins.

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

// WHERE ST_Distance(ST_SRID(`location`, 4326), ST_GeomFromText(?, 4326), 'metre') < ?
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

**Full-text search** — `MATCH() AGAINST()`:

```php
$result = (new Builder())
    ->from('articles')
    ->filter([Query::search('content', 'hello world')])
    ->build();

// WHERE MATCH(`content`) AGAINST (?)
```

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
$result = (new Builder())
    ->from('documents')
    ->select(['title'])
    ->orderByVectorDistance('embedding', [0.1, 0.2, 0.3], 'cosine')
    ->limit(10)
    ->build();

// SELECT "title" FROM "documents" ORDER BY ("embedding" <=> ?::vector) ASC LIMIT ?
```

Metrics: `cosine` (`<=>`), `euclidean` (`<->`), `dot` (`<#>`).

**JSON operations** — uses native JSONB operators:

```php
$result = (new Builder())
    ->from('products')
    ->filterJsonContains('tags', 'sale')
    ->build();

// WHERE "tags" @> ?::jsonb
```

**Full-text search** — `to_tsvector() @@ plainto_tsquery()`:

```php
$result = (new Builder())
    ->from('articles')
    ->filter([Query::search('content', 'hello world')])
    ->build();

// WHERE to_tsvector("content") @@ plainto_tsquery(?)
```

**Regex** — uses PostgreSQL `~` operator instead of `REGEXP`.

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

| Feature | Builder | SQL | MySQL | PostgreSQL | ClickHouse |
|---------|:-------:|:---:|:-----:|:----------:|:----------:|
| Selects, Filters, Aggregates, Joins, Unions, CTEs, Inserts, Updates, Deletes, Hooks | x | | | | |
| Windows | x | | | | |
| Locking, Transactions, Upsert | | x | | | |
| Spatial | | | x | x | |
| Vector Search | | | | x | |
| JSON | | | x | x | |
| Hints | | | x | | x |
| PREWHERE, FINAL, SAMPLE | | | | | x |

## Schema Builder

The schema builder generates DDL statements for table creation, alteration, indexes, views, and more.

```php
use Utopia\Query\Schema\MySQL as Schema;
// or: PostgreSQL, ClickHouse
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

Available column types: `id`, `string`, `text`, `integer`, `bigInteger`, `float`, `boolean`, `datetime`, `timestamp`, `json`, `binary`, `enum`, `point`, `linestring`, `polygon`, `vector` (PostgreSQL only), `timestamps`.

Column modifiers: `nullable()`, `default($value)`, `unsigned()`, `unique()`, `primary()`, `autoIncrement()`, `after($column)`, `comment($text)`.

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

PostgreSQL supports index methods and operator classes:

```php
$schema = new \Utopia\Query\Schema\PostgreSQL();

// GIN trigram index
$result = $schema->createIndex('users', 'idx_name_trgm', ['name'],
    method: 'gin', operatorClass: 'gin_trgm_ops');

// HNSW vector index
$result = $schema->createIndex('documents', 'idx_embedding', ['embedding'],
    method: 'hnsw', operatorClass: 'vector_cosine_ops');
```

### Foreign Keys

```php
$result = $schema->addForeignKey('orders', 'fk_user', 'user_id',
    'users', 'id', onDelete: 'CASCADE');

$result = $schema->dropForeignKey('orders', 'fk_user');
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
// MySQL
$result = $schema->createProcedure('update_stats', ['IN user_id INT'], 'UPDATE stats SET count = count + 1 WHERE id = user_id;');

// Trigger
$result = $schema->createTrigger('before_insert_users', 'users', 'BEFORE', 'INSERT', 'SET NEW.created_at = NOW();');
```

### PostgreSQL Schema Extensions

```php
$schema = new \Utopia\Query\Schema\PostgreSQL();

// Extensions (e.g., pgvector, pg_trgm)
$result = $schema->createExtension('vector');
// CREATE EXTENSION IF NOT EXISTS "vector"

// Procedures → CREATE FUNCTION ... LANGUAGE plpgsql
$result = $schema->createProcedure('increment', ['p_id INTEGER'], '
BEGIN
    UPDATE counters SET value = value + 1 WHERE id = p_id;
END;
');

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

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
