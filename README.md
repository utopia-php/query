# Utopia Query

[![Tests](https://github.com/utopia-php/query/actions/workflows/tests.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/tests.yml)
[![Integration Tests](https://github.com/utopia-php/query/actions/workflows/integration.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/integration.yml)
[![Linter](https://github.com/utopia-php/query/actions/workflows/linter.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/linter.yml)
[![Static Analysis](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/utopia-php/query/actions/workflows/static-analysis.yml)

A PHP library for building type-safe, dialect-aware queries and DDL statements. Provides a fluent builder API with parameterized output for MySQL, MariaDB, PostgreSQL, SQLite, ClickHouse, and MongoDB, plus wire protocol parsers and a serializable `Query` value object for passing query definitions between services.

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
  - [Raw and Column Predicates](#raw-and-column-predicates)
  - [Aggregations](#aggregations)
  - [Statistical Aggregates](#statistical-aggregates)
  - [Bitwise Aggregates](#bitwise-aggregates)
  - [Conditional Aggregates](#conditional-aggregates)
  - [String Aggregates](#string-aggregates)
  - [Group By Modifiers](#group-by-modifiers)
  - [Sequences](#sequences)
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
  - [MongoDB](#mongodb)
  - [Feature Matrix](#feature-matrix)
- [Schema Builder](#schema-builder)
  - [Creating Tables](#creating-tables)
  - [Altering Tables](#altering-tables)
  - [CHECK Constraints](#check-constraints)
  - [Generated Columns](#generated-columns)
  - [Composite Primary Keys](#composite-primary-keys)
  - [Indexes](#indexes)
  - [Foreign Keys](#foreign-keys)
  - [Partitions](#partitions)
  - [Comments](#comments)
  - [Views](#views)
  - [Procedures and Triggers](#procedures-and-triggers)
  - [PostgreSQL Schema Extensions](#postgresql-schema-extensions)
  - [ClickHouse Schema](#clickhouse-schema)
  - [SQLite Schema](#sqlite-schema)
  - [MongoDB Schema](#mongodb-schema)
- [Wire Protocol Parsers](#wire-protocol-parsers)
  - [SQL Parser](#sql-parser)
  - [MySQL Parser](#mysql-parser)
  - [PostgreSQL Parser](#postgresql-parser)
  - [MongoDB Parser](#mongodb-parser)
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

// Substring matching (LIKE '%value%')
Query::containsString('title', ['urgent', 'important']);

// Array / containment (for array or relationship attributes)
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

> **Note:** `Query::contains()` is deprecated — use `Query::containsString()` for string substring matching or `Query::containsAny()` for array/relationship attributes.

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
// Group queries by type — returns a ParsedQuery value object
$parsed = Query::groupByType($queries);
// $parsed->filters, $parsed->selections, $parsed->aggregations, $parsed->groupBy,
// $parsed->having, $parsed->joins, $parsed->unions, $parsed->limit, $parsed->offset,
// $parsed->cursor, $parsed->cursorDirection, $parsed->distinct

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

The builder generates parameterized queries from the fluent API. Every `build()`, `insert()`, `update()`, and `delete()` call returns a `Statement` with `->query` (the query string), `->bindings` (the parameter array), and `->readOnly` (whether the query is read-only).

Six dialect implementations are provided:

- `Utopia\Query\Builder\MySQL` — MySQL
- `Utopia\Query\Builder\MariaDB` — MariaDB (extends MySQL with `RETURNING`, sequences, and dialect-specific spatial handling)
- `Utopia\Query\Builder\PostgreSQL` — PostgreSQL
- `Utopia\Query\Builder\SQLite` — SQLite
- `Utopia\Query\Builder\ClickHouse` — ClickHouse
- `Utopia\Query\Builder\MongoDB` — MongoDB (generates JSON operation documents)

MySQL, MariaDB, PostgreSQL, and SQLite extend `Builder\SQL` which adds locking, transactions, upsert, spatial queries, and full-text search. ClickHouse and MongoDB extend `Builder` directly with their own dialect-specific syntax.

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

### Raw and Column Predicates

In addition to the typed `filter()` API, two escape hatches are available on every SQL dialect (MySQL, MariaDB, PostgreSQL, SQLite, ClickHouse). Both throw `ValidationException` on the MongoDB builder.

**`whereRaw()`** — emit a raw SQL fragment with its own bindings. The caller owns the SQL:

```php
use Utopia\Query\Builder\MySQL as Builder;

$result = (new Builder())
    ->from('users')
    ->whereRaw('LENGTH(`bio`) > ?', [100])
    ->build();

// SELECT * FROM `users` WHERE LENGTH(`bio`) > ?
```

**`whereColumn()`** — typed column-to-column predicate with quoting. The operator is validated against `['=', '!=', '<>', '<', '>', '<=', '>=']`:

```php
// Correlated subquery for a lateral join
$topOrder = (new Builder())
    ->from('orders')
    ->select(['product', 'amount'])
    ->whereColumn('orders.user_id', '=', 'u.id')
    ->sortDesc('amount')
    ->limit(1);

$result = (new Builder())
    ->from('users', 'u')
    ->joinLateral($topOrder, 'top_order')
    ->build();
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

### Statistical Aggregates

Available on MySQL, PostgreSQL, SQLite, and ClickHouse via the `StatisticalAggregates` interface:

```php
use Utopia\Query\Builder\PostgreSQL as Builder;

$result = (new Builder())
    ->from('measurements')
    ->stddev('value', 'std_dev')
    ->stddevPop('value', 'pop_std_dev')
    ->stddevSamp('value', 'samp_std_dev')
    ->variance('value', 'var')
    ->varPop('value', 'pop_var')
    ->varSamp('value', 'samp_var')
    ->build();
```

### Bitwise Aggregates

Available on MySQL, PostgreSQL, SQLite, and ClickHouse via the `BitwiseAggregates` interface:

```php
$result = (new Builder())
    ->from('permissions')
    ->bitAnd('flags', 'combined_and')
    ->bitOr('flags', 'combined_or')
    ->bitXor('flags', 'combined_xor')
    ->build();
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

### String Aggregates

Available on MySQL, PostgreSQL, and ClickHouse via the `StringAggregates` interface:

```php
use Utopia\Query\Builder\MySQL as Builder;

// Concatenate values into a string
$result = (new Builder())
    ->from('tags')
    ->select(['post_id'])
    ->groupConcat('name', ', ', 'tag_list', orderBy: ['name'])
    ->groupBy(['post_id'])
    ->build();

// MySQL:      GROUP_CONCAT(`name` ORDER BY `name` ASC SEPARATOR ', ')
// PostgreSQL: STRING_AGG("name", ', ' ORDER BY "name" ASC)
// ClickHouse: arrayStringConcat(groupArray(`name`), ', ')

// JSON array aggregation
$result = (new Builder())
    ->from('items')
    ->jsonArrayAgg('name', 'names_json')
    ->build();

// JSON object aggregation from key/value pairs
$result = (new Builder())
    ->from('settings')
    ->jsonObjectAgg('key', 'value', 'settings_json')
    ->build();
```

### Group By Modifiers

Available on MySQL, PostgreSQL, and ClickHouse via the `GroupByModifiers` interface:

```php
use Utopia\Query\Builder\MySQL as Builder;

// WITH ROLLUP — adds subtotal and grand total rows
$result = (new Builder())
    ->from('sales')
    ->select(['region', 'product'])
    ->sum('amount', 'total')
    ->groupBy(['region', 'product'])
    ->withRollup()
    ->build();

// WITH CUBE — adds subtotals for all dimension combinations (MySQL 8.0.1+, PostgreSQL, ClickHouse)
$result = (new Builder())
    ->from('sales')
    ->select(['region', 'product'])
    ->sum('amount', 'total')
    ->groupBy(['region', 'product'])
    ->withCube()
    ->build();

// WITH TOTALS (ClickHouse) — adds a totals row
use Utopia\Query\Builder\ClickHouse as ChBuilder;

$result = (new ChBuilder())
    ->from('events')
    ->select(['event_type'])
    ->count('*', 'cnt')
    ->groupBy(['event_type'])
    ->withTotals()
    ->build();
```

### Sequences

Available on MariaDB and PostgreSQL via the `Sequences` interface. Emits `NEXTVAL()` and `CURRVAL()` as select expressions:

```php
use Utopia\Query\Builder\PostgreSQL as Builder;

// Advance the sequence and return the next value
$result = (new Builder())
    ->nextVal('order_seq', 'next_id')
    ->build();

// PostgreSQL: SELECT nextval('order_seq') AS "next_id"
// MariaDB:    SELECT NEXTVAL(`order_seq`) AS `next_id`

// Return the session-local current value
$result = (new Builder())
    ->currVal('order_seq', 'current_id')
    ->build();
```

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
use Utopia\Query\Builder\PostgreSQL as Builder;

$result = (new Builder())
    ->from('left_table')
    ->fullOuterJoin('right_table', 'left_table.id', 'right_table.id')
    ->build();

// SELECT * FROM "left_table" FULL OUTER JOIN "right_table" ON "left_table"."id" = "right_table"."id"
```

**Lateral joins** (MySQL, MariaDB, PostgreSQL):

```php
$sub = (new Builder())
    ->from('orders')
    ->whereColumn('orders.user_id', '=', 'users.id')
    ->limit(3);

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

Build a CASE expression with `Utopia\Query\Builder\Case\Expression`, then pass it to `selectCase()` or `setCase()`. All columns are quoted by the dialect, and all values are bound as parameters:

```php
use Utopia\Query\Builder\Case\Expression as CaseExpression;
use Utopia\Query\Builder\Case\Operator;

$case = (new CaseExpression())
    ->when('amount', Operator::GreaterThan, 1000, 'high')
    ->when('amount', Operator::GreaterThan, 100, 'medium')
    ->else('low')
    ->alias('priority');

$result = (new Builder())
    ->from('orders')
    ->select(['id'])
    ->selectCase($case)
    ->build();

// SELECT `id`, CASE WHEN `amount` > ? THEN ? WHEN `amount` > ? THEN ? ELSE ? END AS `priority`
//   FROM `orders`
```

Supported WHEN shapes:

- `when(string $column, Operator $operator, mixed $value, mixed $then)` — comparison. The operator is a closed enum of the six comparisons: `Operator::Equal`, `Operator::NotEqual`, `Operator::LessThan`, `Operator::LessThanEqual`, `Operator::GreaterThan`, `Operator::GreaterThanEqual`.
- `whenNull(string $column, mixed $then)` and `whenNotNull(string $column, mixed $then)`.
- `whenIn(string $column, array $values, mixed $then)`.
- `whenRaw(string $condition, mixed $then, array $conditionBindings = [])` — escape hatch for complex predicates. The caller owns the SQL fragment; the `$then` value is still bound.

### Inserts

`set()` takes an associative row array. Calling it multiple times appends rows for a batch insert:

```php
// Single row
$result = (new Builder())
    ->into('users')
    ->set(['name' => 'Alice', 'email' => 'alice@example.com'])
    ->insert();

// Batch insert — one set() call per row
$result = (new Builder())
    ->into('users')
    ->set(['name' => 'Alice', 'email' => 'alice@example.com'])
    ->set(['name' => 'Bob', 'email' => 'bob@example.com'])
    ->insert();

// INSERT ... SELECT
$source = (new Builder())->from('archived_users')->filter([Query::equal('status', ['active'])]);

$result = (new Builder())
    ->into('users')
    ->fromSelect(['name', 'email'], $source)
    ->insertSelect();
```

### Updates

```php
$result = (new Builder())
    ->from('users')
    ->set(['status' => 'inactive'])
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

`onConflict()` takes the conflict key columns and the columns to update on conflict:

```php
// MySQL — ON DUPLICATE KEY UPDATE
$result = (new Builder())
    ->into('counters')
    ->set(['key' => 'visits', 'value' => 1])
    ->onConflict(['key'], ['value'])
    ->upsert();

// PostgreSQL — ON CONFLICT (...) DO UPDATE SET
use Utopia\Query\Builder\PostgreSQL as PgBuilder;

$result = (new PgBuilder())
    ->into('counters')
    ->set(['key' => 'visits', 'value' => 1])
    ->onConflict(['key'], ['value'])
    ->upsert();
```

**Insert or ignore** — skip rows that conflict instead of updating:

```php
$result = (new Builder())
    ->into('counters')
    ->set(['key' => 'visits', 'value' => 1])
    ->onConflict(['key'], [])
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
    ->fromSelect(['key', 'value'], $source)
    ->onConflict(['key'], ['value'])
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
use Utopia\Query\Builder\MySQL as Builder;

// Basic explain
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('status', ['active'])])
    ->explain();

// MySQL — with format
$result = (new Builder())
    ->from('users')
    ->explain(analyze: true, format: 'JSON');

// PostgreSQL — with analyze, verbose, buffers, format
use Utopia\Query\Builder\PostgreSQL as PgBuilder;

$result = (new PgBuilder())
    ->from('users')
    ->explain(analyze: true, verbose: true, buffers: true, format: 'JSON');
```

### Conditional Building

`when()` applies a callback only when the condition is true:

```php
$result = (new Builder())
    ->from('users')
    ->when($filterActive, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
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
use Utopia\Query\Builder\Statement;

$result = (new Builder())
    ->from('users')
    ->beforeBuild(fn (Builder $b) => $b->filter([Query::isNotNull('email')]))
    ->afterBuild(fn (Statement $s) => new Statement("/* traced */ {$s->query}", $s->bindings, $s->readOnly))
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

// Mutations (in UPDATE) — combine with set() or setRaw() as needed
$result = (new Builder())
    ->from('products')
    ->filter([Query::equal('id', [1])])
    ->setJsonAppend('tags', ['new-tag'])
    ->update();

// Set a JSON path to a typed value — JSON_SET on MySQL, jsonb_set on PostgreSQL, json_set on SQLite
$result = (new Builder())
    ->from('products')
    ->filter([Query::equal('id', [1])])
    ->setJsonPath('metadata', '$.level', 42)
    ->update();
```

JSON mutation methods: `setJsonAppend`, `setJsonPrepend`, `setJsonInsert`, `setJsonRemove`, `setJsonIntersect`, `setJsonDiff`, `setJsonUnique`, `setJsonPath`.

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
    ->set(['status' => 'premium'])
    ->updateJoin('orders', 'users.id', 'orders.user_id')
    ->filter([Query::greaterThan('orders.total', 1000)])
    ->update();
```

**DELETE with JOIN:**

```php
$result = (new Builder())
    ->from('users', 'u')
    ->deleteJoin('u', 'orders', 'u.id', 'orders.user_id')
    ->filter([Query::equal('orders.status', ['cancelled'])])
    ->delete();

// DELETE `u` FROM `users` AS `u` JOIN `orders` ON `u`.`id` = `orders`.`user_id`
//   WHERE `orders`.`status` IN (?)
```

### MariaDB

```php
use Utopia\Query\Builder\MariaDB as Builder;
```

Extends MySQL with MariaDB-specific features and spatial handling:

- Uses `ST_DISTANCE_SPHERE()` for meter-based distance calculations.
- Uses `ST_GeomFromText()` without the `axis-order` parameter.
- Validates that distance-in-meters only works between POINT types.

All MySQL features (JSON, hints, lateral joins, UPDATE/DELETE JOIN, etc.) are inherited.

**`RETURNING`** (MariaDB 10.5+) — get affected rows back from `INSERT`, `UPDATE`, or `DELETE`:

```php
$result = (new Builder())
    ->into('users')
    ->set(['name' => 'Alice'])
    ->returning(['id', 'created_at'])
    ->insert();

// INSERT INTO `users` (`name`) VALUES (?) RETURNING `id`, `created_at`
```

`returning()` cannot be combined with `upsert()` — MariaDB does not support `RETURNING` with `ON DUPLICATE KEY UPDATE`. Doing so throws `ValidationException`. Clear the returning columns with `returning([])` first, or issue a separate `update()` statement.

**Sequences** — native sequence support via `nextVal()` and `currVal()`:

```php
$result = (new Builder())
    ->nextVal('order_seq', 'next_id')
    ->build();

// SELECT NEXTVAL(`order_seq`) AS `next_id`
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

// setJsonPath compiles to jsonb_set with a translated text-array path
$result = (new Builder())
    ->from('products')
    ->filter([Query::equal('id', [1])])
    ->setJsonPath('data', '$.name', 'NewValue')
    ->update();

// UPDATE "products" SET "data" = jsonb_set("data", '{name}', to_jsonb(?::text), true) WHERE "id" IN (?)
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
    ->set(['name' => 'Alice'])
    ->returning(['id', 'created_at'])
    ->insert();

// INSERT INTO "users" ("name") VALUES (?) RETURNING "id", "created_at"
```

**DISTINCT ON** — select the first row per group:

```php
$result = (new Builder())
    ->from('events')
    ->distinctOn(['user_id'])
    ->select(['user_id', 'event_type', 'created_at'])
    ->sortDesc('created_at')
    ->build();

// SELECT DISTINCT ON ("user_id") "user_id", "event_type", "created_at"
//   FROM "events" ORDER BY "created_at" DESC
```

**Aggregate FILTER** — per-aggregate WHERE clause (SQL standard):

```php
$result = (new Builder())
    ->from('orders')
    ->selectAggregateFilter('COUNT(*)', 'status = ?', 'active_count', ['active'])
    ->selectAggregateFilter('SUM("amount")', 'status = ?', 'active_total', ['active'])
    ->build();

// SELECT COUNT(*) FILTER (WHERE status = ?) AS "active_count",
//   SUM("amount") FILTER (WHERE status = ?) AS "active_total"
//   FROM "orders"
```

**Ordered-set aggregates:**

```php
$result = (new Builder())
    ->from('salaries')
    ->arrayAgg('name', 'all_names')
    ->percentileCont(0.5, 'salary', 'median_salary')
    ->percentileDisc(0.9, 'salary', 'p90_salary')
    ->mode('city', 'top_city')
    ->boolAnd('is_active', 'all_active')
    ->boolOr('is_admin', 'any_admin')
    ->every('is_verified', 'all_verified')
    ->build();

// mode() emits `mode() WITHIN GROUP (ORDER BY "city") AS "top_city"` — returns the most
// frequent value in the column (ties broken arbitrarily).
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
    ->set(['status' => 'premium'])
    ->updateFrom('orders', 'o')
    ->updateFromWhere('"users"."id" = "o"."user_id"')
    ->update();

// DELETE ... USING — PostgreSQL semantics differ from MySQL's deleteJoin
$result = (new Builder())
    ->from('users')
    ->deleteUsing('old_users', '"users"."id" = "old_users"."id"')
    ->delete();
```

**Sequences** — native sequence support via `nextVal()` / `currVal()` (see [Sequences](#sequences)).

**Recursive CTEs** — both `withRecursive()` and `withRecursiveSeedStep()` compile to standard `WITH RECURSIVE` syntax.

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

- JSON support via `json_each()` and `json_extract()`. `setJsonPath` compiles to `json_set`.
- Conditional aggregates using `CASE WHEN` syntax.
- `INSERT OR IGNORE` for `insertOrIgnore()`.
- Regex and full-text search throw `UnsupportedException`.
- Spatial queries throw `UnsupportedException`.

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

**LIMIT BY** — limit rows per group:

```php
$result = (new Builder())
    ->from('events')
    ->select(['user_id', 'event_type'])
    ->limitBy(3, ['user_id'])
    ->build();

// SELECT `user_id`, `event_type` FROM `events` LIMIT 3 BY `user_id`
```

**ARRAY JOIN** — unnest array columns into rows:

```php
$result = (new Builder())
    ->from('events')
    ->select(['name'])
    ->arrayJoin('tags', 'tag')
    ->build();

// SELECT `name`, `tags` AS `tag` FROM `events` ARRAY JOIN `tags` AS `tag`

// LEFT variant preserves rows with empty arrays
$result = (new Builder())
    ->from('events')
    ->leftArrayJoin('tags', 'tag')
    ->build();
```

**ASOF JOIN** — join on the closest matching row (time-series). Requires one or more equi-join pairs plus exactly one inequality condition:

```php
use Utopia\Query\Builder\ClickHouse\AsofOperator;

// For each trade, find the most recent quote with the same symbol
$result = (new Builder())
    ->from('trades', 't')
    ->select(['t.symbol', 't.ts', 't.price', 'q.bid'])
    ->asofJoin(
        table: 'quotes',
        equiPairs: ['t.symbol' => 'q.symbol'],
        leftInequality: 't.ts',
        operator: AsofOperator::GreaterThanEqual,
        rightInequality: 'q.ts',
        alias: 'q',
    )
    ->sortAsc('t.ts')
    ->build();

// SELECT `t`.`symbol`, `t`.`ts`, `t`.`price`, `q`.`bid` FROM `trades` AS `t`
//   ASOF JOIN `quotes` AS `q`
//     ON `t`.`symbol` = `q`.`symbol` AND `t`.`ts` >= `q`.`ts`
//   ORDER BY `t`.`ts` ASC
```

`asofLeftJoin()` takes the same arguments and emits `ASOF LEFT JOIN`, preserving left rows with no match. `AsofOperator` variants: `LessThan`, `LessThanEqual`, `GreaterThan`, `GreaterThanEqual`.

**ORDER BY ... WITH FILL** — fill gaps in ordered results:

```php
$result = (new Builder())
    ->from('daily_stats')
    ->select(['date', 'count'])
    ->orderWithFill('date', 'ASC', from: '2024-01-01', to: '2024-01-31', step: 1)
    ->build();

// SELECT `date`, `count` FROM `daily_stats` ORDER BY `date` ASC WITH FILL FROM '2024-01-01' TO '2024-01-31' STEP 1
```

**Approximate aggregates** — ClickHouse-native probabilistic functions:

```php
$result = (new Builder())
    ->from('events')
    ->quantile(0.95, 'response_time', 'p95')
    ->quantiles([0.25, 0.5, 0.75, 0.95], 'response_time', 'quartiles')
    ->quantileExact(0.99, 'response_time', 'p99')
    ->median('response_time', 'med')
    ->uniq('user_id', 'approx_users')
    ->uniqExact('user_id', 'exact_users')
    ->uniqCombined('user_id', 'combined_users')
    ->build();

// quantiles(0.25, 0.5, 0.75, 0.95)(`response_time`) AS `quartiles`
```

`quantiles()` computes multiple quantile levels in a single pass. Levels are validated to be in `[0, 1]`; the array must be non-empty.

Additional approximate aggregates: `argMin()`, `argMax()`, `topK()`, `topKWeighted()`, `anyValue()`, `anyLastValue()`, `groupUniqArray()`, `groupArrayMovingAvg()`, `groupArrayMovingSum()`.

**String matching** — uses native ClickHouse functions instead of LIKE:

```php
// startsWith/endsWith → native functions
Query::startsWith('name', 'Al');                 // startsWith(`name`, ?)
Query::endsWith('file', '.pdf');                 // endsWith(`file`, ?)

// containsString → position()
Query::containsString('tags', ['php']);          // position(`tags`, ?) > 0
```

**Regex** — uses `match()` function instead of `REGEXP`.

**UPDATE/DELETE** — compiles to `ALTER TABLE ... UPDATE/DELETE` with mandatory WHERE:

```php
$result = (new Builder())
    ->from('events')
    ->set(['status' => 'archived'])
    ->filter([Query::lessThan('created_at', '2024-01-01')])
    ->update();

// ALTER TABLE `events` UPDATE `status` = ? WHERE `created_at` < ?
```

> **Note:** Full-text search (`Query::search()`) is not supported in ClickHouse and throws `UnsupportedException`. The ClickHouse builder also forces all join filter hook conditions to WHERE placement, since ClickHouse does not support subqueries in JOIN ON.

### MongoDB

```php
use Utopia\Query\Builder\MongoDB as Builder;
```

The MongoDB builder generates JSON operation documents instead of SQL. The `Statement->query` contains a JSON-encoded operation and `Statement->bindings` contains parameter values. `whereRaw()` and `whereColumn()` are not supported and throw `ValidationException`.

**Basic queries:**

```php
$result = (new Builder())
    ->from('users')
    ->filter([
        Query::equal('status', ['active']),
        Query::greaterThan('age', 18),
    ])
    ->sortAsc('name')
    ->limit(25)
    ->build();

// Generates a find operation with filter, sort, limit, and projection
```

**Array operations:**

```php
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('_id', ['user_1'])])
    ->push('tags', 'new-tag')
    ->update();

$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('_id', ['user_1'])])
    ->pull('tags', 'old-tag')
    ->addToSet('roles', 'editor')
    ->increment('login_count', 1)
    ->update();
```

**Field update operations:**

```php
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('_id', ['user_1'])])
    ->rename('old_field', 'new_field')
    ->multiply('score', 1.5)
    ->updateMin('low_score', 10)
    ->updateMax('high_score', 100)
    ->currentDate('last_modified')
    ->update();

// Array element removal
$result = (new Builder())
    ->from('lists')
    ->filter([Query::equal('_id', ['list_1'])])
    ->popFirst('items')   // Remove first element — $pop: -1
    ->popLast('queue')    // Remove last element — $pop: 1
    ->pullAll('tags', ['deprecated', 'old'])
    ->update();

// Remove fields entirely
$result = (new Builder())
    ->from('users')
    ->filter([Query::equal('_id', ['user_1'])])
    ->unsetFields('legacy_field', 'temp_data')
    ->update();
```

**Advanced array push** with position, slice, and sort modifiers:

```php
$result = (new Builder())
    ->from('feeds')
    ->filter([Query::equal('_id', ['feed_1'])])
    ->pushEach('items', [['score' => 5], ['score' => 3]], position: 0, slice: 10, sort: ['score' => -1])
    ->update();
```

**Conditional array updates** with array filters:

```php
$result = (new Builder())
    ->from('orders')
    ->filter([Query::equal('_id', ['order_1'])])
    ->arrayFilter('elem', ['elem.status' => 'pending'])
    ->set(['items.$[elem].status' => 'shipped'])
    ->update();
```

**Upsert:**

```php
$result = (new Builder())
    ->into('counters')
    ->set(['key' => 'visits', 'value' => 1])
    ->onConflict(['key'], ['value'])
    ->upsert();
```

**Pipeline aggregation stages:**

```php
// Bucket — group documents into fixed-size ranges
$result = (new Builder())
    ->from('sales')
    ->bucket('price', [0, 50, 100, 500], defaultBucket: 'other', output: ['count' => ['$sum' => 1]])
    ->build();

// BucketAuto — automatically determine bucket boundaries
$result = (new Builder())
    ->from('sales')
    ->bucketAuto('price', 5, output: ['count' => ['$sum' => 1]])
    ->build();

// Facet — run multiple aggregation pipelines in parallel
$byStatus = (new Builder())->from('orders')->groupBy(['status'])->count('*', 'count');
$byRegion = (new Builder())->from('orders')->groupBy(['region'])->sum('amount', 'total');

$result = (new Builder())
    ->from('orders')
    ->facet(['by_status' => $byStatus, 'by_region' => $byRegion])
    ->build();

// GraphLookup — recursive graph traversal
$result = (new Builder())
    ->from('employees')
    ->graphLookup(
        from: 'employees',
        startWith: '$manager_id',
        connectFromField: 'manager_id',
        connectToField: '_id',
        as: 'reporting_chain',
        maxDepth: 5,
        depthField: 'level',
    )
    ->build();

// Merge results into another collection
$result = (new Builder())
    ->from('daily_stats')
    ->mergeIntoCollection('monthly_stats', on: ['month'], whenMatched: ['$set' => ['total' => '$total']])
    ->build();

// Output to a new collection
$result = (new Builder())
    ->from('raw_data')
    ->outputToCollection('processed_data', database: 'analytics')
    ->build();

// Replace the root document
$result = (new Builder())
    ->from('orders')
    ->replaceRoot('$shipping_address')
    ->build();
```

**Atlas Search:**

```php
// Full-text search with Atlas Search
$result = (new Builder())
    ->from('articles')
    ->search(['text' => ['query' => 'mongodb', 'path' => 'content']], index: 'default')
    ->build();

// Search metadata (facet counts, etc.)
$result = (new Builder())
    ->from('articles')
    ->searchMeta(['facet' => ['facets' => ['categories' => ['type' => 'string', 'path' => 'category']]]], index: 'default')
    ->build();

// Atlas Vector Search
$result = (new Builder())
    ->from('documents')
    ->vectorSearch(
        path: 'embedding',
        queryVector: [0.1, 0.2, 0.3],
        numCandidates: 100,
        limit: 10,
        index: 'vector_index',
        filter: ['category' => 'tech'],
    )
    ->build();
```

**Table sampling:**

```php
$result = (new Builder())
    ->from('large_collection')
    ->tablesample(10.0)
    ->build();
```

**Full-text search** (non-Atlas):

```php
$result = (new Builder())
    ->from('articles')
    ->filterSearch('content', 'hello world')
    ->build();
```

### Feature Matrix

Unsupported features are not on the class — consumers type-hint the interface to check capability (e.g., `if ($builder instanceof Spatial)`).

| Feature | Builder | SQL | MySQL | MariaDB | PostgreSQL | SQLite | ClickHouse | MongoDB |
|---------|:-------:|:---:|:-----:|:-------:|:----------:|:------:|:----------:|:-------:|
| Selects, Filters, Aggregates, Joins, Unions, CTEs, Inserts, Updates, Deletes, Hooks | x | | | | | | | |
| Windows | x | | | | | | | |
| `whereRaw` / `whereColumn` | | x | | | | | x | |
| Locking, Transactions, Upsert | | x | | | | | | |
| Spatial, Full-Text Search | | x | | | | | | |
| Statistical Aggregates | | | x | x | x | x | x | |
| Bitwise Aggregates | | | x | x | x | x | x | |
| Conditional Aggregates | | | x | x | x | x | x | |
| JSON (incl. `setJsonPath`) | | | x | x | x | x | | |
| Hints | | | x | x | | | x | |
| Lateral Joins | | | x | x | x | | | |
| String Aggregates | | | x | x | x | | x | |
| Group By Modifiers | | | x | x | x | | x | |
| Sequences (`nextVal`/`currVal`) | | | | x | x | | | |
| `RETURNING` | | | | x | x | | | |
| Full Outer Joins | | | | | x | | x | |
| Table Sampling | | | | | x | | x | x |
| Merge | | | | | x | | | |
| Vector Search | | | | | x | | | |
| DISTINCT ON | | | | | x | | | |
| Aggregate FILTER | | | | | x | | | |
| Ordered-Set Aggregates (incl. `mode`) | | | | | x | | | |
| PREWHERE, FINAL, SAMPLE | | | | | | | x | |
| LIMIT BY | | | | | | | x | |
| ARRAY JOIN | | | | | | | x | |
| ASOF JOIN (typed operator) | | | | | | | x | |
| WITH FILL | | | | | | | x | |
| Approximate Aggregates (incl. `quantiles`) | | | | | | | x | |
| Upsert (Mongo-style) | | | | | | | | x |
| Full-Text Search (Mongo) | | | | | | | | x |
| Field Updates | | | | | | | | x |
| Array Push Modifiers | | | | | | | | x |
| Conditional Array Updates | | | | | | | | x |
| Pipeline Stages | | | | | | | | x |
| Atlas Search | | | | | | | | x |

## Schema Builder

The schema builder generates DDL statements for table creation, alteration, indexes, views, and more.

```php
use Utopia\Query\Schema\MySQL as Schema;
use Utopia\Query\Schema\Table;
// or: PostgreSQL, ClickHouse, SQLite, MongoDB
```

### Creating Tables

```php
$schema = new Schema();

$result = $schema->table('users')
    ->id()
    ->string('name', 255)
    ->string('email', 255)->unique()
    ->integer('age')->nullable()
    ->boolean('active')->default(true)
    ->json('metadata')
    ->timestamps()
    ->create();

$result->query; // CREATE TABLE `users` (...)
```

`Schema::table($name)` returns a fluent builder. Column-adding methods (`id`, `string`, `integer`, …) return a `Column` you can chain modifiers on; the column also exposes the table-level builder so you can keep chaining sibling columns or terminal calls without breaking the chain. Terminal methods (`create`, `createIfNotExists`, `alter`, `drop`, `dropIfExists`, `truncate`, `rename`) compile and return a `Statement`.

Use `createIfNotExists()` to add `IF NOT EXISTS`:

```php
$result = $schema->table('users')
    ->id()
    ->string('name', 255)
    ->createIfNotExists();
```

Available column types: `id`, `string`, `text`, `mediumText`, `longText`, `integer`, `bigInteger`, `serial`, `bigSerial`, `smallSerial`, `float`, `boolean`, `datetime`, `timestamp`, `json`, `binary`, `enum`, `point`, `linestring`, `polygon`, `vector` (PostgreSQL only), `timestamps`.

Column modifiers: `nullable()`, `default($value)`, `unsigned()`, `unique()`, `primary()`, `autoIncrement()`, `after($column)`, `comment($text)`, `collation($collation)`, `check($expression)`, `generatedAs($expression)` + `stored()` / `virtual()`, `ttl($expression)` (ClickHouse), `userType($name)` (PostgreSQL).

**SERIAL types** — auto-incrementing integers. PostgreSQL emits native `SERIAL` / `BIGSERIAL` / `SMALLSERIAL`; MySQL/MariaDB compile to `INT AUTO_INCREMENT` / `BIGINT AUTO_INCREMENT` / `SMALLINT AUTO_INCREMENT`; SQLite maps to `INTEGER`. ClickHouse and MongoDB throw `UnsupportedException`:

```php
$result = $schema->table('orders')
    ->serial('id')->primary()
    ->bigSerial('external_id')
    ->create();
```

### Altering Tables

```php
use Utopia\Query\Schema\ColumnType;

$result = $schema->table('users')
    ->string('phone', 20)->nullable()
    ->modifyColumn('name', ColumnType::String, 500)
    ->renameColumn('email', 'email_address')
    ->dropColumn('legacy_field')
    ->alter();
```

`addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null)` and `modifyColumn(...)` take the `ColumnType` enum directly. The `addIndex(...)` overload takes the `IndexType` enum.

### CHECK Constraints

Typed `CHECK` constraints are supported at both the table and column level on MySQL 8.0.16+, MariaDB, PostgreSQL, and SQLite. ClickHouse throws `UnsupportedException`.

```php
$result = $schema->table('people')
    ->id()
    ->integer('age')->check('>= 0')                           // column-level
    ->string('email', 255)
    ->check('age_range', '`age` >= 0 AND `age` < 150')        // table-level
    ->create();
```

Constraint names are validated as standard SQL identifiers; expressions are emitted verbatim and must come from trusted sources — never from untrusted input.

### Generated Columns

Generated columns compute their value from an expression. Both `STORED` and `VIRTUAL` are supported on MySQL, MariaDB, and SQLite. PostgreSQL supports only `STORED` (calling `virtual()` and compiling for PostgreSQL throws `UnsupportedException`). ClickHouse throws `UnsupportedException` for generated columns.

```php
$result = $schema->table('boxes')
    ->id()
    ->integer('width')
    ->integer('height')
    ->integer('area')
        ->generatedAs('`width` * `height`')
        ->stored()
    ->integer('half_area')
        ->generatedAs('(`width` * `height`) / 2')
        ->virtual()
    ->create();
```

### Composite Primary Keys

Declare a primary key across two or more columns with `Table::primary([...])`. Mixing a column-level `->primary()` with `Table::primary([...])` throws `ValidationException`. MongoDB throws `UnsupportedException`.

```php
$result = $schema->table('order_items')
    ->integer('order_id')
    ->integer('product_id')
    ->integer('quantity')
    ->primary(['order_id', 'product_id'])
    ->create();
```

### Indexes

```php
$result = $schema->createIndex('users', 'idx_email', ['email'], unique: true);
$result = $schema->dropIndex('users', 'idx_email');
```

PostgreSQL supports index methods, operator classes, and concurrent creation:

```php
use Utopia\Query\Schema\PostgreSQL as Schema;

$schema = new Schema();

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
$result = $schema->table('events')
    ->id()
    ->datetime('created_at')
    ->partitionByRange('created_at')
    ->create();

// Create a child partition (MySQL, PostgreSQL)
$result = $schema->createPartition('events', 'events_2024', "VALUES LESS THAN ('2025-01-01')");

// Drop a partition
$result = $schema->dropPartition('events', 'events_2024');
```

Partition strategies: `partitionByRange($expression)`, `partitionByList($expression)`, `partitionByHash($expression, ?int $partitions = null)`. The optional partition count on `partitionByHash()` emits `PARTITIONS <count>` (MySQL/MariaDB HASH/KEY semantics) and must be `>= 1`:

```php
$result = $schema->table('users')
    ->id()
    ->integer('user_id')
    ->partitionByHash('`user_id`', 4)
    ->create();

// ... PARTITION BY HASH(`user_id`) PARTITIONS 4
```

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
use Utopia\Query\Schema\PostgreSQL as Schema;

$schema = new Schema();

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

// Custom types — reference from a column via Column::userType()
$result = $schema->createType('mood_type', ['happy', 'sad', 'angry']);

$result = $schema->table('users')
    ->id()
    ->string('mood')->userType('mood_type')
    ->create();

$result = $schema->dropType('mood_type');

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

Type differences from MySQL: `INTEGER` (not `INT`), `DOUBLE PRECISION` (not `DOUBLE`), `BOOLEAN` (not `TINYINT(1)`), `JSONB` (not `JSON`), `BYTEA` (not `BLOB`), `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` for auto-incrementing ints, `VECTOR(n)` for pgvector, `GEOMETRY(type, srid)` for PostGIS. Enums use `TEXT CHECK (col IN (...))` (or a user-defined enum type via `userType()`).

### ClickHouse Schema

```php
use Utopia\Query\Schema\ClickHouse as Schema;
use Utopia\Query\Schema\ClickHouse\Engine;

$schema = new Schema();

$result = $schema->table('events')
    ->string('event_id', 36)->primary()
    ->string('event_type', 50)
    ->integer('count')
    ->datetime('created_at')
    ->create();

// CREATE TABLE `events` (...) ENGINE = MergeTree() ORDER BY (...)
```

ClickHouse uses `Nullable(type)` wrapping for nullable columns, `Enum8(...)` for enums, `Tuple(Float64, Float64)` for points, and `TYPE minmax GRANULARITY 3` for indexes. Foreign keys, stored procedures, triggers, generated columns, and CHECK constraints throw `UnsupportedException`.

Supports the `TableComments`, `ColumnComments`, and `DropPartition` interfaces.

**Engine selection** — choose from 10 variants of the `Engine` enum:

```php
// Standard MergeTree family
$schema->table('dedup')
    ->bigInteger('id')->primary()
    ->integer('version')
    ->engine(Engine::ReplacingMergeTree, 'version')
    ->create();
// ... ENGINE = ReplacingMergeTree(`version`) ORDER BY (`id`)

$schema->table('metrics')
    ->integer('key')->primary()
    ->bigInteger('total')->unsigned()
    ->engine(Engine::SummingMergeTree, 'total')
    ->create();

// CollapsingMergeTree requires a sign column (throws ValidationException otherwise)
// ReplicatedMergeTree requires zookeeper_path + replica_name
$schema->table('replicated')
    ->integer('id')->primary()
    ->engine(Engine::ReplicatedMergeTree, '/clickhouse/tables/events', 'replica_1')
    ->create();

// Non-MergeTree engines skip the ORDER BY tuple() fallback entirely
$schema->table('cache')
    ->integer('id')->primary()
    ->string('value')
    ->engine(Engine::Memory)
    ->create();
// CREATE TABLE `cache` (...) ENGINE = Memory
```

The 10 variants: `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, `ReplicatedMergeTree`, `Memory`, `Log`, `TinyLog`, `StripeLog`.

**TTL** — table-level and column-level time-to-live expressions:

```php
$schema->table('events')
    ->integer('id')->primary()
    ->datetime('ts')
    ->datetime('expires_at')->ttl('now() + INTERVAL 1 HOUR') // column-level
    ->ttl('ts + INTERVAL 1 DAY')                              // table-level
    ->create();

// Set the ORDER BY clause explicitly with `->orderBy([...])` if it should
// differ from the primary key.
```

TTL expressions are emitted verbatim; they must not be empty or contain semicolons. Dialects other than ClickHouse throw `UnsupportedException`.

**Skip-index algorithms** — every ClickHouse index is a data-skipping index that accelerates WHERE pruning by letting the engine skip whole granules. Pick the algorithm that matches the column shape via the `algorithm` argument on `Table::index()`:

```php
use Utopia\Query\Schema\ClickHouse\IndexAlgorithm;

$schema->create('events', function (Table $table) {
    $table->bigInteger('id')->primary();
    $table->string('user_id');
    $table->string('country');
    $table->string('text');

    // BloomFilter — high-cardinality strings with `=` / `IN` predicates
    $table->index(['user_id'], algorithm: IndexAlgorithm::BloomFilter);

    // Set(N) — small fixed value sets, custom granularity
    $table->index(['country'], algorithm: IndexAlgorithm::Set, algorithmArgs: [100], granularity: 4);

    // NgramBloomFilter(n, size_bytes, hashes, seed) — text search on `LIKE` / `match`
    $table->index(['text'], algorithm: IndexAlgorithm::NgramBloomFilter, algorithmArgs: [4, 1024, 3, 0]);

    // No algorithm specified → defaults to `TYPE minmax GRANULARITY 3`
    $table->index(['id']);
});

// CREATE TABLE `events` (..., INDEX `idx_user_id` `user_id` TYPE bloom_filter GRANULARITY 1, ...)
```

The 6 algorithms are `MinMax`, `Set`, `BloomFilter`, `NgramBloomFilter`, `TokenBloomFilter`, `Inverted`. Algorithm-specific arguments are passed via `algorithmArgs` and rendered verbatim — supply them from trusted (developer-controlled) source. Other dialects ignore the ClickHouse-only `algorithm` / `algorithmArgs` / `granularity` arguments.

`MinMax` and `Inverted` take no parenthesised arguments in ClickHouse DDL — passing `algorithmArgs` for them throws `ValidationException`. Skip indexes can also be added via `ALTER TABLE … ADD INDEX` by calling `index()` inside an `alter()` callback.

**Engine SETTINGS** — emit `SETTINGS k=v` after the TTL clause:

```php
$schema->create('events', function (Table $table) {
    $table->bigInteger('id')->primary();
    $table->settings([
        'index_granularity' => 8192,
        'allow_nullable_key' => true, // booleans become 1/0
    ]);
});

// CREATE TABLE `events` (...) ENGINE = MergeTree() ORDER BY (`id`)
//   SETTINGS index_granularity = 8192, allow_nullable_key = 1
```

Setting names must match `[A-Za-z_][A-Za-z0-9_]*`; string values are restricted to `[A-Za-z0-9_.\-+/]*`. Use ints / floats / booleans for everything else. Other dialects ignore the call.

### SQLite Schema

```php
use Utopia\Query\Schema\SQLite as Schema;
```

SQLite uses simplified type mappings: `INTEGER` for booleans, `TEXT` for datetimes/JSON, `REAL` for floats, `BLOB` for binary. Auto-increment uses `AUTOINCREMENT`. Vector and spatial types are not supported. Foreign keys, stored procedures, and triggers throw `UnsupportedException`. SERIAL types map to `INTEGER`. Both `STORED` and `VIRTUAL` generated columns are supported.

### MongoDB Schema

```php
use Utopia\Query\Schema\MongoDB as Schema;
```

The MongoDB schema generates JSON commands for collection management with BSON type validation.

**Creating collections** with JSON Schema validation:

```php
$schema = new Schema();

$result = $schema->table('users')
    ->string('name', 255)
    ->string('email', 255)->unique()
    ->integer('age')->nullable()
    ->boolean('active')->default(true)
    ->json('metadata')
    ->create();

// Generates a create command with bsonType validators
```

**Altering collections:**

```php
$result = $schema->table('users')
    ->string('phone', 20)->nullable()
    ->alter();

// Generates a collMod command to update the validator
```

**Indexes:**

```php
$result = $schema->createIndex('users', 'idx_email', ['email'], unique: true);
$result = $schema->dropIndex('users', 'idx_email');
```

**Collection operations:**

```php
$result = $schema->table('users')->drop();
$result = $schema->table('old_name')->rename('new_name');
$result = $schema->table('users')->truncate();
$result = $schema->analyzeTable('users');
```

**Views:**

```php
use Utopia\Query\Builder\MongoDB as Builder;

$query = (new Builder())->from('users')->filter([Query::equal('active', [true])]);
$result = $schema->createView('active_users', $query);
```

**Database management:**

```php
$result = $schema->createDatabase('analytics');
$result = $schema->dropDatabase('analytics');
```

Column types map to BSON types: `string` → `string`, `integer`/`bigInteger` → `int`, `float`/`double` → `double`, `boolean` → `bool`, `datetime`/`timestamp` → `date`, `json` → `object`, `binary` → `binData`. Composite primary keys, CHECK constraints, generated columns, SERIAL types, and user-defined types all throw `UnsupportedException`.

## Wire Protocol Parsers

The `Parser` interface classifies raw database traffic into query types (`Read`, `Write`, `TransactionBegin`, `TransactionEnd`, `Unknown`). This is useful for connection proxies, audit logging, and read/write splitting.

```php
use Utopia\Query\Parser;
use Utopia\Query\Type;
```

### SQL Parser

The abstract `Parser\SQL` class provides keyword-based classification for SQL dialects:

```php
use Utopia\Query\Parser\SQL;

// Classify SQL text directly
$type = $parser->classifySQL('SELECT * FROM users');  // Type::Read
$type = $parser->classifySQL('INSERT INTO users ...');  // Type::Write
$type = $parser->classifySQL('BEGIN');  // Type::TransactionBegin
$type = $parser->classifySQL('COMMIT');  // Type::TransactionEnd
```

Read keywords: `SELECT`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`, `WITH` (when followed by a read), `TABLE`, `VALUES`.

Write keywords: `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `CREATE`, `TRUNCATE`, `RENAME`, `REPLACE`, `LOAD`, `GRANT`, `REVOKE`, `MERGE`, `CALL`, `EXECUTE`, `DO`, `HANDLER`, `IMPORT`.

Transaction keywords: `BEGIN`, `START` → `TransactionBegin`; `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE` → `TransactionEnd`.

Special handling: `COPY` is classified based on direction (`FROM STDIN` = Write, `TO STDOUT` = Read). `SET` is classified as `TransactionEnd` (session configuration).

### MySQL Parser

Parses MySQL wire protocol binary packets:

```php
use Utopia\Query\Parser\MySQL;

$parser = new MySQL();
$type = $parser->parse($rawPacketData);  // Type::Read, Write, TransactionBegin, etc.
```

Recognizes MySQL command bytes including `COM_QUERY` (classifies via SQL text), `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, `COM_INIT_DB`, `COM_QUIT`, and others.

### PostgreSQL Parser

Parses PostgreSQL wire protocol messages:

```php
use Utopia\Query\Parser\PostgreSQL;

$parser = new PostgreSQL();
$type = $parser->parse($rawMessageData);  // Type::Read, Write, TransactionBegin, etc.
```

Handles message types including `Q` (simple query), `P` (parse/prepared statement), `X` (terminate), and startup messages.

### MongoDB Parser

Parses MongoDB OP_MSG binary protocol messages:

```php
use Utopia\Query\Parser\MongoDB;

$parser = new MongoDB();
$type = $parser->parse($rawOpMsgData);  // Type::Read, Write, TransactionBegin, etc.
```

Extracts the command name from BSON documents and classifies:

Read commands: `find`, `aggregate`, `count`, `distinct`, `listCollections`, `listDatabases`, `listIndexes`, `dbStats`, `collStats`, `explain`, `getMore`, `serverStatus`, `buildInfo`, `connectionStatus`, `ping`, `isMaster`, `hello`.

Write commands: `insert`, `update`, `delete`, `findAndModify`, `create`, `drop`, `createIndexes`, `dropIndexes`, `dropDatabase`, `renameCollection`.

Transaction detection: checks for `startTransaction: true` in the BSON document (`TransactionBegin`) or `commitTransaction`/`abortTransaction` commands (`TransactionEnd`).

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
