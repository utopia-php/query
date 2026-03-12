<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\SQLite as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;

class SQLiteTest extends TestCase
{
    use AssertsBindingCount;

    public function testImplementsCompiler(): void
    {
        $this->assertInstanceOf(Compiler::class, new Builder());
    }

    public function testImplementsJson(): void
    {
        $this->assertInstanceOf(Json::class, new Builder());
    }

    public function testImplementsConditionalAggregates(): void
    {
        $this->assertInstanceOf(ConditionalAggregates::class, new Builder());
    }

    public function testSortRandom(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RANDOM()', $result->query);
    }

    public function testRegexThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('REGEXP is not natively supported in SQLite.');

        (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^[a-z]+$')])
            ->build();
    }

    public function testSearchThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search is not supported in the SQLite query builder.');

        (new Builder())
            ->from('t')
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    public function testNotSearchThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::notSearch('content', 'hello')])
            ->build();
    }

    public function testUpsertUsesOnConflict(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice', 'email' => 'a@b.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?) ON CONFLICT (`id`) DO UPDATE SET `name` = excluded.`name`, `email` = excluded.`email`',
            $result->query
        );
        $this->assertEquals([1, 'Alice', 'a@b.com'], $result->bindings);
    }

    public function testUpsertMultipleConflictKeys(): void
    {
        $result = (new Builder())
            ->into('user_roles')
            ->set(['user_id' => 1, 'role_id' => 2, 'granted_at' => '2024-01-01'])
            ->onConflict(['user_id', 'role_id'], ['granted_at'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `user_roles` (`user_id`, `role_id`, `granted_at`) VALUES (?, ?, ?) ON CONFLICT (`user_id`, `role_id`) DO UPDATE SET `granted_at` = excluded.`granted_at`',
            $result->query
        );
        $this->assertEquals([1, 2, '2024-01-01'], $result->bindings);
    }

    public function testUpsertWithSetRaw(): void
    {
        $result = (new Builder())
            ->into('counters')
            ->set(['id' => 1, 'count' => 1])
            ->onConflict(['id'], ['count'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON CONFLICT (`id`) DO UPDATE SET `count` = excluded.`count`', $result->query);
    }

    public function testInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John', 'email' => 'john@example.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT OR IGNORE INTO `users` (`name`, `email`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['John', 'john@example.com'], $result->bindings);
    }

    public function testInsertOrIgnoreBatch(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->set(['name' => 'Bob', 'email' => 'b@b.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT OR IGNORE INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'a@b.com', 'Bob', 'b@b.com'], $result->bindings);
    }

    public function testSetJsonAppend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonAppend('tags', ['new_tag'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(value) FROM (SELECT value FROM json_each(IFNULL(`tags`, \'[]\')) UNION ALL SELECT value FROM json_each(?))', $result->query);
        $this->assertStringContainsString('UPDATE `docs` SET', $result->query);
    }

    public function testSetJsonPrepend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonPrepend('tags', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(value) FROM (SELECT value FROM json_each(?) UNION ALL SELECT value FROM json_each(IFNULL(`tags`, \'[]\')))', $result->query);
    }

    public function testSetJsonPrependOrderMatters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonPrepend('items', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_each(?) UNION ALL SELECT value FROM json_each(IFNULL(', $result->query);
    }

    public function testSetJsonInsert(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonInsert('tags', 0, 'inserted')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("json_insert(`tags`, '\$[0]', json(?))", $result->query);
    }

    public function testSetJsonInsertWithIndex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonInsert('items', 3, 'value')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("json_insert(`items`, '\$[3]', json(?))", $result->query);
    }

    public function testSetJsonRemove(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonRemove('tags', 'old_tag')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(value) FROM json_each(`tags`) WHERE value != json(?)', $result->query);
    }

    public function testSetJsonIntersect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonIntersect('tags', ['a', 'b'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(value) FROM json_each(IFNULL(`tags`, \'[]\')) WHERE value IN (SELECT value FROM json_each(?))', $result->query);
        $this->assertStringContainsString('UPDATE `t` SET', $result->query);
    }

    public function testSetJsonDiff(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonDiff('tags', ['x'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(value) FROM json_each(IFNULL(`tags`, \'[]\')) WHERE value NOT IN (SELECT value FROM json_each(?))', $result->query);
        $this->assertContains(\json_encode(['x']), $result->bindings);
    }

    public function testSetJsonUnique(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonUnique('tags')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('json_group_array(DISTINCT value) FROM json_each(IFNULL(`tags`, \'[]\'))', $result->query);
    }

    public function testUpdateClearsJsonSets(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->setJsonAppend('tags', ['a'])
            ->filter([Query::equal('id', [1])]);

        $result1 = $builder->update();
        $this->assertBindingCount($result1);
        $this->assertStringContainsString('json_group_array', $result1->query);

        $builder->reset();

        $result2 = $builder
            ->from('t')
            ->set(['name' => 'test'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result2);
        $this->assertStringNotContainsString('json_group_array', $result2->query);
    }

    public function testCountWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ?', 'active_count', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `active_count`', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testCountWhenWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ?', '', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testSumWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sumWhen('amount', 'status = ?', 'total_active', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(CASE WHEN status = ? THEN `amount` END) AS `total_active`', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testSumWhenWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sumWhen('amount', 'status = ?', '', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(CASE WHEN status = ? THEN `amount` END)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testAvgWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->avgWhen('amount', 'region = ?', 'avg_east', 'east')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('AVG(CASE WHEN region = ? THEN `amount` END) AS `avg_east`', $result->query);
        $this->assertEquals(['east'], $result->bindings);
    }

    public function testAvgWhenWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->avgWhen('amount', 'region = ?', '', 'east')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('AVG(CASE WHEN region = ? THEN `amount` END)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testMinWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('products')
            ->minWhen('price', 'category = ?', 'min_electronics', 'electronics')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MIN(CASE WHEN category = ? THEN `price` END) AS `min_electronics`', $result->query);
        $this->assertEquals(['electronics'], $result->bindings);
    }

    public function testMinWhenWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('products')
            ->minWhen('price', 'category = ?', '', 'electronics')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MIN(CASE WHEN category = ? THEN `price` END)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testMaxWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('products')
            ->maxWhen('price', 'category = ?', 'max_electronics', 'electronics')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MAX(CASE WHEN category = ? THEN `price` END) AS `max_electronics`', $result->query);
        $this->assertEquals(['electronics'], $result->bindings);
    }

    public function testMaxWhenWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('products')
            ->maxWhen('price', 'category = ?', '', 'electronics')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MAX(CASE WHEN category = ? THEN `price` END)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testSpatialDistanceThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Spatial distance queries are not supported in SQLite.');

        (new Builder())
            ->from('t')
            ->filter([Query::distanceLessThan('attr', [0, 0], 1000, true)])
            ->build();
    }

    public function testSpatialPredicateThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Spatial predicates are not supported in SQLite.');

        (new Builder())
            ->from('t')
            ->filter([Query::intersects('attr', [1.0, 2.0])])
            ->build();
    }

    public function testSpatialNotIntersectsThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::notIntersects('attr', [1.0, 2.0])])
            ->build();
    }

    public function testSpatialCoversThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Spatial covers predicates are not supported in SQLite.');

        (new Builder())
            ->from('t')
            ->filterCovers('area', [1.0, 2.0])
            ->build();
    }

    public function testFilterJsonContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonContains('tags', ['php', 'js'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?))', $result->query);
        $this->assertStringContainsString(' AND ', $result->query);
    }

    public function testFilterJsonNotContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('tags', ['admin'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT (EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?)))', $result->query);
    }

    public function testFilterJsonOverlaps(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'js'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?))', $result->query);
        $this->assertStringContainsString(' OR ', $result->query);
    }

    public function testFilterJsonPathValid(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age', '>=', 21)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("json_extract(`data`, '$.age') >= ?", $result->query);
        $this->assertEquals(21, $result->bindings[0]);
    }

    public function testFilterJsonPathInvalidPathThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid JSON path');

        (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age; DROP TABLE users', '=', 1)
            ->build();
    }

    public function testFilterJsonPathInvalidOperatorThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid JSON path operator');

        (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age', 'LIKE', 1)
            ->build();
    }

    public function testFilterJsonPathAllOperators(): void
    {
        $operators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        foreach ($operators as $op) {
            $result = (new Builder())
                ->from('t')
                ->filterJsonPath('data', 'val', $op, 42)
                ->build();
            $this->assertBindingCount($result);

            $this->assertStringContainsString("json_extract(`data`, '$.val') {$op} ?", $result->query);
        }
    }

    public function testFilterJsonContainsSingleItem(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filterJsonContains('tags', 'php')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?))', $result->query);
    }

    public function testFilterJsonContainsMultipleItems(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filterJsonContains('tags', ['php', 'js', 'rust'])
            ->build();
        $this->assertBindingCount($result);

        $count = substr_count($result->query, 'EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?))');
        $this->assertEquals(3, $count);
        $this->assertStringContainsString(' AND ', $result->query);
    }

    public function testFilterJsonOverlapsMultipleItems(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filterJsonOverlaps('tags', ['a', 'b', 'c'])
            ->build();
        $this->assertBindingCount($result);

        $count = substr_count($result->query, 'EXISTS (SELECT 1 FROM json_each(`tags`) WHERE json_each.value = json(?))');
        $this->assertEquals(3, $count);
        $this->assertStringContainsString(' OR ', $result->query);
    }

    public function testResetClearsJsonSets(): void
    {
        $builder = new Builder();
        $builder->from('t')->setJsonAppend('tags', ['a']);
        $builder->reset();

        $result = $builder
            ->from('t')
            ->set(['name' => 'test'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('json_group_array', $result->query);
        $this->assertEquals('UPDATE `t` SET `name` = ? WHERE `id` IN (?)', $result->query);
    }

    public function testBasicSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testSelectWithColumns(): void
    {
        $result = (new Builder())
            ->select(['name', 'email'])
            ->from('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `name`, `email` FROM `users`', $result->query);
    }

    public function testFilterAndSort(): void
    {
        $result = (new Builder())
            ->select(['name'])
            ->from('users')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])
            ->sortAsc('name')
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `name` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['active', 18, 10, 5], $result->bindings);
    }

    public function testInsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`name`, `email`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'a@b.com'], $result->bindings);
    }

    public function testInsertBatch(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->set(['name' => 'Bob', 'email' => 'b@b.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'a@b.com', 'Bob', 'b@b.com'], $result->bindings);
    }

    public function testUpdateWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('status', ['inactive'])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `users` SET `status` = ? WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['archived', 'inactive'], $result->bindings);
    }

    public function testDeleteWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::lessThan('last_login', '2024-01-01')])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'DELETE FROM `users` WHERE `last_login` < ?',
            $result->query
        );
        $this->assertEquals(['2024-01-01'], $result->bindings);
    }

    public function testDeleteWithoutWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals('DELETE FROM `users`', $result->query);
    }

    public function testTransactionStatements(): void
    {
        $builder = new Builder();

        $this->assertEquals('BEGIN', $builder->begin()->query);
        $this->assertEquals('COMMIT', $builder->commit()->query);
        $this->assertEquals('ROLLBACK', $builder->rollback()->query);
    }

    public function testSavepoint(): void
    {
        $builder = new Builder();

        $this->assertEquals('SAVEPOINT `sp1`', $builder->savepoint('sp1')->query);
        $this->assertEquals('RELEASE SAVEPOINT `sp1`', $builder->releaseSavepoint('sp1')->query);
        $this->assertEquals('ROLLBACK TO SAVEPOINT `sp1`', $builder->rollbackToSavepoint('sp1')->query);
    }

    public function testForUpdate(): void
    {
        $result = (new Builder())
            ->from('t')
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE', $result->query);
    }

    public function testForShare(): void
    {
        $result = (new Builder())
            ->from('t')
            ->forShare()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE', $result->query);
    }

    public function testSetJsonAppendBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonAppend('tags', ['a', 'b'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode(['a', 'b']), $result->bindings);
    }

    public function testSetJsonPrependBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonPrepend('tags', ['x', 'y'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode(['x', 'y']), $result->bindings);
    }

    public function testSetJsonInsertBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonInsert('items', 5, 'hello')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode('hello'), $result->bindings);
    }

    public function testSetJsonRemoveBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonRemove('tags', 'remove_me')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode('remove_me'), $result->bindings);
    }

    public function testSetJsonIntersectBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonIntersect('tags', ['keep_a', 'keep_b'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode(['keep_a', 'keep_b']), $result->bindings);
    }

    public function testSetJsonDiffBindingValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonDiff('tags', ['remove_a'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertContains(\json_encode(['remove_a']), $result->bindings);
    }

    public function testConditionalAggregatesMultipleBindings(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ? AND region = ?', 'combo', 'active', 'east')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? AND region = ? THEN 1 END) AS `combo`', $result->query);
        $this->assertEquals(['active', 'east'], $result->bindings);
    }

    public function testSpatialDistanceGreaterThanThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::distanceGreaterThan('attr', [0, 0], 500, false)])
            ->build();
    }

    public function testSpatialEqualsThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filterSpatialEquals('area', [1.0, 2.0])
            ->build();
    }

    public function testSpatialCrossesThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::crosses('path', [1.0, 2.0])])
            ->build();
    }

    public function testSpatialTouchesThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::touches('area', [1.0, 2.0])])
            ->build();
    }

    public function testSpatialOverlapsThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::overlaps('area', [[0, 0], [1, 1]])])
            ->build();
    }

    public function testExactUpsertQuery(): void
    {
        $result = (new Builder())
            ->into('settings')
            ->set(['key' => 'theme', 'value' => 'dark'])
            ->onConflict(['key'], ['value'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertSame(
            'INSERT INTO `settings` (`key`, `value`) VALUES (?, ?) ON CONFLICT (`key`) DO UPDATE SET `value` = excluded.`value`',
            $result->query
        );
        $this->assertEquals(['theme', 'dark'], $result->bindings);
    }

    public function testExactInsertOrIgnoreQuery(): void
    {
        $result = (new Builder())
            ->into('t')
            ->set(['id' => 1, 'name' => 'test'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertSame(
            'INSERT OR IGNORE INTO `t` (`id`, `name`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals([1, 'test'], $result->bindings);
    }

    public function testExactCountWhenQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->countWhen('active = ?', 'active_count', 1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT COUNT(CASE WHEN active = ? THEN 1 END) AS `active_count` FROM `t`',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testExactSumWhenQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sumWhen('amount', 'type = ?', 'credit_total', 'credit')
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT SUM(CASE WHEN type = ? THEN `amount` END) AS `credit_total` FROM `t`',
            $result->query
        );
        $this->assertEquals(['credit'], $result->bindings);
    }

    public function testExactAvgWhenQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->avgWhen('score', 'grade = ?', 'avg_a', 'A')
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT AVG(CASE WHEN grade = ? THEN `score` END) AS `avg_a` FROM `t`',
            $result->query
        );
        $this->assertEquals(['A'], $result->bindings);
    }

    public function testExactMinWhenQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->minWhen('price', 'in_stock = ?', 'min_available', 1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT MIN(CASE WHEN in_stock = ? THEN `price` END) AS `min_available` FROM `t`',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testExactMaxWhenQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->maxWhen('price', 'in_stock = ?', 'max_available', 1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT MAX(CASE WHEN in_stock = ? THEN `price` END) AS `max_available` FROM `t`',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testExactFilterJsonPathQuery(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('profile', 'settings.theme', '=', 'dark')
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            "SELECT * FROM `users` WHERE json_extract(`profile`, '$.settings.theme') = ?",
            $result->query
        );
        $this->assertEquals(['dark'], $result->bindings);
    }

    public function testSetJsonAppendReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonAppend('tags', ['a']);
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonPrependReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonPrepend('tags', ['a']);
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonInsertReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonInsert('tags', 0, 'a');
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonRemoveReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonRemove('tags', 'a');
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonIntersectReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonIntersect('tags', ['a']);
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonDiffReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonDiff('tags', ['a']);
        $this->assertSame($builder, $returned);
    }

    public function testSetJsonUniqueReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->from('t')->setJsonUnique('tags');
        $this->assertSame($builder, $returned);
    }

    public function testResetReturnsSelf(): void
    {
        $builder = new Builder();
        $returned = $builder->reset();
        $this->assertSame($builder, $returned);
    }

    public function testCteJoinWhereGroupByHavingOrderLimit(): void
    {
        $cte = (new Builder())
            ->from('raw_orders')
            ->select(['customer_id', 'amount'])
            ->filter([Query::greaterThan('amount', 0)]);

        $result = (new Builder())
            ->with('filtered_orders', $cte)
            ->from('filtered_orders')
            ->join('customers', 'filtered_orders.customer_id', 'customers.id')
            ->filter([Query::equal('customers.active', [1])])
            ->sum('filtered_orders.amount', 'total')
            ->groupBy(['customers.country'])
            ->having([Query::greaterThan('total', 100)])
            ->sortDesc('total')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH `filtered_orders` AS', $result->query);
        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('WHERE `customers`.`active` IN (?)', $result->query);
        $this->assertStringContainsString('GROUP BY `customers`.`country`', $result->query);
        $this->assertStringContainsString('HAVING `total` > ?', $result->query);
        $this->assertStringContainsString('ORDER BY `total` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testRecursiveCteWithFilter(): void
    {
        $seed = (new Builder())
            ->from('categories')
            ->select(['id', 'name', 'parent_id'])
            ->filter([Query::isNull('parent_id')]);

        $step = (new Builder())
            ->from('categories')
            ->select(['categories.id', 'categories.name', 'categories.parent_id'])
            ->join('tree', 'categories.parent_id', 'tree.id');

        $result = (new Builder())
            ->withRecursiveSeedStep('tree', $seed, $step)
            ->from('tree')
            ->filter([Query::notEqual('name', 'Hidden')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH RECURSIVE `tree` AS', $result->query);
        $this->assertStringContainsString('UNION ALL', $result->query);
        $this->assertStringContainsString('WHERE `name` != ?', $result->query);
    }

    public function testMultipleCTEs(): void
    {
        $cte1 = (new Builder())
            ->from('orders')
            ->select(['customer_id'])
            ->sum('total', 'order_sum')
            ->groupBy(['customer_id']);

        $cte2 = (new Builder())
            ->from('customers')
            ->select(['id', 'name'])
            ->filter([Query::equal('active', [1])]);

        $result = (new Builder())
            ->with('order_sums', $cte1)
            ->with('active_customers', $cte2)
            ->from('order_sums')
            ->join('active_customers', 'order_sums.customer_id', 'active_customers.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH `order_sums` AS', $result->query);
        $this->assertStringContainsString('`active_customers` AS', $result->query);
    }

    public function testWindowFunctionWithJoin(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->join('products', 'sales.product_id', 'products.id')
            ->selectWindow('ROW_NUMBER()', 'rn', ['products.category'], ['sales.amount'])
            ->select(['products.name', 'sales.amount'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER', $result->query);
        $this->assertStringContainsString('JOIN `products`', $result->query);
    }

    public function testWindowFunctionWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->selectWindow('SUM(amount)', 'running', ['category'], ['date'])
            ->select(['category', 'date'])
            ->groupBy(['category', 'date'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(amount) OVER', $result->query);
        $this->assertStringContainsString('GROUP BY', $result->query);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->selectWindow('ROW_NUMBER()', 'rn', ['department'], ['salary'])
            ->selectWindow('RANK()', 'rnk', ['department'], ['-salary'])
            ->select(['name', 'department', 'salary'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER', $result->query);
        $this->assertStringContainsString('RANK() OVER', $result->query);
    }

    public function testNamedWindowDefinition(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->window('w', ['category'], ['date'])
            ->selectWindow('SUM(amount)', 'total', null, null, 'w')
            ->select(['category', 'date', 'amount'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WINDOW `w` AS', $result->query);
        $this->assertStringContainsString('OVER `w`', $result->query);
    }

    public function testJoinAggregateGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('customers', 'orders.customer_id', 'customers.id')
            ->count('*', 'cnt')
            ->sum('orders.total', 'revenue')
            ->groupBy(['customers.country'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('GROUP BY `customers`.`country`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
    }

    public function testSelfJoin(): void
    {
        $result = (new Builder())
            ->from('employees', 'e')
            ->leftJoin('employees', 'e.manager_id', 'm.id', '=', 'm')
            ->select(['e.name', 'm.name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `employees` AS `e`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `employees` AS `m`', $result->query);
    }

    public function testTripleJoin(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('customers', 'orders.customer_id', 'customers.id')
            ->join('products', 'orders.product_id', 'products.id')
            ->leftJoin('categories', 'products.category_id', 'categories.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('JOIN `products`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `categories`', $result->query);
    }

    public function testLeftJoinWithInnerJoinCombined(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('customers', 'orders.customer_id', 'customers.id')
            ->leftJoin('discounts', 'orders.discount_id', 'discounts.id')
            ->filter([Query::isNotNull('customers.email')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `discounts`', $result->query);
    }

    public function testUnionAndUnionAllMixed(): void
    {
        $q2 = (new Builder())->from('t2')->filter([Query::equal('year', [2023])]);
        $q3 = (new Builder())->from('t3')->filter([Query::equal('year', [2022])]);

        $result = (new Builder())
            ->from('t1')
            ->filter([Query::equal('year', [2024])])
            ->union($q2)
            ->unionAll($q3)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION (SELECT', $result->query);
        $this->assertStringContainsString('UNION ALL (SELECT', $result->query);
    }

    public function testMultipleUnions(): void
    {
        $q2 = (new Builder())->from('t2');
        $q3 = (new Builder())->from('t3');
        $q4 = (new Builder())->from('t4');

        $result = (new Builder())
            ->from('t1')
            ->union($q2)
            ->union($q3)
            ->union($q4)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(3, substr_count($result->query, 'UNION'));
    }

    public function testSubSelectWithFilter(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['customer_id'])
            ->sum('total', 'total_spent')
            ->groupBy(['customer_id']);

        $result = (new Builder())
            ->from('customers')
            ->selectSub($sub, 'spending')
            ->filter([Query::equal('active', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE `active` IN (?)', $result->query);
    }

    public function testFromSubqueryWithJoin(): void
    {
        $sub = (new Builder())
            ->from('events')
            ->select(['user_id'])
            ->count('*', 'event_count')
            ->groupBy(['user_id']);

        $result = (new Builder())
            ->fromSub($sub, 'user_events')
            ->join('users', 'user_events.user_id', 'users.id')
            ->filter([Query::greaterThan('event_count', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM (SELECT', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
    }

    public function testFilterWhereInSubquery(): void
    {
        $sub = (new Builder())
            ->from('premium_users')
            ->select(['id'])
            ->filter([Query::equal('tier', ['gold'])]);

        $result = (new Builder())
            ->from('orders')
            ->filterWhereIn('user_id', $sub)
            ->filter([Query::greaterThan('total', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`user_id` IN (SELECT', $result->query);
    }

    public function testExistsSubqueryWithFilter(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->filter([Query::raw('orders.customer_id = customers.id')])
            ->filter([Query::greaterThan('total', 500)]);

        $result = (new Builder())
            ->from('customers')
            ->filterExists($sub)
            ->filter([Query::equal('active', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT', $result->query);
        $this->assertStringContainsString('`active` IN (?)', $result->query);
    }

    public function testInsertOrIgnoreVerifySyntax(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Test', 'email' => 'test@test.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('INSERT OR IGNORE INTO', $result->query);
        $this->assertStringContainsString('(`id`, `name`, `email`)', $result->query);
    }

    public function testUpsertConflictHandling(): void
    {
        $result = (new Builder())
            ->into('settings')
            ->set(['key' => 'theme', 'value' => 'dark', 'updated_at' => '2024-01-01'])
            ->onConflict(['key'], ['value', 'updated_at'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON CONFLICT (`key`) DO UPDATE SET', $result->query);
        $this->assertStringContainsString('`value` = excluded.`value`', $result->query);
        $this->assertStringContainsString('`updated_at` = excluded.`updated_at`', $result->query);
    }

    public function testCaseExpressionWithWhere(): void
    {
        $case = (new \Utopia\Query\Builder\Case\Builder())
            ->when('status = ?', "'Active'", ['active'])
            ->when('status = ?', "'Inactive'", ['inactive'])
            ->elseResult("'Unknown'")
            ->alias('`label`')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->selectCase($case)
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN', $result->query);
        $this->assertStringContainsString('WHERE `age` > ?', $result->query);
    }

    public function testBeforeBuildCallback(): void
    {
        $callbackCalled = false;
        $result = (new Builder())
            ->from('users')
            ->beforeBuild(function (Builder $b) use (&$callbackCalled) {
                $callbackCalled = true;
                $b->filter([Query::equal('injected', ['yes'])]);
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertTrue($callbackCalled);
        $this->assertStringContainsString('`injected` IN (?)', $result->query);
    }

    public function testAfterBuildCallback(): void
    {
        $capturedQuery = '';
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->afterBuild(function (BuildResult $r) use (&$capturedQuery) {
                $capturedQuery = 'executed';
                return $r;
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('executed', $capturedQuery);
    }

    public function testUpdateWithComplexFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([
                Query::or([
                    Query::lessThan('last_login', '2023-01-01'),
                    Query::and([
                        Query::equal('role', ['guest']),
                        Query::isNull('email_verified_at'),
                    ]),
                ]),
            ])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('UPDATE `users` SET', $result->query);
        $this->assertStringContainsString('`last_login` < ?', $result->query);
        $this->assertStringContainsString('`role` IN (?)', $result->query);
    }

    public function testDeleteWithSubqueryFilter(): void
    {
        $sub = (new Builder())
            ->from('blocked_ids')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('sessions')
            ->filterWhereIn('user_id', $sub)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('DELETE FROM `sessions`', $result->query);
        $this->assertStringContainsString('`user_id` IN (SELECT', $result->query);
    }

    public function testNestedLogicalOperatorsDepth3(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::and([
                        Query::or([
                            Query::equal('a', [1]),
                            Query::equal('b', [2]),
                        ]),
                        Query::greaterThan('c', 3),
                    ]),
                    Query::lessThan('d', 4),
                ]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([1, 2, 3, 4], $result->bindings);
    }

    public function testIsNullAndEqualCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::isNull('deleted_at'),
                Query::equal('status', ['active']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`deleted_at` IS NULL', $result->query);
        $this->assertStringContainsString('`status` IN (?)', $result->query);
    }

    public function testBetweenAndGreaterThanCombined(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([
                Query::between('price', 10, 100),
                Query::greaterThan('stock', 0),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`price` BETWEEN ? AND ?', $result->query);
        $this->assertStringContainsString('`stock` > ?', $result->query);
        $this->assertEquals([10, 100, 0], $result->bindings);
    }

    public function testStartsWithAndContainsCombined(): void
    {
        $result = (new Builder())
            ->from('files')
            ->filter([
                Query::startsWith('path', '/usr'),
                Query::contains('name', ['test']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("LIKE ?", $result->query);
    }

    public function testDistinctWithAggregate(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->distinct()
            ->countDistinct('customer_id', 'unique_customers')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('COUNT(DISTINCT `customer_id`)', $result->query);
    }

    public function testMultipleOrderByColumns(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('last_name')
            ->sortAsc('first_name')
            ->sortDesc('created_at')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` ORDER BY `last_name` ASC, `first_name` ASC, `created_at` DESC',
            $result->query
        );
    }

    public function testEmptySelectReturnsAllColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testBooleanValuesInFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::equal('active', [true]),
                Query::equal('deleted', [false]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([true, false], $result->bindings);
    }

    public function testBindingOrderVerification(): void
    {
        $cte = (new Builder())
            ->from('raw')
            ->filter([Query::greaterThan('val', 0)]);

        $result = (new Builder())
            ->with('filtered', $cte)
            ->from('filtered')
            ->filter([Query::equal('status', ['active'])])
            ->count('*', 'cnt')
            ->groupBy(['region'])
            ->having([Query::greaterThan('cnt', 5)])
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(0, $result->bindings[0]);
        $this->assertEquals('active', $result->bindings[1]);
        $this->assertEquals(5, $result->bindings[2]);
        $this->assertEquals(10, $result->bindings[3]);
    }

    public function testCloneAndModify(): void
    {
        $original = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])]);

        $cloned = $original->clone();
        $cloned->filter([Query::greaterThan('age', 18)]);

        $origResult = $original->build();
        $clonedResult = $cloned->build();
        $this->assertBindingCount($origResult);
        $this->assertBindingCount($clonedResult);

        $this->assertStringNotContainsString('`age`', $origResult->query);
        $this->assertStringContainsString('`age` > ?', $clonedResult->query);
    }

    public function testResetAndRebuild(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(10);

        $builder->build();
        $builder->reset();

        $result = $builder
            ->from('orders')
            ->filter([Query::greaterThan('total', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `orders` WHERE `total` > ?', $result->query);
        $this->assertEquals([100], $result->bindings);
    }

    public function testReadOnlyFlagOnSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertTrue($result->readOnly);
    }

    public function testReadOnlyFlagOnInsert(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'test'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertFalse($result->readOnly);
    }

    public function testMultipleSetForInsertUpdate(): void
    {
        $result = (new Builder())
            ->into('events')
            ->set(['name' => 'a', 'value' => 1])
            ->set(['name' => 'b', 'value' => 2])
            ->set(['name' => 'c', 'value' => 3])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('VALUES (?, ?), (?, ?), (?, ?)', $result->query);
    }

    public function testGroupByMultipleColumns(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['region', 'category', 'year'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY `region`, `category`, `year`', $result->query);
    }

    public function testExplainQuery(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
        $this->assertTrue($result->readOnly);
    }

    public function testExplainAnalyzeQuery(): void
    {
        $result = (new Builder())
            ->from('users')
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
    }

    public function testFilterWhereNotInSubquery(): void
    {
        $sub = (new Builder())
            ->from('blocked')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterWhereNotIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`id` NOT IN (SELECT', $result->query);
    }

    public function testInsertSelectFromSubquery(): void
    {
        $source = (new Builder())
            ->from('staging')
            ->select(['name', 'email'])
            ->filter([Query::equal('imported', [0])]);

        $result = (new Builder())
            ->into('users')
            ->fromSelect(['name', 'email'], $source)
            ->insertSelect();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INSERT INTO `users`', $result->query);
        $this->assertStringContainsString('SELECT `name`, `email` FROM `staging`', $result->query);
    }

    public function testLimitOneOffsetZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(1)
            ->offset(0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([1, 0], $result->bindings);
    }

    public function testSelectRawExpression(): void
    {
        $result = (new Builder())
            ->from('users')
            ->selectRaw("strftime('%Y', created_at) AS year")
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("strftime('%Y', created_at) AS year", $result->query);
    }

    public function testCountWhenWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ?', 'active_count', 'active')
            ->countWhen('status = ?', 'pending_count', 'pending')
            ->groupBy(['region'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `active_count`', $result->query);
        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `pending_count`', $result->query);
        $this->assertStringContainsString('GROUP BY `region`', $result->query);
    }

    public function testNotBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([Query::notBetween('price', 10, 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`price` NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([10, 50], $result->bindings);
    }

    public function testMultipleFilterTypes(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([
                Query::greaterThan('price', 10),
                Query::startsWith('name', 'Pro'),
                Query::contains('description', ['premium']),
                Query::isNotNull('sku'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`price` > ?', $result->query);
        $this->assertStringContainsString('LIKE ?', $result->query);
        $this->assertStringContainsString('`sku` IS NOT NULL', $result->query);
    }
}
