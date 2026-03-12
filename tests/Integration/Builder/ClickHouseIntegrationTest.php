<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Query;

class ClickHouseIntegrationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->connectClickhouse();

        $this->trackClickhouseTable('ch_events');
        $this->trackClickhouseTable('ch_users');

        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_events`');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_users`');

        $this->clickhouseStatement('
            CREATE TABLE `ch_users` (
                `id` UInt32,
                `name` String,
                `email` String,
                `age` UInt32,
                `country` String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY `id`
        ');

        $this->clickhouseStatement('
            CREATE TABLE `ch_events` (
                `id` UInt32,
                `user_id` UInt32,
                `action` String,
                `value` Float64,
                `created_at` DateTime
            ) ENGINE = MergeTree()
            ORDER BY (`id`, `created_at`)
        ');

        $this->clickhouseStatement("
            INSERT INTO `ch_users` (`id`, `name`, `email`, `age`, `country`) VALUES
            (1, 'Alice', 'alice@test.com', 30, 'US'),
            (2, 'Bob', 'bob@test.com', 25, 'UK'),
            (3, 'Charlie', 'charlie@test.com', 35, 'US'),
            (4, 'Diana', 'diana@test.com', 28, 'DE'),
            (5, 'Eve', 'eve@test.com', 22, 'UK')
        ");

        $this->clickhouseStatement("
            INSERT INTO `ch_events` (`id`, `user_id`, `action`, `value`, `created_at`) VALUES
            (1, 1, 'click', 1.5, '2024-01-01 10:00:00'),
            (2, 1, 'purchase', 99.99, '2024-01-02 11:00:00'),
            (3, 2, 'click', 2.0, '2024-01-01 12:00:00'),
            (4, 2, 'click', 3.5, '2024-01-03 09:00:00'),
            (5, 3, 'purchase', 49.99, '2024-01-02 14:00:00'),
            (6, 3, 'view', 0.0, '2024-01-04 08:00:00'),
            (7, 4, 'click', 1.0, '2024-01-05 10:00:00'),
            (8, 5, 'purchase', 199.99, '2024-01-06 16:00:00')
        ");
    }

    public function testSelectWithWhere(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name', 'country'])
            ->filter([Query::equal('country', ['US'])])
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    public function testSelectWithOrderByAndLimit(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name', 'age'])
            ->sortDesc('age')
            ->limit(3)
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(3, $rows);
        $this->assertEquals('Charlie', $rows[0]['name']);
        $this->assertEquals('Alice', $rows[1]['name']);
        $this->assertEquals('Diana', $rows[2]['name']);
    }

    public function testSelectWithJoin(): void
    {
        $result = (new Builder())
            ->from('ch_events', 'e')
            ->select(['e.id', 'e.action', 'u.name'])
            ->join('ch_users', 'e.user_id', 'u.id', '=', 'u')
            ->filter([Query::equal('e.action', ['purchase'])])
            ->sortAsc('e.id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(3, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Charlie', $rows[1]['name']);
        $this->assertEquals('Eve', $rows[2]['name']);
    }

    public function testSelectWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('ch_events')
            ->select(['id', 'action', 'value'])
            ->prewhere([Query::equal('action', ['click'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(4, $rows);
        foreach ($rows as $row) {
            $this->assertEquals('click', $row['action']);
        }
    }

    public function testSelectWithFinal(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name'])
            ->final()
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(5, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }

    public function testInsertSingleRow(): void
    {
        $insert = (new Builder())
            ->into('ch_events')
            ->set([
                'id' => 100,
                'user_id' => 1,
                'action' => 'signup',
                'value' => 0.0,
                'created_at' => '2024-02-01 00:00:00',
            ])
            ->insert();

        $this->executeOnClickhouse($insert);

        $select = (new Builder())
            ->from('ch_events')
            ->select(['id', 'action'])
            ->filter([Query::equal('id', [100])])
            ->build();

        $rows = $this->executeOnClickhouse($select);

        $this->assertCount(1, $rows);
        $this->assertEquals('signup', $rows[0]['action']);
    }

    public function testInsertMultipleRows(): void
    {
        $insert = (new Builder())
            ->into('ch_users')
            ->set(['id' => 10, 'name' => 'Frank', 'email' => 'frank@test.com', 'age' => 40, 'country' => 'FR'])
            ->set(['id' => 11, 'name' => 'Grace', 'email' => 'grace@test.com', 'age' => 33, 'country' => 'FR'])
            ->insert();

        $this->executeOnClickhouse($insert);

        $select = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name'])
            ->filter([Query::equal('country', ['FR'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($select);

        $this->assertCount(2, $rows);
        $this->assertEquals('Frank', $rows[0]['name']);
        $this->assertEquals('Grace', $rows[1]['name']);
    }

    public function testSelectWithGroupByAndHaving(): void
    {
        $result = (new Builder())
            ->from('ch_events')
            ->select(['action'])
            ->count('*', 'cnt')
            ->groupBy(['action'])
            ->having([Query::greaterThan('cnt', 1)])
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $actions = array_column($rows, 'action');
        $this->assertContains('click', $actions);
        $this->assertContains('purchase', $actions);
        foreach ($rows as $row) {
            $this->assertGreaterThan(1, (int) $row['cnt']); // @phpstan-ignore cast.int
        }
    }

    public function testSelectWithUnionAll(): void
    {
        $first = (new Builder())
            ->from('ch_users')
            ->select(['name'])
            ->filter([Query::equal('country', ['US'])]);

        $second = (new Builder())
            ->from('ch_users')
            ->select(['name'])
            ->filter([Query::equal('country', ['UK'])]);

        $result = $first->unionAll($second)->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(4, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Eve', $names);
    }

    public function testSelectWithCte(): void
    {
        $cteQuery = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name', 'country'])
            ->filter([Query::equal('country', ['US'])]);

        $result = (new Builder())
            ->with('us_users', $cteQuery)
            ->from('us_users')
            ->select(['id', 'name'])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(2, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Charlie', $rows[1]['name']);
    }

    public function testSelectWithWindowFunction(): void
    {
        $result = (new Builder())
            ->from('ch_events')
            ->select(['id', 'action', 'value'])
            ->selectWindow('row_number()', 'rn', ['action'], ['id'])
            ->filter([Query::equal('action', ['click'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(4, $rows);
        $this->assertEquals(1, (int) $rows[0]['rn']); // @phpstan-ignore cast.int
        $this->assertEquals(2, (int) $rows[1]['rn']); // @phpstan-ignore cast.int
        $this->assertEquals(3, (int) $rows[2]['rn']); // @phpstan-ignore cast.int
        $this->assertEquals(4, (int) $rows[3]['rn']); // @phpstan-ignore cast.int
    }

    public function testSelectWithDistinct(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['country'])
            ->distinct()
            ->sortAsc('country')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(3, $rows);
        $countries = array_column($rows, 'country');
        $this->assertEquals(['DE', 'UK', 'US'], $countries);
    }

    public function testSelectWithSubqueryInWhere(): void
    {
        $subquery = (new Builder())
            ->from('ch_events')
            ->select(['user_id'])
            ->filter([Query::equal('action', ['purchase'])]);

        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name'])
            ->filterWhereIn('id', $subquery)
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Eve', $names);
    }

    public function testSelectWithSample(): void
    {
        $this->trackClickhouseTable('ch_sample');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_sample`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_sample` (
                `id` UInt32,
                `name` String
            ) ENGINE = MergeTree()
            ORDER BY `id`
            SAMPLE BY `id`
        ');
        $this->clickhouseStatement("INSERT INTO `ch_sample` VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')");

        $result = (new Builder())
            ->from('ch_sample')
            ->select(['id', 'name'])
            ->sample(0.5)
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertLessThanOrEqual(5, count($rows));
        foreach ($rows as $row) {
            $this->assertArrayHasKey('id', $row);
            $this->assertArrayHasKey('name', $row);
        }
    }

    public function testSelectWithSettings(): void
    {
        $result = (new Builder())
            ->from('ch_events')
            ->select(['id', 'action', 'value'])
            ->sortAsc('id')
            ->settings(['max_threads' => '2'])
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(8, $rows);
        $this->assertEquals('click', $rows[0]['action']);
    }
}
