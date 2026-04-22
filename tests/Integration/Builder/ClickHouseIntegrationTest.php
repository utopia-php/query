<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\Case\Expression as CaseExpression;
use Utopia\Query\Builder\Case\Operator;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Builder\WindowFrame;
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

    public function testSelectWithBetween(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name', 'age'])
            ->filter([Query::between('age', 25, 30)])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $ages = array_column($rows, 'age');
        foreach ($ages as $age) {
            $this->assertGreaterThanOrEqual(25, (int) $age); // @phpstan-ignore cast.int
            $this->assertLessThanOrEqual(30, (int) $age); // @phpstan-ignore cast.int
        }
        $this->assertContains('Alice', array_column($rows, 'name'));
    }

    public function testSelectWithStartsWithAndContains(): void
    {
        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name', 'email'])
            ->filter([
                Query::startsWith('email', 'a'),
                Query::contains('name', ['Alice']),
            ])
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }

    public function testSelectWithCaseExpression(): void
    {
        $case = (new CaseExpression())
            ->when('age', Operator::LessThan, 30, 'young')
            ->when('age', Operator::LessThan, 35, 'mid')
            ->else('senior')
            ->alias('bucket');

        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name'])
            ->selectCase($case)
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(5, $rows);
        $buckets = array_column($rows, 'bucket');
        $this->assertContains('young', $buckets);
        $this->assertContains('mid', $buckets);
        $this->assertContains('senior', $buckets);
    }

    public function testSelectWithArrayJoin(): void
    {
        $this->trackClickhouseTable('ch_tags');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_tags`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_tags` (
                `id` UInt32,
                `name` String,
                `tags` Array(String)
            ) ENGINE = MergeTree()
            ORDER BY `id`
        ');
        $this->clickhouseStatement("
            INSERT INTO `ch_tags` (`id`, `name`, `tags`) VALUES
            (1, 'Post A', ['news', 'sport']),
            (2, 'Post B', ['tech']),
            (3, 'Post C', ['news', 'tech', 'culture'])
        ");

        $result = (new Builder())
            ->from('ch_tags')
            ->select(['id', 'name'])
            ->arrayJoin('tags', 'tag')
            ->filter([Query::equal('tag', ['news'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(2, $rows);
        $this->assertEquals('Post A', $rows[0]['name']);
        $this->assertEquals('Post C', $rows[1]['name']);
    }

    public function testSelectWithExistsSubquery(): void
    {
        $subquery = (new Builder())
            ->from('ch_events')
            ->filter([
                Query::equal('action', ['purchase']),
                Query::equal('user_id', [1]),
            ]);

        $result = (new Builder())
            ->from('ch_users')
            ->select(['id', 'name'])
            ->filterExists($subquery)
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        // Subquery has rows, so all users are returned.
        $this->assertCount(5, $rows);
    }

    public function testAsofJoin(): void
    {
        $this->markTestSkipped(
            'Builder AsofJoins API only emits `ON left = right` (hardcoded equality). '
            . 'ClickHouse requires the ASOF JOIN ON clause to include at least one '
            . 'equi-join column AND an inequality (e.g. `ON t.symbol = q.symbol AND t.ts >= q.ts`). '
            . 'The current asofJoin() signature cannot express the inequality condition, '
            . 'so a valid ASOF query is not constructible via the builder.'
        );
    }

    public function testAsofLeftJoin(): void
    {
        $this->markTestSkipped(
            'Builder AsofJoins API only emits `ON left = right` (hardcoded equality). '
            . 'ClickHouse requires the ASOF LEFT JOIN ON clause to include at least one '
            . 'equi-join column AND an inequality (e.g. `ON t.symbol = q.symbol AND t.ts >= q.ts`). '
            . 'The current asofLeftJoin() signature cannot express the inequality condition.'
        );
    }

    public function testApproxDistinctCount(): void
    {
        $this->trackClickhouseTable('ch_approx');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_approx`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_approx` (
                `id` UInt64,
                `user_id` UInt64,
                `value` Float64
            ) ENGINE = MergeTree()
            ORDER BY `id`
        ');

        $values = [];
        for ($id = 1; $id <= 120; $id++) {
            $userId = (($id - 1) % 10) + 1;
            $value = (float) $id;
            $values[] = '(' . $id . ', ' . $userId . ', ' . $value . ')';
        }
        $this->clickhouseStatement(
            'INSERT INTO `ch_approx` (`id`, `user_id`, `value`) VALUES '
            . \implode(', ', $values)
        );

        $result = (new Builder())
            ->from('ch_approx')
            ->uniq('user_id', 'approx_distinct')
            ->uniqExact('user_id', 'exact_distinct')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(1, $rows);
        $exact = (int) $rows[0]['exact_distinct']; // @phpstan-ignore cast.int
        $approx = (int) $rows[0]['approx_distinct']; // @phpstan-ignore cast.int

        $this->assertSame(10, $exact);
        $this->assertGreaterThanOrEqual((int) \floor(10 * 0.95), $approx);
        $this->assertLessThanOrEqual((int) \ceil(10 * 1.05), $approx);
    }

    public function testApproxQuantile(): void
    {
        $this->trackClickhouseTable('ch_approx');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_approx`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_approx` (
                `id` UInt64,
                `user_id` UInt64,
                `value` Float64
            ) ENGINE = MergeTree()
            ORDER BY `id`
        ');

        $values = [];
        for ($id = 1; $id <= 101; $id++) {
            $userId = (($id - 1) % 10) + 1;
            $value = (float) $id;
            $values[] = '(' . $id . ', ' . $userId . ', ' . $value . ')';
        }
        $this->clickhouseStatement(
            'INSERT INTO `ch_approx` (`id`, `user_id`, `value`) VALUES '
            . \implode(', ', $values)
        );

        $result = (new Builder())
            ->from('ch_approx')
            ->quantile(0.5, 'value', 'p50')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(1, $rows);
        $median = (float) $rows[0]['p50']; // @phpstan-ignore cast.double

        // Values are 1..101; true median is 51. Allow +/-10% tolerance.
        $this->assertGreaterThanOrEqual(51.0 * 0.9, $median);
        $this->assertLessThanOrEqual(51.0 * 1.1, $median);
    }

    public function testApproxQuantiles(): void
    {
        $this->trackClickhouseTable('ch_approx');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_approx`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_approx` (
                `id` UInt64,
                `user_id` UInt64,
                `value` Float64
            ) ENGINE = MergeTree()
            ORDER BY `id`
        ');

        $values = [];
        for ($id = 1; $id <= 100; $id++) {
            $userId = (($id - 1) % 10) + 1;
            $value = (float) $id;
            $values[] = '(' . $id . ', ' . $userId . ', ' . $value . ')';
        }
        $this->clickhouseStatement(
            'INSERT INTO `ch_approx` (`id`, `user_id`, `value`) VALUES '
            . \implode(', ', $values)
        );

        // Builder has no `quantiles(level, level, ...)` helper; use selectRaw.
        $result = (new Builder())
            ->from('ch_approx')
            ->selectRaw('quantiles(0.25, 0.5, 0.75)(`value`) AS `qs`')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(1, $rows);
        $this->assertArrayHasKey('qs', $rows[0]);
        $qs = $rows[0]['qs'];
        $this->assertIsArray($qs);
        $this->assertCount(3, $qs);

        $q25 = (float) $qs[0]; // @phpstan-ignore cast.double
        $q50 = (float) $qs[1]; // @phpstan-ignore cast.double
        $q75 = (float) $qs[2]; // @phpstan-ignore cast.double

        $this->assertLessThanOrEqual($q50, $q25);
        $this->assertLessThanOrEqual($q75, $q50);
    }

    public function testWindowFunctionWithRowsFrame(): void
    {
        $this->trackClickhouseTable('ch_window');
        $this->clickhouseStatement('DROP TABLE IF EXISTS `ch_window`');
        $this->clickhouseStatement('
            CREATE TABLE `ch_window` (
                `id` UInt64,
                `user_id` UInt64,
                `value` Float64
            ) ENGINE = MergeTree()
            ORDER BY (`user_id`, `id`)
        ');

        $this->clickhouseStatement("
            INSERT INTO `ch_window` (`id`, `user_id`, `value`) VALUES
            (1, 1, 10.0),
            (2, 1, 20.0),
            (3, 1, 30.0),
            (4, 1, 40.0),
            (5, 1, 50.0),
            (6, 2, 100.0),
            (7, 2, 200.0),
            (8, 2, 300.0)
        ");

        $frame = new WindowFrame('ROWS', '2 PRECEDING', 'CURRENT ROW');

        $result = (new Builder())
            ->from('ch_window')
            ->select(['id', 'user_id', 'value'])
            ->selectWindow('sum(`value`)', 'rolling_sum', ['user_id'], ['id'], null, $frame)
            ->filter([Query::equal('user_id', [1])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnClickhouse($result);

        $this->assertCount(5, $rows);
        $sums = \array_map(
            static fn (array $row): float => (float) $row['rolling_sum'], // @phpstan-ignore cast.double
            $rows,
        );

        // user_id=1 values are 10,20,30,40,50 ordered by id.
        // Rolling sum (2 preceding + current):
        //   id=1 -> 10
        //   id=2 -> 10+20 = 30
        //   id=3 -> 10+20+30 = 60
        //   id=4 -> 20+30+40 = 90
        //   id=5 -> 30+40+50 = 120
        $this->assertSame(10.0, $sums[0]);
        $this->assertSame(30.0, $sums[1]);
        $this->assertSame(60.0, $sums[2]);
        $this->assertSame(90.0, $sums[3]);
        $this->assertSame(120.0, $sums[4]);
    }
}
