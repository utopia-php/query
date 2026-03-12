<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\MongoDB as Builder;
use Utopia\Query\Query;

class MongoDBIntegrationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->connectMongoDB();

        $this->trackMongoCollection('mg_users');
        $this->trackMongoCollection('mg_orders');

        $client = $this->mongoClient;
        $this->assertNotNull($client);

        $client->dropCollection('mg_users');
        $client->dropCollection('mg_orders');

        $client->insertMany('mg_users', [
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@test.com', 'age' => 30, 'country' => 'US', 'active' => true],
            ['id' => 2, 'name' => 'Bob', 'email' => 'bob@test.com', 'age' => 25, 'country' => 'UK', 'active' => true],
            ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@test.com', 'age' => 35, 'country' => 'US', 'active' => false],
            ['id' => 4, 'name' => 'Diana', 'email' => 'diana@test.com', 'age' => 28, 'country' => 'DE', 'active' => true],
            ['id' => 5, 'name' => 'Eve', 'email' => 'eve@test.com', 'age' => 22, 'country' => 'UK', 'active' => true],
        ]);

        $client->insertMany('mg_orders', [
            ['id' => 1, 'user_id' => 1, 'product' => 'Widget', 'amount' => 29.99, 'status' => 'completed'],
            ['id' => 2, 'user_id' => 1, 'product' => 'Gadget', 'amount' => 49.99, 'status' => 'completed'],
            ['id' => 3, 'user_id' => 2, 'product' => 'Widget', 'amount' => 29.99, 'status' => 'pending'],
            ['id' => 4, 'user_id' => 3, 'product' => 'Gizmo', 'amount' => 99.99, 'status' => 'completed'],
            ['id' => 5, 'user_id' => 4, 'product' => 'Widget', 'amount' => 29.99, 'status' => 'cancelled'],
            ['id' => 6, 'user_id' => 4, 'product' => 'Gadget', 'amount' => 49.99, 'status' => 'pending'],
            ['id' => 7, 'user_id' => 5, 'product' => 'Gizmo', 'amount' => 99.99, 'status' => 'completed'],
        ]);
    }

    public function testSelectWithWhere(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['id', 'name', 'country'])
            ->filter([Query::equal('country', ['US'])])
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(2, $rows);
        $names = \array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    public function testSelectWithOrderByAndLimit(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['id', 'name', 'age'])
            ->sortDesc('age')
            ->limit(3)
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(3, $rows);
        $this->assertEquals('Charlie', $rows[0]['name']);
        $this->assertEquals('Alice', $rows[1]['name']);
        $this->assertEquals('Diana', $rows[2]['name']);
    }

    public function testSelectWithJoin(): void
    {
        $result = (new Builder())
            ->from('mg_orders')
            ->select(['id', 'product', 'u.name'])
            ->join('mg_users', 'mg_orders.user_id', 'mg_users.id', '=', 'u')
            ->filter([Query::equal('status', ['completed'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(4, $rows);
        /** @var array<string, mixed> $joined */
        $joined = $rows[0]['u'];
        $this->assertEquals('Alice', $joined['name']);
    }

    public function testSelectWithLeftJoin(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->leftJoin('mg_orders', 'mg_users.id', 'mg_orders.user_id', '=', 'o')
            ->filter([Query::equal('o.status', ['cancelled'])])
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Diana', $rows[0]['name']);
    }

    public function testInsertSingleRow(): void
    {
        $insert = (new Builder())
            ->into('mg_users')
            ->set(['id' => 10, 'name' => 'Frank', 'email' => 'frank@test.com', 'age' => 40, 'country' => 'FR', 'active' => true])
            ->insert();

        $this->executeOnMongoDB($insert);

        $select = (new Builder())
            ->from('mg_users')
            ->select(['id', 'name'])
            ->filter([Query::equal('id', [10])])
            ->build();

        $rows = $this->executeOnMongoDB($select);

        $this->assertCount(1, $rows);
        $this->assertEquals('Frank', $rows[0]['name']);
    }

    public function testInsertMultipleRows(): void
    {
        $insert = (new Builder())
            ->into('mg_users')
            ->set(['id' => 10, 'name' => 'Frank', 'email' => 'frank@test.com', 'age' => 40, 'country' => 'FR', 'active' => true])
            ->set(['id' => 11, 'name' => 'Grace', 'email' => 'grace@test.com', 'age' => 33, 'country' => 'FR', 'active' => true])
            ->insert();

        $this->executeOnMongoDB($insert);

        $select = (new Builder())
            ->from('mg_users')
            ->select(['id', 'name'])
            ->filter([Query::equal('country', ['FR'])])
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnMongoDB($select);

        $this->assertCount(2, $rows);
        $this->assertEquals('Frank', $rows[0]['name']);
        $this->assertEquals('Grace', $rows[1]['name']);
    }

    public function testUpdateWithWhere(): void
    {
        $update = (new Builder())
            ->from('mg_users')
            ->set(['country' => 'CA'])
            ->filter([Query::equal('name', ['Alice'])])
            ->update();

        $this->executeOnMongoDB($update);

        $select = (new Builder())
            ->from('mg_users')
            ->select(['country'])
            ->filter([Query::equal('name', ['Alice'])])
            ->build();

        $rows = $this->executeOnMongoDB($select);

        $this->assertCount(1, $rows);
        $this->assertEquals('CA', $rows[0]['country']);
    }

    public function testDeleteWithWhere(): void
    {
        $delete = (new Builder())
            ->from('mg_users')
            ->filter([Query::equal('name', ['Eve'])])
            ->delete();

        $this->executeOnMongoDB($delete);

        $select = (new Builder())
            ->from('mg_users')
            ->filter([Query::equal('name', ['Eve'])])
            ->build();

        $rows = $this->executeOnMongoDB($select);

        $this->assertCount(0, $rows);
    }

    public function testSelectWithGroupByAndHaving(): void
    {
        $result = (new Builder())
            ->from('mg_orders')
            ->select(['status'])
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $statuses = \array_column($rows, 'status');
        $this->assertContains('completed', $statuses);
        foreach ($rows as $row) {
            /** @var int $cnt */
            $cnt = $row['cnt'];
            $this->assertGreaterThan(1, $cnt);
        }
    }

    public function testSelectWithUnionAll(): void
    {
        $first = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->filter([Query::equal('country', ['US'])]);

        $second = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->filter([Query::equal('country', ['UK'])]);

        $result = $first->unionAll($second)->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(4, $rows);
        $names = \array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Eve', $names);
    }

    public function testSelectWithDistinct(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['country'])
            ->distinct()
            ->sortAsc('country')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(3, $rows);
        $countries = \array_column($rows, 'country');
        $this->assertEquals(['DE', 'UK', 'US'], $countries);
    }

    public function testSelectWithSubqueryInWhere(): void
    {
        $subquery = (new Builder())
            ->from('mg_orders')
            ->select(['user_id'])
            ->filter([Query::equal('status', ['completed'])]);

        $result = (new Builder())
            ->from('mg_users')
            ->select(['id', 'name'])
            ->filterWhereIn('id', $subquery)
            ->sortAsc('id')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(3, $rows);
        $names = \array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Eve', $names);
    }

    public function testUpsertOnConflict(): void
    {
        $result = (new Builder())
            ->into('mg_users')
            ->set(['email' => 'alice@test.com', 'name' => 'Alice Updated', 'age' => 31, 'country' => 'US', 'active' => true])
            ->onConflict(['email'], ['name', 'age'])
            ->upsert();

        $this->executeOnMongoDB($result);

        $check = (new Builder())
            ->from('mg_users')
            ->select(['name', 'age'])
            ->filter([Query::equal('email', ['alice@test.com'])])
            ->build();

        $rows = $this->executeOnMongoDB($check);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice Updated', $rows[0]['name']);
        $this->assertEquals(31, $rows[0]['age']);
    }

    public function testSelectWithWindowFunction(): void
    {
        $result = (new Builder())
            ->from('mg_orders')
            ->select(['user_id', 'product', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-amount'])
            ->sortAsc('user_id')
            ->sortDesc('amount')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertGreaterThan(0, \count($rows));
        $this->assertArrayHasKey('rn', $rows[0]);

        // Check first user's rows are numbered
        $user1Rows = \array_values(\array_filter($rows, fn ($r) => $r['user_id'] === 1));
        $this->assertEquals(1, $user1Rows[0]['rn']);
        $this->assertEquals(2, $user1Rows[1]['rn']);
    }

    public function testFilterStartsWith(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->filter([Query::startsWith('name', 'Al')])
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }

    public function testFilterContains(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->filter([Query::contains('email', ['test.com'])])
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(5, $rows);
    }

    public function testFilterBetween(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name', 'age'])
            ->filter([Query::between('age', 25, 30)])
            ->sortAsc('age')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(3, $rows);
        foreach ($rows as $row) {
            $this->assertGreaterThanOrEqual(25, $row['age']);
            $this->assertLessThanOrEqual(30, $row['age']);
        }
    }

    public function testSelectWithOffset(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->sortAsc('name')
            ->limit(2)
            ->offset(1)
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(2, $rows);
        $this->assertEquals('Bob', $rows[0]['name']);
        $this->assertEquals('Charlie', $rows[1]['name']);
    }

    public function testFilterRegex(): void
    {
        $result = (new Builder())
            ->from('mg_users')
            ->select(['name'])
            ->filter([Query::regex('name', '^[A-C]')])
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertCount(3, $rows);
        $names = \array_column($rows, 'name');
        $this->assertEquals(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testAggregateSum(): void
    {
        $result = (new Builder())
            ->from('mg_orders')
            ->sum('amount', 'total')
            ->groupBy(['user_id'])
            ->sortAsc('user_id')
            ->build();

        $rows = $this->executeOnMongoDB($result);

        $this->assertGreaterThan(0, \count($rows));
        // User 1 has orders of 29.99 + 49.99 = 79.98
        $user1 = \array_values(\array_filter($rows, fn ($r) => $r['user_id'] === 1))[0];
        $this->assertEqualsWithDelta(79.98, $user1['total'], 0.01);
    }
}
