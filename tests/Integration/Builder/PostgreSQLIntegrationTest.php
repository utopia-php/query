<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\PostgreSQL as Builder;
use Utopia\Query\Query;

class PostgreSQLIntegrationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->trackPostgresTable('users');
        $this->trackPostgresTable('orders');

        $this->postgresStatement('DROP TABLE IF EXISTS "orders" CASCADE');
        $this->postgresStatement('DROP TABLE IF EXISTS "users" CASCADE');

        $this->postgresStatement('
            CREATE TABLE "users" (
                "id" SERIAL PRIMARY KEY,
                "name" VARCHAR(255) NOT NULL,
                "email" VARCHAR(255) NOT NULL UNIQUE,
                "age" INT NOT NULL DEFAULT 0,
                "city" VARCHAR(100) DEFAULT NULL,
                "active" BOOLEAN NOT NULL DEFAULT TRUE,
                "created_at" TIMESTAMP NOT NULL DEFAULT NOW()
            )
        ');

        $this->postgresStatement('
            CREATE TABLE "orders" (
                "id" SERIAL PRIMARY KEY,
                "user_id" INT NOT NULL REFERENCES "users"("id"),
                "product" VARCHAR(255) NOT NULL,
                "amount" DECIMAL(10,2) NOT NULL,
                "status" VARCHAR(50) NOT NULL DEFAULT \'pending\',
                "created_at" TIMESTAMP NOT NULL DEFAULT NOW()
            )
        ');

        $this->postgresStatement("
            INSERT INTO \"users\" (\"name\", \"email\", \"age\", \"city\", \"active\") VALUES
            ('Alice', 'alice@example.com', 30, 'New York', TRUE),
            ('Bob', 'bob@example.com', 25, 'London', TRUE),
            ('Charlie', 'charlie@example.com', 35, 'New York', FALSE),
            ('Diana', 'diana@example.com', 28, 'Paris', TRUE),
            ('Eve', 'eve@example.com', 22, 'London', TRUE)
        ");

        $this->postgresStatement("
            INSERT INTO \"orders\" (\"user_id\", \"product\", \"amount\", \"status\") VALUES
            (1, 'Widget', 29.99, 'completed'),
            (1, 'Gadget', 49.99, 'completed'),
            (2, 'Widget', 29.99, 'pending'),
            (3, 'Gizmo', 99.99, 'completed'),
            (4, 'Widget', 29.99, 'cancelled'),
            (4, 'Gadget', 49.99, 'pending'),
            (5, 'Gizmo', 99.99, 'completed')
        ");
    }

    public function testSelectWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('city', ['New York'])])
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(2, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Charlie', $rows[1]['name']);
    }

    public function testSelectWithOrderByLimitOffset(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->sortAsc('name')
            ->limit(2)
            ->offset(1)
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(2, $rows);
        $this->assertEquals('Bob', $rows[0]['name']);
        $this->assertEquals('Charlie', $rows[1]['name']);
    }

    public function testSelectWithJoin(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->select(['u.name', 'o.product', 'o.amount'])
            ->join('orders', 'u.id', 'o.user_id', '=', 'o')
            ->filter([Query::equal('o.status', ['completed'])])
            ->sortAsc('u.name')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(4, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Widget', $rows[0]['product']);
    }

    public function testSelectWithLeftJoin(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->select(['u.name', 'o.product'])
            ->leftJoin('orders', 'u.id', 'o.user_id', '=', 'o')
            ->filter([Query::equal('o.status', ['cancelled'])])
            ->sortAsc('u.name')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Diana', $rows[0]['name']);
    }

    public function testInsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Frank', 'email' => 'frank@example.com', 'age' => 40, 'city' => 'Berlin', 'active' => true])
            ->insert();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->select(['name', 'city'])
            ->filter([Query::equal('email', ['frank@example.com'])])
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(1, $rows);
        $this->assertEquals('Frank', $rows[0]['name']);
        $this->assertEquals('Berlin', $rows[0]['city']);
    }

    public function testInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Grace', 'email' => 'grace@example.com', 'age' => 33, 'city' => 'Tokyo', 'active' => true])
            ->set(['name' => 'Hank', 'email' => 'hank@example.com', 'age' => 45, 'city' => 'Tokyo', 'active' => false])
            ->insert();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('city', ['Tokyo'])])
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(2, $rows);
        $this->assertEquals('Grace', $rows[0]['name']);
        $this->assertEquals('Hank', $rows[1]['name']);
    }

    public function testInsertWithReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Ivy', 'email' => 'ivy@example.com', 'age' => 27, 'city' => 'Madrid', 'active' => true])
            ->returning(['id', 'name', 'email'])
            ->insert();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Ivy', $rows[0]['name']);
        $this->assertEquals('ivy@example.com', $rows[0]['email']);
        $this->assertArrayHasKey('id', $rows[0]);
        $this->assertGreaterThan(0, (int) $rows[0]['id']); // @phpstan-ignore cast.int
    }

    public function testUpdateWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['city' => 'San Francisco'])
            ->filter([Query::equal('name', ['Alice'])])
            ->update();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->select(['city'])
            ->filter([Query::equal('name', ['Alice'])])
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(1, $rows);
        $this->assertEquals('San Francisco', $rows[0]['city']);
    }

    public function testUpdateWithReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['age' => 31])
            ->filter([Query::equal('name', ['Alice'])])
            ->returning(['name', 'age'])
            ->update();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals(31, (int) $rows[0]['age']); // @phpstan-ignore cast.int
    }

    public function testDeleteWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ['Eve'])])
            ->delete();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ['Eve'])])
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(0, $rows);
    }

    public function testDeleteWithReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ['Charlie'])])
            ->returning(['id', 'name'])
            ->delete();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Charlie', $rows[0]['name']);
    }

    public function testSelectWithGroupByAndHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->count('*', 'order_count')
            ->groupBy(['user_id'])
            ->having([Query::greaterThan('order_count', 1)])
            ->sortAsc('user_id')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['user_id']); // @phpstan-ignore cast.int
        $this->assertEquals(2, (int) $rows[0]['order_count']); // @phpstan-ignore cast.int
        $this->assertEquals(4, (int) $rows[1]['user_id']); // @phpstan-ignore cast.int
        $this->assertEquals(2, (int) $rows[1]['order_count']); // @phpstan-ignore cast.int
    }

    public function testSelectWithUnion(): void
    {
        $query1 = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('city', ['New York'])]);

        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('city', ['London'])])
            ->union($query1)
            ->build();

        $rows = $this->executeOnPostgres($result);

        $names = array_column($rows, 'name');
        sort($names);

        $this->assertCount(4, $rows);
        $this->assertEquals(['Alice', 'Bob', 'Charlie', 'Eve'], $names);
    }

    public function testUpsertOnConflictDoUpdate(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice Updated', 'email' => 'alice@example.com', 'age' => 31, 'city' => 'Boston', 'active' => true])
            ->onConflict(['email'], ['name', 'age', 'city'])
            ->upsert();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->select(['name', 'age', 'city'])
            ->filter([Query::equal('email', ['alice@example.com'])])
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice Updated', $rows[0]['name']);
        $this->assertEquals(31, (int) $rows[0]['age']); // @phpstan-ignore cast.int
        $this->assertEquals('Boston', $rows[0]['city']);
    }

    public function testInsertOrIgnoreOnConflictDoNothing(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice Duplicate', 'email' => 'alice@example.com', 'age' => 99, 'city' => 'Nowhere', 'active' => false])
            ->insertOrIgnore();

        $this->executeOnPostgres($result);

        $check = (new Builder())
            ->from('users')
            ->select(['name', 'age'])
            ->filter([Query::equal('email', ['alice@example.com'])])
            ->build();

        $rows = $this->executeOnPostgres($check);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals(30, (int) $rows[0]['age']); // @phpstan-ignore cast.int
    }

    public function testSelectWithCte(): void
    {
        $cteQuery = (new Builder())
            ->from('users')
            ->select(['id', 'name', 'city'])
            ->filter([Query::equal('active', [true])]);

        $result = (new Builder())
            ->with('active_users', $cteQuery)
            ->from('active_users')
            ->select(['name', 'city'])
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(4, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Bob', $rows[1]['name']);
        $this->assertEquals('Diana', $rows[2]['name']);
        $this->assertEquals('Eve', $rows[3]['name']);
    }

    public function testSelectWithWindowFunction(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id', 'product', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-amount'])
            ->sortAsc('user_id')
            ->sortDesc('amount')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertGreaterThan(0, count($rows));
        $this->assertArrayHasKey('rn', $rows[0]);

        $user1Rows = array_filter($rows, fn ($r) => (int) $r['user_id'] === 1); // @phpstan-ignore cast.int
        $user1Rows = array_values($user1Rows);
        $this->assertEquals(1, (int) $user1Rows[0]['rn']); // @phpstan-ignore cast.int
        $this->assertEquals(2, (int) $user1Rows[1]['rn']); // @phpstan-ignore cast.int
    }

    public function testSelectWithDistinct(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['product'])
            ->distinct()
            ->sortAsc('product')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $this->assertCount(3, $rows);
        $products = array_column($rows, 'product');
        $this->assertEquals(['Gadget', 'Gizmo', 'Widget'], $products);
    }

    public function testSelectWithSubqueryInWhere(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::equal('status', ['completed'])]);

        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filterWhereIn('id', $subquery)
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnPostgres($result);

        $names = array_column($rows, 'name');

        $this->assertCount(3, $rows);
        $this->assertEquals(['Alice', 'Charlie', 'Eve'], $names);
    }

    public function testSelectForUpdate(): void
    {
        $pdo = $this->connectPostgres();
        $pdo->beginTransaction();

        try {
            $result = (new Builder())
                ->from('users')
                ->select(['id', 'name'])
                ->filter([Query::equal('name', ['Alice'])])
                ->forUpdate()
                ->build();

            $rows = $this->executeOnPostgres($result);

            $this->assertCount(1, $rows);
            $this->assertEquals('Alice', $rows[0]['name']);
        } finally {
            $pdo->rollBack();
        }
    }
}
