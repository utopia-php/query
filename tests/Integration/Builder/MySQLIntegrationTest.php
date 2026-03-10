<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\Case\Builder as CaseBuilder;
use Utopia\Query\Builder\MySQL as Builder;
use Utopia\Query\Query;

class MySQLIntegrationTest extends IntegrationTestCase
{
    private Builder $builder;

    protected function setUp(): void
    {
        parent::setUp();

        $this->builder = new Builder();
        $pdo = $this->connectMysql();

        $this->trackMysqlTable('users');
        $this->trackMysqlTable('orders');

        $this->mysqlStatement('DROP TABLE IF EXISTS `orders`');
        $this->mysqlStatement('DROP TABLE IF EXISTS `users`');

        $this->mysqlStatement('
            CREATE TABLE `users` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(100) NOT NULL,
                `email` VARCHAR(150) NOT NULL UNIQUE,
                `age` INT NOT NULL DEFAULT 0,
                `city` VARCHAR(100) NOT NULL DEFAULT \'\',
                `active` TINYINT(1) NOT NULL DEFAULT 1,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        ');

        $this->mysqlStatement('
            CREATE TABLE `orders` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `user_id` INT NOT NULL,
                `product` VARCHAR(100) NOT NULL,
                `amount` DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                `status` VARCHAR(20) NOT NULL DEFAULT \'pending\',
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
            ) ENGINE=InnoDB
        ');

        $stmt = $pdo->prepare('
            INSERT INTO `users` (`name`, `email`, `age`, `city`, `active`) VALUES
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?)
        ');
        $stmt->execute([
            'Alice', 'alice@example.com', 30, 'New York', 1,
            'Bob', 'bob@example.com', 25, 'London', 1,
            'Charlie', 'charlie@example.com', 35, 'New York', 0,
            'Diana', 'diana@example.com', 28, 'Paris', 1,
            'Eve', 'eve@example.com', 22, 'London', 1,
        ]);

        $stmt = $pdo->prepare('
            INSERT INTO `orders` (`user_id`, `product`, `amount`, `status`) VALUES
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?),
            (?, ?, ?, ?)
        ');
        $stmt->execute([
            1, 'Widget', 29.99, 'completed',
            1, 'Gadget', 49.99, 'completed',
            2, 'Widget', 29.99, 'pending',
            3, 'Gizmo', 19.99, 'completed',
            4, 'Widget', 29.99, 'cancelled',
            4, 'Gadget', 49.99, 'pending',
        ]);
    }

    private function fresh(): Builder
    {
        return $this->builder->reset();
    }

    public function testSelectWithWhere(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('city', ['New York'])])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    public function testSelectWithOrderByAndLimit(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name', 'age'])
            ->sortDesc('age')
            ->limit(3)
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(3, $rows);
        $this->assertEquals('Charlie', $rows[0]['name']);
        $this->assertEquals('Alice', $rows[1]['name']);
        $this->assertEquals('Diana', $rows[2]['name']);
    }

    public function testSelectWithJoin(): void
    {
        $result = $this->fresh()
            ->from('users', 'u')
            ->select(['u.name', 'o.product', 'o.amount'])
            ->join('orders', 'u.id', 'o.user_id', '=', 'o')
            ->filter([Query::equal('o.status', ['completed'])])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(3, $rows);
        $products = array_column($rows, 'product');
        $this->assertContains('Widget', $products);
        $this->assertContains('Gadget', $products);
        $this->assertContains('Gizmo', $products);
    }

    public function testSelectWithLeftJoin(): void
    {
        $result = $this->fresh()
            ->from('users', 'u')
            ->select(['u.name', 'o.product'])
            ->leftJoin('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertNotEmpty($rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Eve', $names);
    }

    public function testInsertSingleRow(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Frank', 'email' => 'frank@example.com', 'age' => 40, 'city' => 'Berlin', 'active' => 1])
            ->insert();

        $this->executeOnMysql($result);

        $rows = $this->executeOnMysql(
            $this->fresh()
                ->from('users')
                ->select(['name'])
                ->filter([Query::equal('email', ['frank@example.com'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertEquals('Frank', $rows[0]['name']);
    }

    public function testInsertMultipleRows(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Grace', 'email' => 'grace@example.com', 'age' => 33, 'city' => 'Tokyo', 'active' => 1])
            ->set(['name' => 'Hank', 'email' => 'hank@example.com', 'age' => 45, 'city' => 'Tokyo', 'active' => 0])
            ->insert();

        $this->executeOnMysql($result);

        $rows = $this->executeOnMysql(
            $this->fresh()
                ->from('users')
                ->select(['name'])
                ->filter([Query::equal('city', ['Tokyo'])])
                ->build()
        );

        $this->assertCount(2, $rows);
    }

    public function testUpdateWithWhere(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->set(['active' => 0])
            ->filter([Query::equal('name', ['Bob'])])
            ->update();

        $this->executeOnMysql($result);

        $rows = $this->executeOnMysql(
            $this->fresh()
                ->from('users')
                ->select(['active'])
                ->filter([Query::equal('name', ['Bob'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(0, $rows[0]['active']);
    }

    public function testDeleteWithWhere(): void
    {
        $this->mysqlStatement('DELETE FROM `orders` WHERE `user_id` = 3');

        $result = $this->fresh()
            ->from('users')
            ->filter([Query::equal('name', ['Charlie'])])
            ->delete();

        $this->executeOnMysql($result);

        $rows = $this->executeOnMysql(
            $this->fresh()
                ->from('users')
                ->select(['name'])
                ->filter([Query::equal('name', ['Charlie'])])
                ->build()
        );

        $this->assertCount(0, $rows);
    }

    public function testSelectWithGroupByAndHaving(): void
    {
        $result = $this->fresh()
            ->from('orders')
            ->select(['user_id'])
            ->count('*', 'order_count')
            ->groupBy(['user_id'])
            ->having([Query::greaterThan('order_count', 1)])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(2, $rows);
        foreach ($rows as $row) {
            $this->assertGreaterThan(1, (int) $row['order_count']); // @phpstan-ignore cast.int
        }
    }

    public function testSelectWithUnion(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('city', ['New York'])])
            ->union(
                (new Builder())
                    ->from('users')
                    ->select(['name'])
                    ->filter([Query::equal('city', ['London'])])
            )
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(4, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Eve', $names);
    }

    public function testSelectWithCaseExpression(): void
    {
        $case = (new CaseBuilder())
            ->when('`age` < 25', "'young'")
            ->when('`age` BETWEEN 25 AND 30', "'mid'")
            ->elseResult("'senior'")
            ->alias('`age_group`')
            ->build();

        $result = $this->fresh()
            ->from('users')
            ->select(['name'])
            ->selectCase($case)
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(5, $rows);
        $map = array_column($rows, 'age_group', 'name');
        $this->assertEquals('mid', $map['Alice']);
        $this->assertEquals('mid', $map['Bob']);
        $this->assertEquals('senior', $map['Charlie']);
        $this->assertEquals('mid', $map['Diana']);
        $this->assertEquals('young', $map['Eve']);
    }

    public function testSelectWithWhereInSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::equal('status', ['completed'])]);

        $result = $this->fresh()
            ->from('users')
            ->select(['name'])
            ->filterWhereIn('id', $subquery)
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    public function testSelectWithExistsSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders', 'o')
            ->selectRaw('1')
            ->filter([Query::equal('o.status', ['completed'])]);

        $result = $this->fresh()
            ->from('users', 'u')
            ->select(['u.name'])
            ->filterExists($subquery)
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(5, $rows);

        $noMatchSubquery = (new Builder())
            ->from('orders', 'o')
            ->selectRaw('1')
            ->filter([Query::equal('o.status', ['refunded'])]);

        $emptyResult = $this->fresh()
            ->from('users', 'u')
            ->select(['u.name'])
            ->filterExists($noMatchSubquery)
            ->build();

        $emptyRows = $this->executeOnMysql($emptyResult);

        $this->assertCount(0, $emptyRows);
    }

    public function testSelectWithCte(): void
    {
        $cteQuery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->sum('amount', 'total')
            ->groupBy(['user_id']);

        $result = $this->fresh()
            ->with('user_totals', $cteQuery)
            ->from('user_totals')
            ->select(['user_id', 'total'])
            ->filter([Query::greaterThan('total', 30)])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertNotEmpty($rows);
        foreach ($rows as $row) {
            $this->assertGreaterThan(30, (float) $row['total']); // @phpstan-ignore cast.double
        }
    }

    public function testUpsertOnDuplicateKeyUpdate(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@example.com', 'age' => 31, 'city' => 'New York', 'active' => 1])
            ->onConflict(['email'], ['age'])
            ->upsert();

        $this->executeOnMysql($result);

        $rows = $this->executeOnMysql(
            $this->fresh()
                ->from('users')
                ->select(['age'])
                ->filter([Query::equal('email', ['alice@example.com'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(31, (int) $rows[0]['age']); // @phpstan-ignore cast.int
    }

    public function testSelectWithWindowFunction(): void
    {
        $result = $this->fresh()
            ->from('orders')
            ->select(['user_id', 'product', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-amount'])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertNotEmpty($rows);
        foreach ($rows as $row) {
            $this->assertArrayHasKey('rn', $row);
            $this->assertGreaterThanOrEqual(1, (int) $row['rn']); // @phpstan-ignore cast.int
        }
    }

    public function testSelectWithDistinct(): void
    {
        $result = $this->fresh()
            ->from('orders')
            ->select(['product'])
            ->distinct()
            ->sortAsc('product')
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(3, $rows);
        $products = array_column($rows, 'product');
        $this->assertEquals(['Gadget', 'Gizmo', 'Widget'], $products);
    }

    public function testSelectWithBetween(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name', 'age'])
            ->filter([Query::between('age', 25, 30)])
            ->sortAsc('name')
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(3, $rows);
        foreach ($rows as $row) {
            $this->assertGreaterThanOrEqual(25, (int) $row['age']); // @phpstan-ignore cast.int
            $this->assertLessThanOrEqual(30, (int) $row['age']); // @phpstan-ignore cast.int
        }
    }

    public function testSelectWithStartsWith(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::startsWith('name', 'Al')])
            ->build();

        $rows = $this->executeOnMysql($result);

        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }

    public function testSelectForUpdate(): void
    {
        $pdo = $this->connectMysql();
        $pdo->beginTransaction();

        try {
            $result = $this->fresh()
                ->from('users')
                ->select(['name', 'age'])
                ->filter([Query::equal('name', ['Alice'])])
                ->forUpdate()
                ->build();

            $rows = $this->executeOnMysql($result);

            $this->assertCount(1, $rows);
            $this->assertEquals('Alice', $rows[0]['name']);

            $pdo->commit();
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }
    }
}
