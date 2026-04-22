<?php

namespace Tests\Integration\Builder;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Builder\MariaDB as Builder;
use Utopia\Query\Query;

class MariaDBIntegrationTest extends IntegrationTestCase
{
    private Builder $builder;

    protected function setUp(): void
    {
        parent::setUp();

        $this->builder = new Builder();
        $pdo = $this->connectMariadb();

        $this->trackMariadbTable('users');

        $this->mariadbStatement('DROP TABLE IF EXISTS `users`');

        $this->mariadbStatement('
            CREATE TABLE `users` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(100) NOT NULL,
                `email` VARCHAR(150) NOT NULL UNIQUE,
                `age` INT NOT NULL DEFAULT 0,
                `city` VARCHAR(100) NOT NULL DEFAULT \'\',
                `active` TINYINT(1) NOT NULL DEFAULT 1
            ) ENGINE=InnoDB
        ');

        $stmt = $pdo->prepare('
            INSERT INTO `users` (`name`, `email`, `age`, `city`, `active`) VALUES
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?)
        ');
        $stmt->execute([
            'Alice', 'alice@example.com', 30, 'New York', 1,
            'Bob', 'bob@example.com', 25, 'London', 1,
            'Charlie', 'charlie@example.com', 35, 'New York', 0,
        ]);
    }

    private function fresh(): Builder
    {
        return $this->builder->reset();
    }

    public function testInsertSingleRow(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Frank', 'email' => 'frank@example.com', 'age' => 40, 'city' => 'Berlin', 'active' => 1])
            ->insert();

        $this->executeOnMariadb($result);

        $rows = $this->executeOnMariadb(
            $this->fresh()
                ->from('users')
                ->select(['name'])
                ->filter([Query::equal('email', ['frank@example.com'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    public function testSelectWithWhere(): void
    {
        $result = $this->fresh()
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('city', ['New York'])])
            ->build();

        $rows = $this->executeOnMariadb($result);

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    public function testInsertWithReturning(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Gina', 'email' => 'gina@example.com', 'age' => 27, 'city' => 'Madrid', 'active' => 1])
            ->returning(['id', 'name'])
            ->insert();

        $rows = $this->executeOnMariadb($result);

        $this->assertCount(1, $rows);
        $this->assertSame('Gina', $rows[0]['name']);
        $this->assertArrayHasKey('id', $rows[0]);
        $this->assertGreaterThan(0, (int) $rows[0]['id']); // @phpstan-ignore cast.int

        $verify = $this->executeOnMariadb(
            $this->fresh()
                ->from('users')
                ->select(['name', 'email'])
                ->filter([Query::equal('id', [(int) $rows[0]['id']])]) // @phpstan-ignore cast.int
                ->build()
        );

        $this->assertCount(1, $verify);
        $this->assertSame('gina@example.com', $verify[0]['email']);
    }

    public function testSequences(): void
    {
        $this->trackMariadbTable('seq_user_id');

        $this->mariadbStatement('DROP SEQUENCE IF EXISTS `seq_user_id`');
        $this->mariadbStatement('CREATE SEQUENCE `seq_user_id` START WITH 1000 INCREMENT BY 1');

        $source = (new Builder())
            ->fromNone()
            ->selectRaw('NEXTVAL(`seq_user_id`)')
            ->selectRaw('?', ['SeqUser'])
            ->selectRaw('?', ['seq@example.com'])
            ->selectRaw('?', [21])
            ->selectRaw('?', ['Lisbon'])
            ->selectRaw('?', [1]);

        $result = $this->fresh()
            ->into('users')
            ->fromSelect(['id', 'name', 'email', 'age', 'city', 'active'], $source)
            ->insertSelect();

        $this->executeOnMariadb($result);

        $rows = $this->executeOnMariadb(
            $this->fresh()
                ->from('users')
                ->select(['id'])
                ->filter([Query::equal('email', ['seq@example.com'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertSame(1000, (int) $rows[0]['id']); // @phpstan-ignore cast.int

        $pdo = $this->connectMariadb();
        $stmt = $pdo->query('SELECT NEXTVAL(`seq_user_id`) AS v');
        $this->assertNotFalse($stmt);
        /** @var array<string, mixed> $next */
        $next = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertSame(1001, (int) $next['v']); // @phpstan-ignore cast.int
    }

    public function testUpsertOnDuplicateKeyUpdate(): void
    {
        $result = $this->fresh()
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@example.com', 'age' => 31, 'city' => 'New York', 'active' => 1])
            ->onConflict(['email'], ['age'])
            ->upsert();

        $this->executeOnMariadb($result);

        $rows = $this->executeOnMariadb(
            $this->fresh()
                ->from('users')
                ->select(['age'])
                ->filter([Query::equal('email', ['alice@example.com'])])
                ->build()
        );

        $this->assertCount(1, $rows);
        $this->assertSame(31, (int) $rows[0]['age']); // @phpstan-ignore cast.int
    }
}
