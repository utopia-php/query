<?php

namespace Tests\Integration\Schema;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\MySQL;

class MySQLIntegrationTest extends IntegrationTestCase
{
    private MySQL $schema;

    protected function setUp(): void
    {
        parent::setUp();
        $this->schema = new MySQL();
    }

    public function testCreateTableWithBasicColumns(): void
    {
        $table = 'test_basic_' . uniqid();
        $this->trackMysqlTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('age');
            $bp->string('name', 100);
            $bp->boolean('active');
        });

        $this->mysqlStatement($result->query);

        $columns = $this->fetchMysqlColumns($table);
        $columnNames = array_column($columns, 'COLUMN_NAME');

        $this->assertContains('age', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('active', $columnNames);

        $nameCol = $this->findColumn($columns, 'name');
        $this->assertSame('varchar', $nameCol['DATA_TYPE']);
        $this->assertSame('100', (string) $nameCol['CHARACTER_MAXIMUM_LENGTH']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithPrimaryKeyAndUnique(): void
    {
        $table = 'test_pk_uniq_' . uniqid();
        $this->trackMysqlTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('email', 255)->unique();
        });

        $this->mysqlStatement($result->query);

        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ?"
        );
        \assert($stmt !== false);
        $stmt->execute([$table]);
        /** @var list<array<string, mixed>> $rows */
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $idRow = $this->findColumn($rows, 'id');
        $this->assertSame('PRI', $idRow['COLUMN_KEY']);

        $emailRow = $this->findColumn($rows, 'email');
        $this->assertSame('UNI', $emailRow['COLUMN_KEY']);
    }

    public function testCreateTableWithAutoIncrement(): void
    {
        $table = 'test_autoinc_' . uniqid();
        $this->trackMysqlTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->id();
            $bp->string('label', 50);
        });

        $this->mysqlStatement($result->query);

        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT EXTRA FROM information_schema.COLUMNS "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ? AND COLUMN_NAME = 'id'"
        );
        \assert($stmt !== false);
        $stmt->execute([$table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertStringContainsString('auto_increment', (string) $row['EXTRA']); // @phpstan-ignore cast.string
    }

    public function testAlterTableAddColumn(): void
    {
        $table = 'test_alter_add_' . uniqid();
        $this->trackMysqlTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->mysqlStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->addColumn('description', ColumnType::Text);
        });
        $this->mysqlStatement($alter->query);

        $columns = $this->fetchMysqlColumns($table);
        $columnNames = array_column($columns, 'COLUMN_NAME');

        $this->assertContains('description', $columnNames);
    }

    public function testAlterTableDropColumn(): void
    {
        $table = 'test_alter_drop_' . uniqid();
        $this->trackMysqlTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('temp', 100);
        });
        $this->mysqlStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->dropColumn('temp');
        });
        $this->mysqlStatement($alter->query);

        $columns = $this->fetchMysqlColumns($table);
        $columnNames = array_column($columns, 'COLUMN_NAME');

        $this->assertNotContains('temp', $columnNames);
    }

    public function testAlterTableAddIndex(): void
    {
        $table = 'test_alter_idx_' . uniqid();
        $this->trackMysqlTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('email', 255);
        });
        $this->mysqlStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->addIndex('idx_email', ['email']);
        });
        $this->mysqlStatement($alter->query);

        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT INDEX_NAME FROM information_schema.STATISTICS "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ? AND INDEX_NAME = 'idx_email'"
        );
        \assert($stmt !== false);
        $stmt->execute([$table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        $this->assertNotFalse($row);
        \assert(\is_array($row));
        $this->assertSame('idx_email', $row['INDEX_NAME']);
    }

    public function testDropTable(): void
    {
        $table = 'test_drop_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->mysqlStatement($create->query);

        $drop = $this->schema->drop($table);
        $this->mysqlStatement($drop->query);

        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT COUNT(*) as cnt FROM information_schema.TABLES "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ?"
        );
        \assert($stmt !== false);
        $stmt->execute([$table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame('0', (string) $row['cnt']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithForeignKey(): void
    {
        $parentTable = 'test_fk_parent_' . uniqid();
        $childTable = 'test_fk_child_' . uniqid();
        $this->trackMysqlTable($childTable);
        $this->trackMysqlTable($parentTable);

        $createParent = $this->schema->create($parentTable, function (Blueprint $bp) {
            $bp->id();
        });
        $this->mysqlStatement($createParent->query);

        $createChild = $this->schema->create($childTable, function (Blueprint $bp) use ($parentTable) {
            $bp->id();
            $bp->bigInteger('parent_id')->unsigned();
            $bp->foreignKey('parent_id')
                ->references('id')
                ->on($parentTable)
                ->onDelete(ForeignKeyAction::Cascade);
        });
        $this->mysqlStatement($createChild->query);

        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL"
        );
        \assert($stmt !== false);
        $stmt->execute([$childTable]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        $this->assertNotFalse($row);
        \assert(\is_array($row));
        $this->assertSame($parentTable, $row['REFERENCED_TABLE_NAME']);
    }

    public function testCreateTableWithNullableAndDefault(): void
    {
        $table = 'test_null_def_' . uniqid();
        $this->trackMysqlTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('nickname', 100)->nullable()->default('anonymous');
            $bp->integer('score')->default(0);
        });

        $this->mysqlStatement($result->query);

        $columns = $this->fetchMysqlColumns($table);

        $nicknameCol = $this->findColumn($columns, 'nickname');
        $this->assertSame('YES', $nicknameCol['IS_NULLABLE']);
        $this->assertSame('anonymous', $nicknameCol['COLUMN_DEFAULT']);

        $scoreCol = $this->findColumn($columns, 'score');
        $this->assertSame('0', (string) $scoreCol['COLUMN_DEFAULT']); // @phpstan-ignore cast.string
    }

    public function testTruncateTable(): void
    {
        $table = 'test_truncate_' . uniqid();
        $this->trackMysqlTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('name', 50);
        });
        $this->mysqlStatement($create->query);

        $pdo = $this->connectMysql();
        $insertStmt = $pdo->prepare("INSERT INTO `{$table}` (`id`, `name`) VALUES (1, 'a'), (2, 'b')");
        \assert($insertStmt !== false);
        $insertStmt->execute();

        $truncate = $this->schema->truncate($table);
        $this->mysqlStatement($truncate->query);

        $stmt = $pdo->prepare("SELECT COUNT(*) as cnt FROM `{$table}`");
        \assert($stmt !== false);
        $stmt->execute();
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame('0', (string) $row['cnt']); // @phpstan-ignore cast.string
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function fetchMysqlColumns(string $table): array
    {
        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare(
            "SELECT * FROM information_schema.COLUMNS "
            . "WHERE TABLE_SCHEMA = 'query_test' AND TABLE_NAME = ?"
        );
        $stmt->execute([$table]);

        /** @var list<array<string, mixed>> */
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * @param  list<array<string, mixed>>  $columns
     * @return array<string, mixed>
     */
    private function findColumn(array $columns, string $name): array
    {
        foreach ($columns as $col) {
            if ($col['COLUMN_NAME'] === $name) {
                return $col;
            }
        }

        $this->fail("Column '{$name}' not found");
    }
}
