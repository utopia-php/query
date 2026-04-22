<?php

namespace Tests\Integration\Schema;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\SQLite;

class SQLiteIntegrationTest extends IntegrationTestCase
{
    private SQLite $schema;

    protected function setUp(): void
    {
        parent::setUp();
        $this->schema = new SQLite();
    }

    public function testCreateTableBasic(): void
    {
        $table = 'test_basic_' . uniqid();

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('age');
            $bp->string('name', 100);
            $bp->float('score');
        });

        $this->sqliteStatement($result->query);

        $columns = $this->fetchSqliteColumns($table);
        $columnNames = array_column($columns, 'name');

        $this->assertContains('age', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('score', $columnNames);

        $nameCol = $this->findColumn($columns, 'name');
        $this->assertSame('VARCHAR(100)', (string) $nameCol['type']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithPrimaryKeyAndUnique(): void
    {
        $table = 'test_pk_unique_' . uniqid();

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('email', 255)->unique();
        });

        $this->sqliteStatement($result->query);

        $columns = $this->fetchSqliteColumns($table);

        $idCol = $this->findColumn($columns, 'id');
        $this->assertSame(1, (int) $idCol['pk']); // @phpstan-ignore cast.int

        $pdo = $this->connectSqlite();
        $stmt = $pdo->prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = :table");
        $stmt->execute(['table' => $table]);
        /** @var list<array<string, mixed>> $indexes */
        $indexes = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $found = false;
        foreach ($indexes as $idx) {
            $name = (string) $idx['name']; // @phpstan-ignore cast.string
            $info = $pdo->query("PRAGMA index_info('{$name}')");
            \assert($info !== false);
            /** @var list<array<string, mixed>> $infoRows */
            $infoRows = $info->fetchAll(\PDO::FETCH_ASSOC);
            foreach ($infoRows as $infoRow) {
                if ($infoRow['name'] === 'email') {
                    $found = true;
                }
            }
        }

        $this->assertTrue($found, 'Expected a unique index covering the email column');
    }

    public function testAlterTableAddColumn(): void
    {
        $table = 'test_alter_add_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->sqliteStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->addColumn('description', ColumnType::Text);
        });
        $this->sqliteStatement($alter->query);

        $columns = $this->fetchSqliteColumns($table);
        $columnNames = array_column($columns, 'name');

        $this->assertContains('description', $columnNames);
    }

    public function testCreateIndex(): void
    {
        $table = 'test_index_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('email', 255);
        });
        $this->sqliteStatement($create->query);

        $indexName = 'idx_' . $table . '_email';
        $index = $this->schema->createIndex($table, $indexName, ['email']);
        $this->sqliteStatement($index->query);

        $pdo = $this->connectSqlite();
        $stmt = $pdo->prepare(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = :table AND name = :name"
        );
        $stmt->execute(['table' => $table, 'name' => $indexName]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        $this->assertNotFalse($row);
        \assert(\is_array($row));
        $this->assertSame($indexName, $row['name']);
    }

    public function testDropTable(): void
    {
        $table = 'test_drop_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->sqliteStatement($create->query);

        $drop = $this->schema->drop($table);
        $this->sqliteStatement($drop->query);

        $pdo = $this->connectSqlite();
        $stmt = $pdo->prepare(
            "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type = 'table' AND name = :table"
        );
        $stmt->execute(['table' => $table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame(0, (int) $row['cnt']); // @phpstan-ignore cast.int
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function fetchSqliteColumns(string $table): array
    {
        $pdo = $this->connectSqlite();
        $stmt = $pdo->query("PRAGMA table_info('{$table}')");
        \assert($stmt !== false);

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
            if ($col['name'] === $name) {
                return $col;
            }
        }

        $this->fail("Column '{$name}' not found");
    }
}
