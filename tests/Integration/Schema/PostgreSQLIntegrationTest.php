<?php

namespace Tests\Integration\Schema;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\PostgreSQL;

class PostgreSQLIntegrationTest extends IntegrationTestCase
{
    private PostgreSQL $schema;

    protected function setUp(): void
    {
        parent::setUp();
        $this->schema = new PostgreSQL();
    }

    public function testCreateTableWithBasicColumns(): void
    {
        $table = 'test_basic_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('age');
            $bp->string('name', 100);
            $bp->float('score');
        });

        $this->postgresStatement($result->query);

        $columns = $this->fetchPostgresColumns($table);
        $columnNames = array_column($columns, 'column_name');

        $this->assertContains('age', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('score', $columnNames);

        $nameCol = $this->findColumn($columns, 'name');
        $this->assertSame('character varying', $nameCol['data_type']);
        $this->assertSame('100', (string) $nameCol['character_maximum_length']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithIdentityColumn(): void
    {
        $table = 'test_identity_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->id();
            $bp->string('label', 50);
        });

        $this->postgresStatement($result->query);

        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare(
            "SELECT is_identity, identity_generation FROM information_schema.columns "
            . "WHERE table_catalog = 'query_test' AND table_name = :table AND column_name = 'id'"
        );
        $stmt->execute(['table' => $table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame('YES', $row['is_identity']);
        $this->assertSame('BY DEFAULT', $row['identity_generation']);
    }

    public function testCreateTableWithJsonbColumn(): void
    {
        $table = 'test_jsonb_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->json('metadata');
        });

        $this->postgresStatement($result->query);

        $columns = $this->fetchPostgresColumns($table);
        $metaCol = $this->findColumn($columns, 'metadata');

        $this->assertSame('jsonb', $metaCol['data_type']);
    }

    public function testAlterTableAddColumn(): void
    {
        $table = 'test_alter_add_' . uniqid();
        $this->trackPostgresTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->postgresStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->addColumn('description', ColumnType::Text);
        });
        $this->postgresStatement($alter->query);

        $columns = $this->fetchPostgresColumns($table);
        $columnNames = array_column($columns, 'column_name');

        $this->assertContains('description', $columnNames);
    }

    public function testAlterTableDropColumn(): void
    {
        $table = 'test_alter_drop_' . uniqid();
        $this->trackPostgresTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('temp', 100);
        });
        $this->postgresStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->dropColumn('temp');
        });
        $this->postgresStatement($alter->query);

        $columns = $this->fetchPostgresColumns($table);
        $columnNames = array_column($columns, 'column_name');

        $this->assertNotContains('temp', $columnNames);
    }

    public function testDropTable(): void
    {
        $table = 'test_drop_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->postgresStatement($create->query);

        $drop = $this->schema->drop($table);
        $this->postgresStatement($drop->query);

        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare(
            "SELECT COUNT(*) as cnt FROM information_schema.tables "
            . "WHERE table_catalog = 'query_test' AND table_schema = 'public' AND table_name = :table"
        );
        $stmt->execute(['table' => $table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame('0', (string) $row['cnt']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithBooleanAndText(): void
    {
        $table = 'test_bool_text_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->boolean('is_active');
            $bp->text('bio');
        });

        $this->postgresStatement($result->query);

        $columns = $this->fetchPostgresColumns($table);

        $boolCol = $this->findColumn($columns, 'is_active');
        $this->assertSame('boolean', $boolCol['data_type']);

        $textCol = $this->findColumn($columns, 'bio');
        $this->assertSame('text', $textCol['data_type']);
    }

    public function testCreateTableWithUniqueConstraint(): void
    {
        $table = 'test_unique_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('email', 255)->unique();
        });

        $this->postgresStatement($result->query);

        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare(
            "SELECT tc.constraint_type FROM information_schema.table_constraints tc "
            . "JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name "
            . "WHERE tc.table_name = :table AND ccu.column_name = 'email' AND tc.constraint_type = 'UNIQUE'"
        );
        $stmt->execute(['table' => $table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        $this->assertNotFalse($row);
        \assert(\is_array($row));
        $this->assertSame('UNIQUE', $row['constraint_type']);
    }

    public function testCreateTableWithNullableAndDefault(): void
    {
        $table = 'test_null_def_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('nickname', 100)->nullable()->default('anonymous');
            $bp->integer('score')->default(0);
        });

        $this->postgresStatement($result->query);

        $columns = $this->fetchPostgresColumns($table);

        $nicknameCol = $this->findColumn($columns, 'nickname');
        $this->assertSame('YES', $nicknameCol['is_nullable']);
        $this->assertStringContainsString('anonymous', (string) $nicknameCol['column_default']); // @phpstan-ignore cast.string

        $scoreCol = $this->findColumn($columns, 'score');
        $this->assertSame('0', (string) $scoreCol['column_default']); // @phpstan-ignore cast.string
    }

    public function testTruncateTable(): void
    {
        $table = 'test_truncate_' . uniqid();
        $this->trackPostgresTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('name', 50);
        });
        $this->postgresStatement($create->query);

        $pdo = $this->connectPostgres();
        $insertStmt = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"name\") VALUES (1, 'a'), (2, 'b')");
        \assert($insertStmt !== false);
        $insertStmt->execute();

        $truncate = $this->schema->truncate($table);
        $this->postgresStatement($truncate->query);

        $stmt = $pdo->prepare("SELECT COUNT(*) as cnt FROM \"{$table}\"");
        \assert($stmt !== false);
        $stmt->execute();
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame('0', (string) $row['cnt']); // @phpstan-ignore cast.string
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function fetchPostgresColumns(string $table): array
    {
        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare(
            "SELECT * FROM information_schema.columns "
            . "WHERE table_catalog = 'query_test' AND table_schema = 'public' AND table_name = :table"
        );
        $stmt->execute(['table' => $table]);

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
            if ($col['column_name'] === $name) {
                return $col;
            }
        }

        $this->fail("Column '{$name}' not found");
    }
}
