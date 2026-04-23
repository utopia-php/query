<?php

namespace Tests\Integration\Schema;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\PostgreSQL;
use Utopia\Query\Schema\Table;

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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $create = $this->schema->create($table, function (Table $bp) {
            $bp->integer('id')->primary();
        });
        $this->postgresStatement($create->query);

        $alter = $this->schema->alter($table, function (Table $bp) {
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

        $create = $this->schema->create($table, function (Table $bp) {
            $bp->integer('id')->primary();
            $bp->string('temp', 100);
        });
        $this->postgresStatement($create->query);

        $alter = $this->schema->alter($table, function (Table $bp) {
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

        $create = $this->schema->create($table, function (Table $bp) {
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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $result = $this->schema->create($table, function (Table $bp) {
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

        $create = $this->schema->create($table, function (Table $bp) {
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

    public function testCreateTableWithCheckConstraint(): void
    {
        $table = 'test_check_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Table $bp) {
            $bp->integer('id')->primary();
            $bp->integer('age');
            $bp->check('age_min', '"age" >= 18');
        });

        $this->postgresStatement($result->query);

        $pdo = $this->connectPostgres();
        $insertOk = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"age\") VALUES (1, 21)");
        \assert($insertOk !== false);
        $insertOk->execute();

        $this->expectException(\PDOException::class);
        $insertFail = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"age\") VALUES (2, 10)");
        \assert($insertFail !== false);
        $insertFail->execute();
    }

    public function testCreateTableWithGeneratedColumn(): void
    {
        $table = 'test_generated_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Table $bp) {
            $bp->integer('id')->primary();
            $bp->integer('price');
            $bp->integer('quantity');
            $bp->integer('total')->generatedAs('"price" * "quantity"')->stored();
        });

        $this->postgresStatement($result->query);

        $pdo = $this->connectPostgres();
        $insert = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"price\", \"quantity\") VALUES (1, 5, 3)");
        \assert($insert !== false);
        $insert->execute();

        $stmt = $pdo->prepare("SELECT \"total\" FROM \"{$table}\" WHERE \"id\" = 1");
        \assert($stmt !== false);
        $stmt->execute();
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertSame(15, (int) $row['total']); // @phpstan-ignore cast.int

        $columns = $this->fetchPostgresColumns($table);
        $totalCol = $this->findColumn($columns, 'total');
        $this->assertSame('ALWAYS', $totalCol['is_generated']);
    }

    public function testCreateTableWithSerial(): void
    {
        $table = 'test_serial_' . uniqid();
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Table $bp) {
            $bp->bigSerial('id')->primary();
            $bp->string('label', 50);
        });

        $this->assertStringContainsString('BIGSERIAL', $result->query);
        $this->postgresStatement($result->query);

        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare("SELECT pg_get_serial_sequence(:table, 'id') AS seq");
        \assert($stmt !== false);
        $stmt->execute(['table' => $table]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));

        $this->assertNotNull($row['seq']);
        $this->assertStringContainsString($table, (string) $row['seq']); // @phpstan-ignore cast.string

        $insert = $pdo->prepare("INSERT INTO \"{$table}\" (\"label\") VALUES ('a'), ('b') RETURNING \"id\"");
        \assert($insert !== false);
        $insert->execute();
        $ids = $insert->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertCount(2, $ids);
        $first = (int) $ids[0]; // @phpstan-ignore cast.int
        $second = (int) $ids[1]; // @phpstan-ignore cast.int
        $this->assertGreaterThan($first, $second);
    }

    public function testCreateEnumType(): void
    {
        $typeName = 'mood_' . uniqid();
        $table = 'test_enum_type_' . uniqid();
        $this->trackPostgresTable($table);

        try {
            $createType = $this->schema->createType($typeName, ['happy', 'sad', 'neutral']);
            $this->postgresStatement($createType->query);

            $result = $this->schema->create($table, function (Table $bp) use ($typeName) {
                $bp->integer('id')->primary();
                $bp->string('mood')->userType($typeName);
            });

            $this->assertStringContainsString('"mood" "' . $typeName . '"', $result->query);

            $this->postgresStatement($result->query);

            $pdo = $this->connectPostgres();
            $insert = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"mood\") VALUES (1, 'happy')");
            \assert($insert !== false);
            $insert->execute();

            $stmt = $pdo->prepare("SELECT \"mood\" FROM \"{$table}\" WHERE \"id\" = 1");
            \assert($stmt !== false);
            $stmt->execute();
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
            \assert(\is_array($row));
            $this->assertSame('happy', $row['mood']);

            $typeStmt = $pdo->prepare(
                "SELECT typname FROM pg_type WHERE typname = :name"
            );
            $typeStmt->execute(['name' => $typeName]);
            $typeRow = $typeStmt->fetch(\PDO::FETCH_ASSOC);
            $this->assertNotFalse($typeRow);
            \assert(\is_array($typeRow));
            $this->assertSame($typeName, $typeRow['typname']);
        } finally {
            $this->postgresStatement("DROP TABLE IF EXISTS \"{$table}\" CASCADE");
            $this->postgresStatement("DROP TYPE IF EXISTS \"{$typeName}\"");
        }
    }

    public function testCreatePartitionedTable(): void
    {
        $table = 'test_partitioned_' . uniqid();
        $partition = $table . '_2024';
        $this->trackPostgresTable($partition);
        $this->trackPostgresTable($table);

        $result = $this->schema->create($table, function (Table $bp) {
            $bp->integer('id');
            $bp->timestamp('created_at');
            $bp->primary(['id', 'created_at']);
            $bp->partitionByRange('"created_at"');
        });

        $this->postgresStatement($result->query);

        $partitionStatement = $this->schema->createPartition($table, $partition, "FROM ('2024-01-01') TO ('2025-01-01')");
        $this->postgresStatement($partitionStatement->query);

        $pdo = $this->connectPostgres();
        $insert = $pdo->prepare("INSERT INTO \"{$table}\" (\"id\", \"created_at\") VALUES (1, '2024-06-15')");
        \assert($insert !== false);
        $insert->execute();

        $stmt = $pdo->prepare("SELECT COUNT(*) AS cnt FROM \"{$partition}\"");
        \assert($stmt !== false);
        $stmt->execute();
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($row));
        $this->assertSame('1', (string) $row['cnt']); // @phpstan-ignore cast.string

        $partitionCheck = $pdo->prepare(
            "SELECT relkind FROM pg_class WHERE relname = :name"
        );
        $partitionCheck->execute(['name' => $table]);
        $relRow = $partitionCheck->fetch(\PDO::FETCH_ASSOC);
        \assert(\is_array($relRow));
        $this->assertSame('p', $relRow['relkind']);
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
