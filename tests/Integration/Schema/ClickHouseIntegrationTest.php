<?php

namespace Tests\Integration\Schema;

use Tests\Integration\IntegrationTestCase;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ClickHouse;
use Utopia\Query\Schema\ColumnType;

class ClickHouseIntegrationTest extends IntegrationTestCase
{
    private ClickHouse $schema;

    protected function setUp(): void
    {
        parent::setUp();
        $this->schema = new ClickHouse();
    }

    public function testCreateTableWithMergeTreeEngine(): void
    {
        $table = 'test_mergetree_' . uniqid();
        $this->trackClickhouseTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('name', 100);
            $bp->integer('value');
        });

        $this->clickhouseStatement($result->query);

        $ch = $this->connectClickhouse();
        $rows = $ch->execute(
            "SELECT name, type FROM system.columns WHERE database = 'query_test' AND table = '{$table}' ORDER BY position"
        );

        $columnNames = array_column($rows, 'name');
        $this->assertContains('id', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('value', $columnNames);

        $tables = $ch->execute(
            "SELECT engine FROM system.tables WHERE database = 'query_test' AND name = '{$table}'"
        );
        $this->assertSame('MergeTree', $tables[0]['engine']);
    }

    public function testCreateTableWithNullableColumns(): void
    {
        $table = 'test_nullable_' . uniqid();
        $this->trackClickhouseTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->string('optional_name', 100)->nullable();
            $bp->integer('optional_count')->nullable();
        });

        $this->clickhouseStatement($result->query);

        $ch = $this->connectClickhouse();
        $rows = $ch->execute(
            "SELECT name, type FROM system.columns WHERE database = 'query_test' AND table = '{$table}'"
        );

        $typeMap = [];
        foreach ($rows as $row) {
            $name = $row['name'];
            $type = $row['type'];
            \assert(\is_string($name) && \is_string($type));
            $typeMap[$name] = $type;
        }

        $this->assertStringContainsString('Nullable', $typeMap['optional_name']);
        $this->assertStringContainsString('Nullable', $typeMap['optional_count']);
        $this->assertStringNotContainsString('Nullable', $typeMap['id']);
    }

    public function testAlterTableAddColumn(): void
    {
        $table = 'test_alter_add_' . uniqid();
        $this->trackClickhouseTable($table);

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->clickhouseStatement($create->query);

        $alter = $this->schema->alter($table, function (Blueprint $bp) {
            $bp->addColumn('description', ColumnType::String, 200);
        });
        $this->clickhouseStatement($alter->query);

        $ch = $this->connectClickhouse();
        $rows = $ch->execute(
            "SELECT name FROM system.columns WHERE database = 'query_test' AND table = '{$table}'"
        );

        $columnNames = array_column($rows, 'name');
        $this->assertContains('description', $columnNames);
    }

    public function testDropTable(): void
    {
        $table = 'test_drop_' . uniqid();

        $create = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
        });
        $this->clickhouseStatement($create->query);

        $drop = $this->schema->drop($table);
        $this->clickhouseStatement($drop->query);

        $ch = $this->connectClickhouse();
        $rows = $ch->execute(
            "SELECT count() as cnt FROM system.tables WHERE database = 'query_test' AND name = '{$table}'"
        );

        $this->assertSame('0', (string) $rows[0]['cnt']); // @phpstan-ignore cast.string
    }

    public function testCreateTableWithDateTimePrecision(): void
    {
        $table = 'test_dt64_' . uniqid();
        $this->trackClickhouseTable($table);

        $result = $this->schema->create($table, function (Blueprint $bp) {
            $bp->integer('id')->primary();
            $bp->datetime('created_at', 3);
            $bp->datetime('updated_at', 6);
        });

        $this->clickhouseStatement($result->query);

        $ch = $this->connectClickhouse();
        $rows = $ch->execute(
            "SELECT name, type FROM system.columns WHERE database = 'query_test' AND table = '{$table}'"
        );

        $typeMap = [];
        foreach ($rows as $row) {
            $name = $row['name'];
            $type = $row['type'];
            \assert(\is_string($name) && \is_string($type));
            $typeMap[$name] = $type;
        }

        $this->assertSame('DateTime64(3)', $typeMap['created_at']);
        $this->assertSame('DateTime64(6)', $typeMap['updated_at']);
    }
}
