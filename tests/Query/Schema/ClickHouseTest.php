<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\ClickHouse as ClickHouseBuilder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Query;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ClickHouse as Schema;

class ClickHouseTest extends TestCase
{
    // CREATE TABLE

    public function testCreateTableBasic(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->datetime('created_at', 3);
        });

        $this->assertStringContainsString('CREATE TABLE `events`', $result->query);
        $this->assertStringContainsString('`id` Int64', $result->query);
        $this->assertStringContainsString('`name` String', $result->query);
        $this->assertStringContainsString('`created_at` DateTime64(3)', $result->query);
        $this->assertStringContainsString('ENGINE = MergeTree()', $result->query);
        $this->assertStringContainsString('ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('test_types', function (Blueprint $table) {
            $table->integer('int_col');
            $table->integer('uint_col')->unsigned();
            $table->bigInteger('big_col');
            $table->bigInteger('ubig_col')->unsigned();
            $table->float('float_col');
            $table->boolean('bool_col');
            $table->text('text_col');
            $table->json('json_col');
            $table->binary('bin_col');
        });

        $this->assertStringContainsString('`int_col` Int32', $result->query);
        $this->assertStringContainsString('`uint_col` UInt32', $result->query);
        $this->assertStringContainsString('`big_col` Int64', $result->query);
        $this->assertStringContainsString('`ubig_col` UInt64', $result->query);
        $this->assertStringContainsString('`float_col` Float64', $result->query);
        $this->assertStringContainsString('`bool_col` UInt8', $result->query);
        $this->assertStringContainsString('`text_col` String', $result->query);
        $this->assertStringContainsString('`json_col` String', $result->query);
        $this->assertStringContainsString('`bin_col` String', $result->query);
    }

    public function testCreateTableNullableWrapping(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->string('name')->nullable();
        });

        $this->assertStringContainsString('Nullable(String)', $result->query);
    }

    public function testCreateTableWithEnum(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->enum('status', ['active', 'inactive']);
        });

        $this->assertStringContainsString("Enum8('active' = 1, 'inactive' = 2)", $result->query);
    }

    public function testCreateTableWithVector(): void
    {
        $schema = new Schema();
        $result = $schema->create('embeddings', function (Blueprint $table) {
            $table->vector('embedding', 768);
        });

        $this->assertStringContainsString('Array(Float64)', $result->query);
    }

    public function testCreateTableWithSpatialTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('geo', function (Blueprint $table) {
            $table->point('coords');
            $table->linestring('path');
            $table->polygon('area');
        });

        $this->assertStringContainsString('Tuple(Float64, Float64)', $result->query);
        $this->assertStringContainsString('Array(Tuple(Float64, Float64))', $result->query);
        $this->assertStringContainsString('Array(Array(Tuple(Float64, Float64)))', $result->query);
    }

    public function testCreateTableForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->create('t', function (Blueprint $table) {
            $table->foreignKey('user_id')->references('id')->on('users');
        });
    }

    public function testCreateTableWithIndex(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->index(['name']);
        });

        $this->assertStringContainsString('INDEX `idx_name` `name` TYPE minmax GRANULARITY 3', $result->query);
    }
    // ALTER TABLE

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->addColumn('score', 'float');
        });

        $this->assertEquals('ALTER TABLE `events` ADD COLUMN `score` Float64', $result->query);
    }

    public function testAlterModifyColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->modifyColumn('name', 'string');
        });

        $this->assertEquals('ALTER TABLE `events` MODIFY COLUMN `name` String', $result->query);
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->renameColumn('old', 'new');
        });

        $this->assertEquals('ALTER TABLE `events` RENAME COLUMN `old` TO `new`', $result->query);
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->dropColumn('old_col');
        });

        $this->assertEquals('ALTER TABLE `events` DROP COLUMN `old_col`', $result->query);
    }

    public function testAlterForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->alter('events', function (Blueprint $table) {
            $table->addForeignKey('user_id')->references('id')->on('users');
        });
    }
    // DROP TABLE / TRUNCATE

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('events');

        $this->assertEquals('DROP TABLE `events`', $result->query);
    }

    public function testTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('events');

        $this->assertEquals('TRUNCATE TABLE `events`', $result->query);
    }
    // VIEW

    public function testCreateView(): void
    {
        $schema = new Schema();
        $builder = (new ClickHouseBuilder())->from('events')->filter([Query::equal('status', ['active'])]);
        $result = $schema->createView('active_events', $builder);

        $this->assertEquals(
            'CREATE VIEW `active_events` AS SELECT * FROM `events` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testDropView(): void
    {
        $schema = new Schema();
        $result = $schema->dropView('active_events');

        $this->assertEquals('DROP VIEW `active_events`', $result->query);
    }
    // DROP INDEX (ClickHouse-specific)

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('events', 'idx_name');

        $this->assertEquals('ALTER TABLE `events` DROP INDEX `idx_name`', $result->query);
    }
    // Feature interface checks — ClickHouse does NOT implement these

    public function testDoesNotImplementForeignKeys(): void
    {
        $this->assertNotInstanceOf(\Utopia\Query\Schema\Feature\ForeignKeys::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementProcedures(): void
    {
        $this->assertNotInstanceOf(\Utopia\Query\Schema\Feature\Procedures::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementTriggers(): void
    {
        $this->assertNotInstanceOf(\Utopia\Query\Schema\Feature\Triggers::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    // Edge cases

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('events');

        $this->assertEquals('DROP TABLE IF EXISTS `events`', $result->query);
    }

    public function testCreateTableWithDefaultValue(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->integer('count')->default(0);
        });

        $this->assertStringContainsString('DEFAULT 0', $result->query);
    }

    public function testCreateTableWithComment(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->string('name')->comment('User name');
        });

        $this->assertStringContainsString("COMMENT 'User name'", $result->query);
    }

    public function testCreateTableMultiplePrimaryKeys(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('created_at', 3)->primary();
            $table->string('name');
        });

        $this->assertStringContainsString('ORDER BY (`id`, `created_at`)', $result->query);
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->addColumn('score', 'float');
            $table->dropColumn('old_col');
            $table->renameColumn('nm', 'name');
        });

        $this->assertStringContainsString('ADD COLUMN `score` Float64', $result->query);
        $this->assertStringContainsString('DROP COLUMN `old_col`', $result->query);
        $this->assertStringContainsString('RENAME COLUMN `nm` TO `name`', $result->query);
    }

    public function testAlterDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Blueprint $table) {
            $table->dropIndex('idx_name');
        });

        $this->assertStringContainsString('DROP INDEX `idx_name`', $result->query);
    }

    public function testCreateTableWithMultipleIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->string('type');
            $table->index(['name']);
            $table->index(['type']);
        });

        $this->assertStringContainsString('INDEX `idx_name`', $result->query);
        $this->assertStringContainsString('INDEX `idx_type`', $result->query);
    }

    public function testCreateTableTimestampWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->timestamp('ts_col');
        });

        $this->assertStringContainsString('`ts_col` DateTime', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableDatetimeWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('dt_col');
        });

        $this->assertStringContainsString('`dt_col` DateTime', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableWithCompositeIndex(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Blueprint $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->string('type');
            $table->index(['name', 'type']);
        });

        // Composite index wraps in parentheses
        $this->assertStringContainsString('INDEX `idx_name_type` (`name`, `type`) TYPE minmax GRANULARITY 3', $result->query);
    }

    public function testAlterForeignKeyStillThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        $schema = new Schema();
        $schema->alter('events', function (Blueprint $table) {
            $table->dropForeignKey('fk_old');
        });
    }
}
