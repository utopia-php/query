<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\ClickHouse as ClickHouseBuilder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;
use Utopia\Query\Schema\ClickHouse as Schema;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\ClickHouse\SkipIndexAlgorithm;
use Utopia\Query\Schema\Feature\ColumnComments;
use Utopia\Query\Schema\Feature\DropPartition;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\TableComments;
use Utopia\Query\Schema\Feature\Triggers;
use Utopia\Query\Schema\Table;

class ClickHouseTest extends TestCase
{
    use AssertsBindingCount;
    // CREATE TABLE

    public function testCreateTableBasic(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->datetime('created_at', 3);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `created_at` DateTime64(3)) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('test_types', function (Table $table) {
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
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `test_types` (`int_col` Int32, `uint_col` UInt32, `big_col` Int64, `ubig_col` UInt64, `float_col` Float64, `bool_col` UInt8, `text_col` String, `json_col` String, `bin_col` String) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableNullableWrapping(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->string('name')->nullable();
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` Nullable(String)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithEnum(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->enum('status', ['active', 'inactive']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`status` Enum8(\'active\' = 1, \'inactive\' = 2)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithVector(): void
    {
        $schema = new Schema();
        $result = $schema->create('embeddings', function (Table $table) {
            $table->vector('embedding', 768);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `embeddings` (`embedding` Array(Float64)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithSpatialTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('geo', function (Table $table) {
            $table->point('coords');
            $table->linestring('path');
            $table->polygon('area');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `geo` (`coords` Tuple(Float64, Float64), `path` Array(Tuple(Float64, Float64)), `area` Array(Array(Tuple(Float64, Float64)))) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->create('t', function (Table $table) {
            $table->foreignKey('user_id')->references('id')->on('users');
        });
    }

    public function testCreateTableWithIndex(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->index(['name']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, INDEX `idx_name` `name` TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }
    // ALTER TABLE

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->addColumn('score', 'float');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` ADD COLUMN `score` Float64', $result->query);
    }

    public function testAlterModifyColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->modifyColumn('name', 'string');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` MODIFY COLUMN `name` String', $result->query);
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->renameColumn('old', 'new');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` RENAME COLUMN `old` TO `new`', $result->query);
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->dropColumn('old_col');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` DROP COLUMN `old_col`', $result->query);
    }

    public function testAlterForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->alter('events', function (Table $table) {
            $table->addForeignKey('user_id')->references('id')->on('users');
        });
    }
    // DROP TABLE / TRUNCATE

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('events');
        $this->assertBindingCount($result);

        $this->assertSame('DROP TABLE `events`', $result->query);
    }

    public function testTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('events');
        $this->assertBindingCount($result);

        $this->assertSame('TRUNCATE TABLE `events`', $result->query);
    }
    // VIEW

    public function testCreateView(): void
    {
        $schema = new Schema();
        $builder = (new ClickHouseBuilder())->from('events')->filter([Query::equal('status', ['active'])]);
        $result = $schema->createView('active_events', $builder);

        $this->assertSame(
            'CREATE VIEW `active_events` AS SELECT * FROM `events` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertSame(['active'], $result->bindings);
    }

    public function testDropView(): void
    {
        $schema = new Schema();
        $result = $schema->dropView('active_events');

        $this->assertSame('DROP VIEW `active_events`', $result->query);
    }
    // DROP INDEX (ClickHouse-specific)

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('events', 'idx_name');

        $this->assertSame('ALTER TABLE `events` DROP INDEX `idx_name`', $result->query);
    }
    // Feature interface checks — ClickHouse does NOT implement these

    public function testDoesNotImplementForeignKeys(): void
    {
        $this->assertNotInstanceOf(ForeignKeys::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementProcedures(): void
    {
        $this->assertNotInstanceOf(Procedures::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementTriggers(): void
    {
        $this->assertNotInstanceOf(Triggers::class, new Schema()); // @phpstan-ignore method.alreadyNarrowedType
    }

    // Edge cases

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('events');

        $this->assertSame('DROP TABLE IF EXISTS `events`', $result->query);
    }

    public function testCreateTableWithDefaultValue(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->integer('count')->default(0);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `count` Int32 DEFAULT 0) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableWithComment(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name')->comment('User name');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `name` String COMMENT \'User name\') ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableMultiplePrimaryKeys(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('created_at', 3)->primary();
            $table->string('name');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `created_at` DateTime64(3), `name` String) ENGINE = MergeTree() ORDER BY (`id`, `created_at`)', $result->query);
    }

    public function testCreateTableWithCompositePrimaryKey(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id');
            $table->datetime('created_at', 3);
            $table->string('name');
            $table->primary(['id', 'created_at']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `created_at` DateTime64(3), `name` String) ENGINE = MergeTree() ORDER BY (`id`, `created_at`)', $result->query);
    }

    public function testCreateTableRejectsMixedColumnAndTablePrimary(): void
    {
        $schema = new Schema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Cannot combine column-level primary() with Table::primary() composite key.');

        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('created_at', 3);
            $table->primary(['id', 'created_at']);
        });
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->addColumn('score', 'float');
            $table->dropColumn('old_col');
            $table->renameColumn('nm', 'name');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` ADD COLUMN `score` Float64, RENAME COLUMN `nm` TO `name`, DROP COLUMN `old_col`', $result->query);
    }

    public function testAlterDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->dropIndex('idx_name');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` DROP INDEX `idx_name`', $result->query);
    }

    public function testCreateTableWithMultipleIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->string('type');
            $table->index(['name']);
            $table->index(['type']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `type` String, INDEX `idx_name` `name` TYPE minmax GRANULARITY 3, INDEX `idx_type` `type` TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableTimestampWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->timestamp('ts_col');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `ts_col` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableDatetimeWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('dt_col');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `dt_col` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableWithCompositeIndex(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->string('type');
            $table->index(['name', 'type']);
        });
        $this->assertBindingCount($result);

        // Composite index wraps in parentheses
        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `type` String, INDEX `idx_name_type` (`name`, `type`) TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testAlterForeignKeyStillThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        $schema = new Schema();
        $schema->alter('events', function (Table $table) {
            $table->dropForeignKey('fk_old');
        });
    }

    public function testExactCreateTableWithEngine(): void
    {
        $schema = new Schema();
        $result = $schema->create('metrics', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->float('value');
            $table->datetime('recorded_at', 3);
        });

        $this->assertSame(
            'CREATE TABLE `metrics` (`id` Int64, `name` String, `value` Float64, `recorded_at` DateTime64(3)) ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query
        );
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAlterTableAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('metrics', function (Table $table) {
            $table->addColumn('description', 'text')->nullable();
        });

        $this->assertSame(
            'ALTER TABLE `metrics` ADD COLUMN `description` Nullable(String)',
            $result->query
        );
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('metrics');

        $this->assertSame('DROP TABLE `metrics`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testImplementsTableComments(): void
    {
        $this->assertInstanceOf(TableComments::class, new Schema());
    }

    public function testImplementsColumnComments(): void
    {
        $this->assertInstanceOf(ColumnComments::class, new Schema());
    }

    public function testImplementsDropPartition(): void
    {
        $this->assertInstanceOf(DropPartition::class, new Schema());
    }

    public function testCommentOnTable(): void
    {
        $schema = new Schema();
        $result = $schema->commentOnTable('events', 'Main events table');

        $this->assertSame("ALTER TABLE `events` MODIFY COMMENT 'Main events table'", $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testCommentOnColumn(): void
    {
        $schema = new Schema();
        $result = $schema->commentOnColumn('events', 'name', 'Event name');

        $this->assertSame("ALTER TABLE `events` COMMENT COLUMN `name` 'Event name'", $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testDropPartition(): void
    {
        $schema = new Schema();
        $result = $schema->dropPartition('events', '202401');

        $this->assertSame("ALTER TABLE `events` DROP PARTITION '202401'", $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testCreateTableWithPartition(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
            $table->datetime('created_at', 3);
            $table->partitionByRange('toYYYYMM(created_at)');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `created_at` DateTime64(3)) ENGINE = MergeTree() PARTITION BY toYYYYMM(created_at) ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableIfNotExists(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('name');
        }, ifNotExists: true);
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE IF NOT EXISTS `events` (`id` Int64, `name` String) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCompileAutoIncrementReturnsEmpty(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->bigInteger('id')->primary()->autoIncrement();
        });
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('AUTO_INCREMENT', $result->query);
        $this->assertStringNotContainsString('IDENTITY', $result->query);
    }

    public function testCompileUnsignedReturnsEmpty(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->integer('val')->unsigned();
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`val` UInt32) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
        $this->assertStringNotContainsString('UNSIGNED', $result->query);
    }

    public function testCommentOnTableEscapesSingleQuotes(): void
    {
        $schema = new Schema();
        $result = $schema->commentOnTable('events', "User's events");

        $this->assertSame('ALTER TABLE `events` MODIFY COMMENT \'User\'\'s events\'', $result->query);
    }

    public function testCommentOnColumnEscapesSingleQuotes(): void
    {
        $schema = new Schema();
        $result = $schema->commentOnColumn('events', 'name', "It's a name");

        $this->assertSame('ALTER TABLE `events` COMMENT COLUMN `name` \'It\'\'s a name\'', $result->query);
    }

    public function testDropPartitionEscapesSingleQuotes(): void
    {
        $schema = new Schema();
        $result = $schema->dropPartition('events', "test'val");

        $this->assertSame('ALTER TABLE `events` DROP PARTITION \'test\'\'val\'', $result->query);
    }

    public function testEnumEscapesBackslash(): void
    {
        $schema = new Schema();
        $result = $schema->create('items', function (Table $table) {
            // Input: a\' ; backslash must be escaped BEFORE the quote
            // so the quote-escape `\'` is not cancelled by a trailing `\`.
            $table->enum('status', ["a\\'b"]);
        });

        // Output literal: 'a\\\'b' (a, 2 backslashes, escaped quote, b)
        $this->assertSame("CREATE TABLE `items` (`status` Enum8('a\\\\\\'b' = 1)) ENGINE = MergeTree() ORDER BY tuple()", $result->query);
    }

    public function testCreateMergeTreeWithoutPrimaryKeysEmitsOrderByTuple(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->string('name');
            $table->integer('count');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`name` String, `count` Int32) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
        $this->assertStringNotContainsString('ORDER BY (', $result->query);
    }

    public function testAlterTableWithNoAlterationsThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('ALTER TABLE requires at least one alteration.');

        $schema = new Schema();
        $schema->alter('events', function (Table $table) {
            // no alterations
        });
    }

    public function testCreateReplacingMergeTreeEmitsEngineWithVersion(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->integer('version');
            $table->engine(Engine::ReplacingMergeTree, 'version');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `version` Int32) ENGINE = ReplacingMergeTree(`version`) ORDER BY (`id`)', $result->query);
    }

    public function testCreateSummingMergeTreeEmitsEngineWithColumns(): void
    {
        $schema = new Schema();
        $result = $schema->create('metrics', function (Table $table) {
            $table->integer('key')->primary();
            $table->bigInteger('total')->unsigned();
            $table->bigInteger('count')->unsigned();
            $table->engine(Engine::SummingMergeTree, 'total', 'count');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `metrics` (`key` Int32, `total` UInt64, `count` UInt64) ENGINE = SummingMergeTree(`total`, `count`) ORDER BY (`key`)', $result->query);
    }

    public function testCreateCollapsingMergeTreeRejectsMissingSignColumn(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('CollapsingMergeTree requires a sign column.');

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->integer('id')->primary();
            $table->engine(Engine::CollapsingMergeTree);
        });
    }

    public function testCreateMemoryEngineSkipsOrderBy(): void
    {
        $schema = new Schema();
        $result = $schema->create('cache', function (Table $table) {
            $table->integer('id')->primary();
            $table->string('value');
            $table->engine(Engine::Memory);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `cache` (`id` Int32, `value` String) ENGINE = Memory', $result->query);
        $this->assertStringNotContainsString('ORDER BY', $result->query);
    }

    public function testCreateAggregatingMergeTreeEmitsEmptyArgs(): void
    {
        $schema = new Schema();
        $result = $schema->create('agg', function (Table $table) {
            $table->integer('key')->primary();
            $table->engine(Engine::AggregatingMergeTree);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `agg` (`key` Int32) ENGINE = AggregatingMergeTree() ORDER BY (`key`)', $result->query);
    }

    public function testCreateReplicatedMergeTreeRejectsMissingArgs(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->integer('id')->primary();
            $table->engine(Engine::ReplicatedMergeTree, '/clickhouse/tables/events');
        });
    }

    public function testTableLevelTTL(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->integer('id')->primary();
            $table->datetime('ts');
            $table->ttl('ts + INTERVAL 1 DAY');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int32, `ts` DateTime) ENGINE = MergeTree() ORDER BY (`id`) TTL ts + INTERVAL 1 DAY', $result->query);
    }

    public function testTableLevelTTLRejectsSemicolon(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->integer('id')->primary();
            $table->ttl('ts + INTERVAL 1 DAY;');
        });
    }

    public function testColumnLevelTTL(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->integer('id')->primary();
            $table->string('temporary')->ttl('ts + INTERVAL 1 DAY');
            $table->datetime('ts');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int32, `temporary` String TTL ts + INTERVAL 1 DAY, `ts` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    // Data-skipping indexes

    public function testDataSkippingIndexBloomFilter(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('user_id');
            $table->dataSkippingIndex(['user_id'], SkipIndexAlgorithm::BloomFilter);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `user_id` String, INDEX `skip_user_id` `user_id` TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testDataSkippingIndexWithArgs(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('country');
            $table->string('text');
            $table->dataSkippingIndex(['country'], SkipIndexAlgorithm::Set, granularity: 4, algorithmArgs: [100]);
            $table->dataSkippingIndex(['text'], SkipIndexAlgorithm::NgramBloomFilter, algorithmArgs: [4, 1024, 3, 0]);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `country` String, `text` String,'
            . ' INDEX `skip_country` `country` TYPE set(100) GRANULARITY 4,'
            . ' INDEX `skip_text` `text` TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testDataSkippingIndexCompositeColumns(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('user_id');
            $table->string('event');
            $table->dataSkippingIndex(['user_id', 'event'], SkipIndexAlgorithm::BloomFilter, name: 'idx_user_event');
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `user_id` String, `event` String,'
            . ' INDEX `idx_user_event` (`user_id`, `event`) TYPE bloom_filter GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testDataSkippingIndexInvalidGranularityThrows(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('user_id');
            $table->dataSkippingIndex(['user_id'], SkipIndexAlgorithm::BloomFilter, granularity: 0);
        });
    }

    public function testDataSkippingIndexEmptyColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->dataSkippingIndex([], SkipIndexAlgorithm::BloomFilter);
        });
    }

    // SETTINGS

    public function testTableSettings(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->settings(['index_granularity' => 8192, 'allow_nullable_key' => true]);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64) ENGINE = MergeTree() ORDER BY (`id`)'
            . ' SETTINGS index_granularity = 8192, allow_nullable_key = 1',
            $result->query,
        );
    }

    public function testTableSettingsWithTtlOrdering(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->datetime('created_at');
            $table->ttl('`created_at` + INTERVAL 30 DAY');
            $table->settings(['index_granularity' => 4096]);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `created_at` DateTime) ENGINE = MergeTree() ORDER BY (`id`)'
            . ' TTL `created_at` + INTERVAL 30 DAY'
            . ' SETTINGS index_granularity = 4096',
            $result->query,
        );
    }

    public function testTableSettingsRejectsInvalidKey(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->settings(['1bad-key' => 8192]);
        });
    }

    public function testTableSettingsRejectsInvalidValue(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->settings(['ok_key' => "evil'; DROP TABLE x; --"]);
        });
    }

    public function testDataSkippingIndexNoArgAlgorithmRejectsArgs(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('minmax does not accept algorithm arguments.');

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->integer('score');
            $table->dataSkippingIndex(['score'], SkipIndexAlgorithm::MinMax, algorithmArgs: [3]);
        });
    }

    public function testDataSkippingIndexInvertedRejectsArgs(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('text');
            $table->dataSkippingIndex(['text'], SkipIndexAlgorithm::Inverted, algorithmArgs: [42]);
        });
    }

    public function testDataSkippingIndexAutoNameSanitisesNonIdentifierColumns(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('event-type');
            $table->dataSkippingIndex(['event-type'], SkipIndexAlgorithm::BloomFilter);
        });
        $this->assertBindingCount($result);

        // Auto name: skip_event_type (non-identifier chars collapsed to _)
        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `event-type` String,'
            . ' INDEX `skip_event_type` `event-type` TYPE bloom_filter GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testDataSkippingIndexFloatArgAvoidsScientificNotation(): void
    {
        $schema = new Schema();
        $result = $schema->create('events', function (Table $table) {
            $table->bigInteger('id')->primary();
            $table->string('user_id');
            // 1e-5 false positive rate: the bug pre-fix is `(string) 1e-5` returning "1.0E-5"
            $table->dataSkippingIndex(['user_id'], SkipIndexAlgorithm::BloomFilter, algorithmArgs: [1.0e-5]);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('TYPE bloom_filter(0.00001)', $result->query);
        // Numeric arg should be fixed-point — no 'E-' or 'E+' anywhere
        $this->assertDoesNotMatchRegularExpression('/[Ee][+-]\d/', $result->query);
    }

    public function testAlterAddSkipIndex(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->dataSkippingIndex(['user_id'], SkipIndexAlgorithm::BloomFilter);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `events` ADD INDEX `skip_user_id` `user_id` TYPE bloom_filter GRANULARITY 1',
            $result->query,
        );
    }

    public function testAlterAddSkipIndexComposite(): void
    {
        $schema = new Schema();
        $result = $schema->alter('events', function (Table $table) {
            $table->dataSkippingIndex(['user_id', 'event'], SkipIndexAlgorithm::Set, granularity: 4, algorithmArgs: [100], name: 'idx_user_event');
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `events` ADD INDEX `idx_user_event` (`user_id`, `event`) TYPE set(100) GRANULARITY 4',
            $result->query,
        );
    }

    public function testAlterRejectsSettings(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('SETTINGS');

        $schema = new Schema();
        $schema->alter('events', function (Table $table) {
            $table->settings(['index_granularity' => 4096]);
        });
    }
}
