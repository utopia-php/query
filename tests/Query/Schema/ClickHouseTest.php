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
use Utopia\Query\Schema\ClickHouse\IndexAlgorithm;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\Feature\ColumnComments;
use Utopia\Query\Schema\Feature\DropPartition;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\TableComments;
use Utopia\Query\Schema\Feature\Triggers;

class ClickHouseTest extends TestCase
{
    use AssertsBindingCount;

    public function testCreateTableBasic(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->datetime('created_at', 3)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `created_at` DateTime64(3)) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->table('test_types')
            ->integer('int_col')
            ->integer('uint_col')->unsigned()
            ->bigInteger('big_col')
            ->bigInteger('ubig_col')->unsigned()
            ->float('float_col')
            ->boolean('bool_col')
            ->text('text_col')
            ->json('json_col')
            ->binary('bin_col')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `test_types` (`int_col` Int32, `uint_col` UInt32, `big_col` Int64, `ubig_col` UInt64, `float_col` Float64, `bool_col` UInt8, `text_col` String, `json_col` String, `bin_col` String) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableNullableWrapping(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->string('name')->nullable()
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` Nullable(String)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithEnum(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->enum('status', ['active', 'inactive'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`status` Enum8(\'active\' = 1, \'inactive\' = 2)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithVector(): void
    {
        $schema = new Schema();
        $result = $schema->table('embeddings')
            ->vector('embedding', 768)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `embeddings` (`embedding` Array(Float64)) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableWithSpatialTypes(): void
    {
        $schema = new Schema();
        $result = $schema->table('geo')
            ->point('coords')
            ->linestring('path')
            ->polygon('area')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `geo` (`coords` Tuple(Float64, Float64), `path` Array(Tuple(Float64, Float64)), `area` Array(Array(Tuple(Float64, Float64)))) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
    }

    public function testCreateTableForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->table('t')
            ->foreignKey('user_id')->references('id')->on('users')
            ->create();
    }

    public function testCreateTableWithIndex(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->index(['name'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, INDEX `idx_name` `name` TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->addColumn('score', ColumnType::Float)
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` ADD COLUMN `score` Float64', $result->query);
    }

    public function testAlterModifyColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->modifyColumn('name', ColumnType::String)
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` MODIFY COLUMN `name` String', $result->query);
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->renameColumn('old', 'new')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` RENAME COLUMN `old` TO `new`', $result->query);
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->dropColumn('old_col')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` DROP COLUMN `old_col`', $result->query);
    }

    public function testAlterForeignKeyThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Foreign keys are not supported in ClickHouse');

        $schema = new Schema();
        $schema->table('events')
            ->addForeignKey('user_id')->references('id')->on('users')
            ->alter();
    }

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')->drop();
        $this->assertBindingCount($result);

        $this->assertSame('DROP TABLE `events`', $result->query);
    }

    public function testTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')->truncate();
        $this->assertBindingCount($result);

        $this->assertSame('TRUNCATE TABLE `events`', $result->query);
    }

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

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('events', 'idx_name');

        $this->assertSame('ALTER TABLE `events` DROP INDEX `idx_name`', $result->query);
    }

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

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')->dropIfExists();

        $this->assertSame('DROP TABLE IF EXISTS `events`', $result->query);
    }

    public function testCreateTableWithDefaultValue(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->bigInteger('id')->primary()
            ->integer('count')->default(0)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `count` Int32 DEFAULT 0) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableWithComment(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->bigInteger('id')->primary()
            ->string('name')->comment('User name')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `name` String COMMENT \'User name\') ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableMultiplePrimaryKeys(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->datetime('created_at', 3)->primary()
            ->string('name')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `created_at` DateTime64(3), `name` String) ENGINE = MergeTree() ORDER BY (`id`, `created_at`)', $result->query);
    }

    public function testCreateTableWithCompositePrimaryKey(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')
            ->datetime('created_at', 3)
            ->string('name')
            ->primary(['id', 'created_at'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `created_at` DateTime64(3), `name` String) ENGINE = MergeTree() ORDER BY (`id`, `created_at`)', $result->query);
    }

    public function testCreateTableRejectsMixedColumnAndTablePrimary(): void
    {
        $schema = new Schema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Cannot combine column-level primary() with Table::primary() composite key.');

        $schema->table('events')
            ->bigInteger('id')->primary()
            ->datetime('created_at', 3)
            ->primary(['id', 'created_at'])
            ->create();
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->addColumn('score', ColumnType::Float)
            ->dropColumn('old_col')
            ->renameColumn('nm', 'name')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` ADD COLUMN `score` Float64, RENAME COLUMN `nm` TO `name`, DROP COLUMN `old_col`', $result->query);
    }

    public function testAlterDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->dropIndex('idx_name')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `events` DROP INDEX `idx_name`', $result->query);
    }

    public function testCreateTableWithMultipleIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->string('type')
            ->index(['name'])
            ->index(['type'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `type` String, INDEX `idx_name` `name` TYPE minmax GRANULARITY 3, INDEX `idx_type` `type` TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableTimestampWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->bigInteger('id')->primary()
            ->timestamp('ts_col')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `ts_col` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableDatetimeWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->bigInteger('id')->primary()
            ->datetime('dt_col')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` Int64, `dt_col` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
        $this->assertStringNotContainsString('DateTime64', $result->query);
    }

    public function testCreateTableWithCompositeIndex(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->string('type')
            ->index(['name', 'type'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `type` String, INDEX `idx_name_type` (`name`, `type`) TYPE minmax GRANULARITY 3) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testAlterForeignKeyStillThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        $schema = new Schema();
        $schema->table('events')
            ->dropForeignKey('fk_old')
            ->alter();
    }

    public function testExactCreateTableWithEngine(): void
    {
        $schema = new Schema();
        $result = $schema->table('metrics')
            ->bigInteger('id')->primary()
            ->string('name')
            ->float('value')
            ->datetime('recorded_at', 3)
            ->create();

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
        $result = $schema->table('metrics')
            ->addColumn('description', ColumnType::Text)->nullable()
            ->alter();

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
        $result = $schema->table('metrics')->drop();

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
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->datetime('created_at', 3)
            ->partitionByRange('toYYYYMM(created_at)')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `name` String, `created_at` DateTime64(3)) ENGINE = MergeTree() PARTITION BY toYYYYMM(created_at) ORDER BY (`id`)', $result->query);
    }

    public function testCreateTableIfNotExists(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('name')
            ->createIfNotExists();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE IF NOT EXISTS `events` (`id` Int64, `name` String) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    public function testCompileAutoIncrementReturnsEmpty(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->bigInteger('id')->primary()->autoIncrement()
            ->create();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('AUTO_INCREMENT', $result->query);
        $this->assertStringNotContainsString('IDENTITY', $result->query);
    }

    public function testCompileUnsignedReturnsEmpty(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->integer('val')->unsigned()
            ->create();
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
        $result = $schema->table('items')
            ->enum('status', ["a\\'b"])
            ->create();

        $this->assertSame("CREATE TABLE `items` (`status` Enum8('a\\\\\\'b' = 1)) ENGINE = MergeTree() ORDER BY tuple()", $result->query);
    }

    public function testCreateMergeTreeWithoutPrimaryKeysEmitsOrderByTuple(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->string('name')
            ->integer('count')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`name` String, `count` Int32) ENGINE = MergeTree() ORDER BY tuple()', $result->query);
        $this->assertStringNotContainsString('ORDER BY (', $result->query);
    }

    public function testAlterTableWithNoAlterationsThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('ALTER TABLE requires at least one alteration.');

        $schema = new Schema();
        $schema->table('events')->alter();
    }

    public function testCreateReplacingMergeTreeEmitsEngineWithVersion(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->integer('version')
            ->engine(Engine::ReplacingMergeTree, 'version')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int64, `version` Int32) ENGINE = ReplacingMergeTree(`version`) ORDER BY (`id`)', $result->query);
    }

    public function testCreateSummingMergeTreeEmitsEngineWithColumns(): void
    {
        $schema = new Schema();
        $result = $schema->table('metrics')
            ->integer('key')->primary()
            ->bigInteger('total')->unsigned()
            ->bigInteger('count')->unsigned()
            ->engine(Engine::SummingMergeTree, 'total', 'count')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `metrics` (`key` Int32, `total` UInt64, `count` UInt64) ENGINE = SummingMergeTree(`total`, `count`) ORDER BY (`key`)', $result->query);
    }

    public function testCreateCollapsingMergeTreeRejectsMissingSignColumn(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('CollapsingMergeTree requires a sign column.');

        $schema = new Schema();
        $schema->table('events')
            ->integer('id')->primary()
            ->engine(Engine::CollapsingMergeTree)
            ->create();
    }

    public function testCreateMemoryEngineSkipsOrderBy(): void
    {
        $schema = new Schema();
        $result = $schema->table('cache')
            ->integer('id')->primary()
            ->string('value')
            ->engine(Engine::Memory)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `cache` (`id` Int32, `value` String) ENGINE = Memory', $result->query);
        $this->assertStringNotContainsString('ORDER BY', $result->query);
    }

    public function testCreateAggregatingMergeTreeEmitsEmptyArgs(): void
    {
        $schema = new Schema();
        $result = $schema->table('agg')
            ->integer('key')->primary()
            ->engine(Engine::AggregatingMergeTree)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `agg` (`key` Int32) ENGINE = AggregatingMergeTree() ORDER BY (`key`)', $result->query);
    }

    public function testCreateReplicatedMergeTreeRejectsMissingArgs(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->integer('id')->primary()
            ->engine(Engine::ReplicatedMergeTree, '/clickhouse/tables/events')
            ->create();
    }

    public function testTableLevelTTL(): void
    {
        $schema = new Schema();
        $table = $schema->table('events');
        $table->integer('id')->primary();
        $table->datetime('ts');
        $result = $table->ttl('ts + INTERVAL 1 DAY')->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int32, `ts` DateTime) ENGINE = MergeTree() ORDER BY (`id`) TTL ts + INTERVAL 1 DAY', $result->query);
    }

    public function testTableLevelTTLRejectsSemicolon(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->integer('id')->primary()
            ->ttl('ts + INTERVAL 1 DAY;')
            ->create();
    }

    public function testColumnLevelTTL(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->integer('id')->primary()
            ->string('temporary')->ttl('ts + INTERVAL 1 DAY')
            ->datetime('ts')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `events` (`id` Int32, `temporary` String TTL ts + INTERVAL 1 DAY, `ts` DateTime) ENGINE = MergeTree() ORDER BY (`id`)', $result->query);
    }

    // ClickHouse skip-index algorithm selection

    public function testIndexBloomFilter(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->index(['user_id'], algorithm: IndexAlgorithm::BloomFilter)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `user_id` String, INDEX `idx_user_id` `user_id` TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testIndexWithAlgorithmArgs(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('country')
            ->string('text')
            ->index(['country'], algorithm: IndexAlgorithm::Set, algorithmArgs: [100], granularity: 4)
            ->index(['text'], algorithm: IndexAlgorithm::NgramBloomFilter, algorithmArgs: [4, 1024, 3, 0])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `country` String, `text` String,'
            . ' INDEX `idx_country` `country` TYPE set(100) GRANULARITY 4,'
            . ' INDEX `idx_text` `text` TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testIndexCompositeColumnsWithAlgorithm(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->string('event')
            ->index(['user_id', 'event'], name: 'idx_user_event', algorithm: IndexAlgorithm::BloomFilter)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `user_id` String, `event` String,'
            . ' INDEX `idx_user_event` (`user_id`, `event`) TYPE bloom_filter GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testIndexInvalidGranularityThrows(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->index(['user_id'], algorithm: IndexAlgorithm::BloomFilter, granularity: 0);
    }

    public function testIndexEmptyColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->index([]);
    }

    public function testIndexNameRegexOnlyEnforcedForClickHouseAlgorithms(): void
    {
        // No algorithm → permissive name allowed (other dialects quote names)
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->index(['user_id'], name: 'idx-with-hyphens')
            ->create();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('INDEX `idx-with-hyphens`', $result->query);
    }

    public function testIndexNameRegexEnforcedWhenAlgorithmIsSet(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid index name: idx-with-hyphens');

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->index(['user_id'], name: 'idx-with-hyphens', algorithm: IndexAlgorithm::BloomFilter);
    }

    // SETTINGS

    public function testTableSettings(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->settings(['index_granularity' => 8192, 'allow_nullable_key' => true])
            ->create();
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
        $table = $schema->table('events');
        $table->bigInteger('id')->primary();
        $table->datetime('created_at');
        $result = $table
            ->ttl('`created_at` + INTERVAL 30 DAY')
            ->settings(['index_granularity' => 4096])
            ->create();
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
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->settings(['1bad-key' => 8192]);
    }

    public function testTableSettingsRejectsInvalidValue(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->settings(['ok_key' => "evil'; DROP TABLE x; --"]);
    }

    public function testTableSettingsFloatAvoidsScientificNotation(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->settings(['merge_with_ttl_timeout' => 1.0e-5])
            ->create();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS merge_with_ttl_timeout = 0.00001', $result->query);
        $this->assertDoesNotMatchRegularExpression('/[Ee][+-]\d/', $result->query);
    }

    public function testIndexNoArgAlgorithmRejectsArgs(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('minmax does not accept algorithm arguments.');

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->integer('score')
            ->index(['score'], algorithm: IndexAlgorithm::MinMax, algorithmArgs: [3]);
    }

    public function testIndexInvertedRejectsArgs(): void
    {
        $this->expectException(ValidationException::class);

        $schema = new Schema();
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('text')
            ->index(['text'], algorithm: IndexAlgorithm::Inverted, algorithmArgs: [42]);
    }

    public function testIndexAutoNameSanitisesNonIdentifierColumns(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('event-type')
            ->index(['event-type'], algorithm: IndexAlgorithm::BloomFilter)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `events` (`id` Int64, `event-type` String,'
            . ' INDEX `idx_event_type` `event-type` TYPE bloom_filter GRANULARITY 1)'
            . ' ENGINE = MergeTree() ORDER BY (`id`)',
            $result->query,
        );
    }

    public function testIndexFloatArgAvoidsScientificNotation(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->bigInteger('id')->primary()
            ->string('user_id')
            ->index(['user_id'], algorithm: IndexAlgorithm::BloomFilter, algorithmArgs: [1.0e-5])
            ->create();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('TYPE bloom_filter(0.00001)', $result->query);
        // Numeric arg should be fixed-point — no 'E-' or 'E+' anywhere
        $this->assertDoesNotMatchRegularExpression('/[Ee][+-]\d/', $result->query);
    }

    public function testAlterAddIndexWithAlgorithm(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->index(['user_id'], algorithm: IndexAlgorithm::BloomFilter)
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `events` ADD INDEX `idx_user_id` `user_id` TYPE bloom_filter GRANULARITY 1',
            $result->query,
        );
    }

    public function testAlterAddIndexComposite(): void
    {
        $schema = new Schema();
        $result = $schema->table('events')
            ->index(['user_id', 'event'], name: 'idx_user_event', algorithm: IndexAlgorithm::Set, algorithmArgs: [100], granularity: 4)
            ->alter();
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
        $schema->table('events')
            ->bigInteger('id')->primary()
            ->settings(['index_granularity' => 4096])
            ->alter();
    }
}
