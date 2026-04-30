<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\SQLite as SQLBuilder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\Triggers;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\ParameterDirection;
use Utopia\Query\Schema\SQLite as Schema;
use Utopia\Query\Schema\TriggerEvent;
use Utopia\Query\Schema\TriggerTiming;

class SQLiteTest extends TestCase
{
    use AssertsBindingCount;

    public function testImplementsForeignKeys(): void
    {
        $this->assertInstanceOf(ForeignKeys::class, new Schema());
    }

    public function testImplementsProcedures(): void
    {
        $this->assertInstanceOf(Procedures::class, new Schema());
    }

    public function testImplementsTriggers(): void
    {
        $this->assertInstanceOf(Triggers::class, new Schema());
    }

    public function testCreateTableBasic(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->id()
            ->string('name', 255)
            ->string('email', 255)->unique()
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `users` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `name` VARCHAR(255) NOT NULL, `email` VARCHAR(255) NOT NULL, UNIQUE (`email`))',
            $result->query
        );
        $this->assertSame([], $result->bindings);
    }

    public function testCreateTableAllColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->table('test_types')
            ->integer('int_col')
            ->bigInteger('big_col')
            ->float('float_col')
            ->boolean('bool_col')
            ->text('text_col')
            ->datetime('dt_col', 3)
            ->timestamp('ts_col', 6)
            ->json('json_col')
            ->binary('bin_col')
            ->enum('status', ['active', 'inactive'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `test_types` (`int_col` INTEGER NOT NULL, `big_col` INTEGER NOT NULL, `float_col` REAL NOT NULL, `bool_col` INTEGER NOT NULL, `text_col` TEXT NOT NULL, `dt_col` TEXT NOT NULL, `ts_col` TEXT NOT NULL, `json_col` TEXT NOT NULL, `bin_col` BLOB NOT NULL, `status` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeStringMapsToVarchar(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->string('name', 100)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` VARCHAR(100) NOT NULL)', $result->query);
    }

    public function testColumnTypeBooleanMapsToInteger(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->boolean('active')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`active` INTEGER NOT NULL)', $result->query);
    }

    public function testColumnTypeDatetimeMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->datetime('created_at')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`created_at` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeTimestampMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->timestamp('updated_at')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`updated_at` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeJsonMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->json('data')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`data` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeBinaryMapsToBlob(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->binary('content')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`content` BLOB NOT NULL)', $result->query);
    }

    public function testColumnTypeEnumMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->enum('status', ['a', 'b'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`status` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeSpatialMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->point('coords', 4326)
            ->linestring('path')
            ->polygon('area')
            ->create();
        $this->assertBindingCount($result);

        $count = substr_count($result->query, 'TEXT NOT NULL');
        $this->assertGreaterThanOrEqual(3, $count);
    }

    public function testColumnTypeUuid7MapsToVarchar36(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->string('uid', 36)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`uid` VARCHAR(36) NOT NULL)', $result->query);
    }

    public function testColumnTypeVectorThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Vector type is not supported in SQLite.');

        $schema = new Schema();
        $schema->table('t')
            ->vector('embedding', 768)
            ->create();
    }

    public function testAutoIncrementUsesAutoincrement(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->id()
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL)', $result->query);
        $this->assertStringNotContainsString('AUTO_INCREMENT', $result->query);
    }

    public function testUnsignedIsEmptyString(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->integer('age')->unsigned()
            ->create();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('UNSIGNED', $result->query);
    }

    public function testCreateDatabaseThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('SQLite does not support CREATE DATABASE.');

        $schema = new Schema();
        $schema->createDatabase('mydb');
    }

    public function testDropDatabaseThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('SQLite does not support DROP DATABASE.');

        $schema = new Schema();
        $schema->dropDatabase('mydb');
    }

    public function testRenameUsesAlterTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('old_table')->rename('new_table');
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `old_table` RENAME TO `new_table`',
            $result->query
        );
        $this->assertSame([], $result->bindings);
    }

    public function testTruncateUsesDeleteFrom(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')->truncate();
        $this->assertBindingCount($result);

        $this->assertSame('DELETE FROM `users`', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testDropIndexWithoutTableName(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('users', 'idx_email');
        $this->assertBindingCount($result);

        $this->assertSame('DROP INDEX `idx_email`', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testRenameIndexThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('SQLite does not support renaming indexes directly.');

        $schema = new Schema();
        $schema->renameIndex('users', 'old_idx', 'new_idx');
    }

    public function testCreateTableWithNullableAndDefault(): void
    {
        $schema = new Schema();
        $result = $schema->table('posts')
            ->id()
            ->text('bio')->nullable()
            ->boolean('active')->default(true)
            ->integer('score')->default(0)
            ->string('status')->default('draft')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `bio` TEXT NULL, `active` INTEGER NOT NULL DEFAULT 1, `score` INTEGER NOT NULL DEFAULT 0, `status` VARCHAR(255) NOT NULL DEFAULT \'draft\')', $result->query);
    }

    public function testCreateTableWithForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->table('posts')
            ->id()
            ->foreignKey('user_id')
                ->references('id')->on('users')
                ->onDelete(ForeignKeyAction::Cascade)->onUpdate(ForeignKeyAction::SetNull)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL)', $result->query);
    }

    public function testCreateTableWithIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->id()
            ->string('name')
            ->string('email')
            ->index(['name', 'email'])
            ->uniqueIndex(['email'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `users` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `name` VARCHAR(255) NOT NULL, `email` VARCHAR(255) NOT NULL, INDEX `idx_name_email` (`name`, `email`), UNIQUE INDEX `uniq_email` (`email`))', $result->query);
    }

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')->drop();
        $this->assertBindingCount($result);

        $this->assertSame('DROP TABLE `users`', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testDropTableIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')->dropIfExists();

        $this->assertSame('DROP TABLE IF EXISTS `users`', $result->query);
    }

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->addColumn('avatar_url', ColumnType::String, 255)->nullable()
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `users` ADD COLUMN `avatar_url` VARCHAR(255) NULL', $result->query);
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->dropColumn('age')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `users` DROP COLUMN `age`',
            $result->query
        );
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->renameColumn('bio', 'biography')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `users` RENAME COLUMN `bio` TO `biography`',
            $result->query
        );
    }

    public function testCreateIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email']);

        $this->assertSame('CREATE INDEX `idx_email` ON `users` (`email`)', $result->query);
    }

    public function testCreateUniqueIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email'], unique: true);

        $this->assertSame('CREATE UNIQUE INDEX `idx_email` ON `users` (`email`)', $result->query);
    }

    public function testCreateView(): void
    {
        $schema = new Schema();
        $builder = (new SQLBuilder())->from('users')->filter([Query::equal('active', [true])]);
        $result = $schema->createView('active_users', $builder);

        $this->assertSame(
            'CREATE VIEW `active_users` AS SELECT * FROM `users` WHERE `active` IN (?)',
            $result->query
        );
        $this->assertSame([true], $result->bindings);
    }

    public function testCreateOrReplaceView(): void
    {
        $schema = new Schema();
        $builder = (new SQLBuilder())->from('users')->filter([Query::equal('active', [true])]);
        $result = $schema->createOrReplaceView('active_users', $builder);

        $this->assertSame(
            'CREATE OR REPLACE VIEW `active_users` AS SELECT * FROM `users` WHERE `active` IN (?)',
            $result->query
        );
        $this->assertSame([true], $result->bindings);
    }

    public function testDropView(): void
    {
        $schema = new Schema();
        $result = $schema->dropView('active_users');

        $this->assertSame('DROP VIEW `active_users`', $result->query);
    }

    public function testAddForeignKeyStandalone(): void
    {
        $schema = new Schema();
        $result = $schema->addForeignKey(
            'orders',
            'fk_user',
            'user_id',
            'users',
            'id',
            onDelete: ForeignKeyAction::Cascade,
            onUpdate: ForeignKeyAction::SetNull
        );

        $this->assertSame(
            'ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL',
            $result->query
        );
    }

    public function testAddForeignKeyNoActions(): void
    {
        $schema = new Schema();
        $result = $schema->addForeignKey('orders', 'fk_user', 'user_id', 'users', 'id');

        $this->assertStringNotContainsString('ON DELETE', $result->query);
        $this->assertStringNotContainsString('ON UPDATE', $result->query);
    }

    public function testDropForeignKeyStandalone(): void
    {
        $schema = new Schema();
        $result = $schema->dropForeignKey('orders', 'fk_user');

        $this->assertSame(
            'ALTER TABLE `orders` DROP FOREIGN KEY `fk_user`',
            $result->query
        );
    }

    public function testCreateProcedure(): void
    {
        $schema = new Schema();
        $result = $schema->createProcedure(
            'update_stats',
            params: [[ParameterDirection::In, 'user_id', 'INT'], [ParameterDirection::Out, 'total', 'INT']],
            body: 'SELECT COUNT(*) INTO total FROM orders WHERE orders.user_id = user_id;'
        );

        $this->assertSame(
            'CREATE PROCEDURE `update_stats`(IN `user_id` INT, OUT `total` INT) BEGIN SELECT COUNT(*) INTO total FROM orders WHERE orders.user_id = user_id; END',
            $result->query
        );
    }

    public function testDropProcedure(): void
    {
        $schema = new Schema();
        $result = $schema->dropProcedure('update_stats');

        $this->assertSame('DROP PROCEDURE `update_stats`', $result->query);
    }

    public function testCreateTrigger(): void
    {
        $schema = new Schema();
        $result = $schema->createTrigger(
            'trg_updated_at',
            'users',
            timing: TriggerTiming::Before,
            event: TriggerEvent::Update,
            body: 'SET NEW.updated_at = datetime();'
        );

        $this->assertSame(
            'CREATE TRIGGER `trg_updated_at` BEFORE UPDATE ON `users` FOR EACH ROW BEGIN SET NEW.updated_at = datetime(); END',
            $result->query
        );
    }

    public function testDropTrigger(): void
    {
        $schema = new Schema();
        $result = $schema->dropTrigger('trg_updated_at');

        $this->assertSame('DROP TRIGGER `trg_updated_at`', $result->query);
    }

    public function testCreateTableWithMultiplePrimaryKeys(): void
    {
        $schema = new Schema();
        $result = $schema->table('order_items')
            ->integer('order_id')->primary()
            ->integer('product_id')->primary()
            ->integer('quantity')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `order_items` (`order_id` INTEGER NOT NULL, `product_id` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, PRIMARY KEY (`order_id`, `product_id`))', $result->query);
    }

    public function testCreateTableWithCompositePrimaryKey(): void
    {
        $schema = new Schema();
        $result = $schema->table('order_items')
            ->integer('order_id')
            ->integer('product_id')
            ->integer('quantity')
            ->primary(['order_id', 'product_id'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `order_items` (`order_id` INTEGER NOT NULL, `product_id` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, PRIMARY KEY (`order_id`, `product_id`))', $result->query);
    }

    public function testCreateTableRejectsMixedColumnAndTablePrimary(): void
    {
        $schema = new Schema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Cannot combine column-level primary() with Table::primary() composite key.');

        $schema->table('order_items')
            ->integer('order_id')->primary()
            ->integer('product_id')
            ->primary(['order_id', 'product_id'])
            ->create();
    }

    public function testCreateTableWithDefaultNull(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->string('name')->nullable()->default(null)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` VARCHAR(255) NULL DEFAULT NULL)', $result->query);
    }

    public function testCreateTableWithNumericDefault(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->float('score')->default(0.5)
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`score` REAL NOT NULL DEFAULT 0.5)', $result->query);
    }

    public function testCreateTableWithTimestamps(): void
    {
        $schema = new Schema();
        $result = $schema->table('posts')
            ->id()
            ->timestamps()
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `created_at` TEXT NOT NULL, `updated_at` TEXT NOT NULL)', $result->query);
    }

    public function testExactCreateTableWithColumnsAndIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->table('products')
            ->id()
            ->string('name', 100)
            ->integer('price')
            ->index(['name'])
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `products` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `name` VARCHAR(100) NOT NULL, `price` INTEGER NOT NULL, INDEX `idx_name` (`name`))',
            $result->query
        );
        $this->assertSame([], $result->bindings);
    }

    public function testExactDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('sessions')->drop();

        $this->assertSame('DROP TABLE `sessions`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactRenameTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('old_name')->rename('new_name');

        $this->assertSame('ALTER TABLE `old_name` RENAME TO `new_name`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->table('logs')->truncate();

        $this->assertSame('DELETE FROM `logs`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('users', 'idx_email');

        $this->assertSame('DROP INDEX `idx_email`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactCreateTableWithForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->table('orders')
            ->id()
            ->integer('customer_id')
            ->foreignKey('customer_id')
                ->references('id')->on('customers')
                ->onDelete(ForeignKeyAction::Cascade)->onUpdate(ForeignKeyAction::Cascade)
            ->create();

        $this->assertSame(
            'CREATE TABLE `orders` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `customer_id` INTEGER NOT NULL, FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE)',
            $result->query
        );
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testColumnTypeFloatMapsToReal(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->float('ratio')
            ->create();
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`ratio` REAL NOT NULL)', $result->query);
    }

    public function testCreateIfNotExists(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->integer('id')->primary()
            ->createIfNotExists();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('CREATE TABLE IF NOT EXISTS', $result->query);
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->table('users')
            ->addColumn('avatar', ColumnType::String, 255)->nullable()
            ->dropColumn('age')
            ->renameColumn('bio', 'biography')
            ->alter();
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `users` ADD COLUMN `avatar` VARCHAR(255) NULL, RENAME COLUMN `bio` TO `biography`, DROP COLUMN `age`', $result->query);
    }

    public function testSerialColumnMapsToInteger(): void
    {
        $schema = new Schema();
        $result = $schema->table('t')
            ->serial('id')->primary()
            ->create();

        $this->assertSame('CREATE TABLE `t` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL)', $result->query);
    }

    public function testUserTypeColumnThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);

        $schema = new Schema();
        $schema->table('t')
            ->integer('id')->primary()
            ->string('mood')->userType('mood_type')
            ->create();
    }
}
