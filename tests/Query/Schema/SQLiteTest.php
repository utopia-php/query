<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\SQLite as SQLBuilder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\Triggers;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\ParameterDirection;
use Utopia\Query\Schema\SQLite as Schema;
use Utopia\Query\Schema\Table;
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
        $result = $schema->create('users', function (Table $table) {
            $table->id();
            $table->string('name', 255);
            $table->string('email', 255)->unique();
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `users` (`id` INTEGER AUTOINCREMENT NOT NULL, `name` VARCHAR(255) NOT NULL, `email` VARCHAR(255) NOT NULL, PRIMARY KEY (`id`), UNIQUE (`email`))',
            $result->query
        );
        $this->assertSame([], $result->bindings);
    }

    public function testCreateTableAllColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('test_types', function (Table $table) {
            $table->integer('int_col');
            $table->bigInteger('big_col');
            $table->float('float_col');
            $table->boolean('bool_col');
            $table->text('text_col');
            $table->datetime('dt_col', 3);
            $table->timestamp('ts_col', 6);
            $table->json('json_col');
            $table->binary('bin_col');
            $table->enum('status', ['active', 'inactive']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `test_types` (`int_col` INTEGER NOT NULL, `big_col` INTEGER NOT NULL, `float_col` REAL NOT NULL, `bool_col` INTEGER NOT NULL, `text_col` TEXT NOT NULL, `dt_col` TEXT NOT NULL, `ts_col` TEXT NOT NULL, `json_col` TEXT NOT NULL, `bin_col` BLOB NOT NULL, `status` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeStringMapsToVarchar(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->string('name', 100);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` VARCHAR(100) NOT NULL)', $result->query);
    }

    public function testColumnTypeBooleanMapsToInteger(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->boolean('active');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`active` INTEGER NOT NULL)', $result->query);
    }

    public function testColumnTypeDatetimeMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->datetime('created_at');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`created_at` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeTimestampMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->timestamp('updated_at');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`updated_at` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeJsonMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->json('data');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`data` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeBinaryMapsToBlob(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->binary('content');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`content` BLOB NOT NULL)', $result->query);
    }

    public function testColumnTypeEnumMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->enum('status', ['a', 'b']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`status` TEXT NOT NULL)', $result->query);
    }

    public function testColumnTypeSpatialMapsToText(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->point('coords', 4326);
            $table->linestring('path');
            $table->polygon('area');
        });
        $this->assertBindingCount($result);

        $count = substr_count($result->query, 'TEXT NOT NULL');
        $this->assertGreaterThanOrEqual(3, $count);
    }

    public function testColumnTypeUuid7MapsToVarchar36(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->string('uid', 36);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`uid` VARCHAR(36) NOT NULL)', $result->query);
    }

    public function testColumnTypeVectorThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Vector type is not supported in SQLite.');

        $schema = new Schema();
        $schema->create('t', function (Table $table) {
            $table->vector('embedding', 768);
        });
    }

    public function testAutoIncrementUsesAutoincrement(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->id();
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`id` INTEGER AUTOINCREMENT NOT NULL, PRIMARY KEY (`id`))', $result->query);
        $this->assertStringNotContainsString('AUTO_INCREMENT', $result->query);
    }

    public function testUnsignedIsEmptyString(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->integer('age')->unsigned();
        });
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
        $result = $schema->rename('old_table', 'new_table');
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
        $result = $schema->truncate('users');
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
        $result = $schema->create('posts', function (Table $table) {
            $table->id();
            $table->text('bio')->nullable();
            $table->boolean('active')->default(true);
            $table->integer('score')->default(0);
            $table->string('status')->default('draft');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER AUTOINCREMENT NOT NULL, `bio` TEXT NULL, `active` INTEGER NOT NULL DEFAULT 1, `score` INTEGER NOT NULL DEFAULT 0, `status` VARCHAR(255) NOT NULL DEFAULT \'draft\', PRIMARY KEY (`id`))', $result->query);
    }

    public function testCreateTableWithForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Table $table) {
            $table->id();
            $table->foreignKey('user_id')
                ->references('id')->on('users')
                ->onDelete(ForeignKeyAction::Cascade)->onUpdate(ForeignKeyAction::SetNull);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER AUTOINCREMENT NOT NULL, PRIMARY KEY (`id`), FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL)', $result->query);
    }

    public function testCreateTableWithIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Table $table) {
            $table->id();
            $table->string('name');
            $table->string('email');
            $table->index(['name', 'email']);
            $table->uniqueIndex(['email']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `users` (`id` INTEGER AUTOINCREMENT NOT NULL, `name` VARCHAR(255) NOT NULL, `email` VARCHAR(255) NOT NULL, PRIMARY KEY (`id`), INDEX `idx_name_email` (`name`, `email`), UNIQUE INDEX `uniq_email` (`email`))', $result->query);
    }

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('users');
        $this->assertBindingCount($result);

        $this->assertSame('DROP TABLE `users`', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testDropTableIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('users');

        $this->assertSame('DROP TABLE IF EXISTS `users`', $result->query);
    }

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->addColumn('avatar_url', 'string', 255)->nullable();
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `users` ADD COLUMN `avatar_url` VARCHAR(255) NULL', $result->query);
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->dropColumn('age');
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'ALTER TABLE `users` DROP COLUMN `age`',
            $result->query
        );
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->renameColumn('bio', 'biography');
        });
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
        $result = $schema->create('order_items', function (Table $table) {
            $table->integer('order_id')->primary();
            $table->integer('product_id')->primary();
            $table->integer('quantity');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `order_items` (`order_id` INTEGER NOT NULL, `product_id` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, PRIMARY KEY (`order_id`, `product_id`))', $result->query);
    }

    public function testCreateTableWithCompositePrimaryKey(): void
    {
        $schema = new Schema();
        $result = $schema->create('order_items', function (Table $table) {
            $table->integer('order_id');
            $table->integer('product_id');
            $table->integer('quantity');
            $table->primary(['order_id', 'product_id']);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `order_items` (`order_id` INTEGER NOT NULL, `product_id` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, PRIMARY KEY (`order_id`, `product_id`))', $result->query);
    }

    public function testCreateTableRejectsMixedColumnAndTablePrimary(): void
    {
        $schema = new Schema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Cannot combine column-level primary() with Table::primary() composite key.');

        $schema->create('order_items', function (Table $table) {
            $table->integer('order_id')->primary();
            $table->integer('product_id');
            $table->primary(['order_id', 'product_id']);
        });
    }

    public function testCreateTableWithDefaultNull(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->string('name')->nullable()->default(null);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`name` VARCHAR(255) NULL DEFAULT NULL)', $result->query);
    }

    public function testCreateTableWithNumericDefault(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->float('score')->default(0.5);
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`score` REAL NOT NULL DEFAULT 0.5)', $result->query);
    }

    public function testCreateTableWithTimestamps(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Table $table) {
            $table->id();
            $table->timestamps();
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `posts` (`id` INTEGER AUTOINCREMENT NOT NULL, `created_at` TEXT NOT NULL, `updated_at` TEXT NOT NULL, PRIMARY KEY (`id`))', $result->query);
    }

    public function testExactCreateTableWithColumnsAndIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('products', function (Table $table) {
            $table->id();
            $table->string('name', 100);
            $table->integer('price');
            $table->index(['name']);
        });
        $this->assertBindingCount($result);

        $this->assertSame(
            'CREATE TABLE `products` (`id` INTEGER AUTOINCREMENT NOT NULL, `name` VARCHAR(100) NOT NULL, `price` INTEGER NOT NULL, PRIMARY KEY (`id`), INDEX `idx_name` (`name`))',
            $result->query
        );
        $this->assertSame([], $result->bindings);
    }

    public function testExactDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('sessions');

        $this->assertSame('DROP TABLE `sessions`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactRenameTable(): void
    {
        $schema = new Schema();
        $result = $schema->rename('old_name', 'new_name');

        $this->assertSame('ALTER TABLE `old_name` RENAME TO `new_name`', $result->query);
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('logs');

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
        $result = $schema->create('orders', function (Table $table) {
            $table->id();
            $table->integer('customer_id');
            $table->foreignKey('customer_id')
                ->references('id')->on('customers')
                ->onDelete(ForeignKeyAction::Cascade)->onUpdate(ForeignKeyAction::Cascade);
        });

        $this->assertSame(
            'CREATE TABLE `orders` (`id` INTEGER AUTOINCREMENT NOT NULL, `customer_id` INTEGER NOT NULL, PRIMARY KEY (`id`), FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE)',
            $result->query
        );
        $this->assertSame([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testColumnTypeFloatMapsToReal(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->float('ratio');
        });
        $this->assertBindingCount($result);

        $this->assertSame('CREATE TABLE `t` (`ratio` REAL NOT NULL)', $result->query);
    }

    public function testCreateIfNotExists(): void
    {
        $schema = new Schema();
        $result = $schema->createIfNotExists('t', function (Table $table) {
            $table->integer('id')->primary();
        });
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('CREATE TABLE IF NOT EXISTS', $result->query);
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->addColumn('avatar', 'string', 255)->nullable();
            $table->dropColumn('age');
            $table->renameColumn('bio', 'biography');
        });
        $this->assertBindingCount($result);

        $this->assertSame('ALTER TABLE `users` ADD COLUMN `avatar` VARCHAR(255) NULL, RENAME COLUMN `bio` TO `biography`, DROP COLUMN `age`', $result->query);
    }

    public function testSerialColumnMapsToInteger(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Table $table) {
            $table->serial('id')->primary();
        });

        $this->assertSame('CREATE TABLE `t` (`id` INTEGER AUTOINCREMENT NOT NULL, PRIMARY KEY (`id`))', $result->query);
    }

    public function testUserTypeColumnThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);

        $schema = new Schema();
        $schema->create('t', function (Table $table) {
            $table->integer('id')->primary();
            $table->string('mood')->userType('mood_type');
        });
    }
}
