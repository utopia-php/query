<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\MySQL as SQLBuilder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Query;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\Triggers;
use Utopia\Query\Schema\MySQL as Schema;

class MySQLTest extends TestCase
{
    use AssertsBindingCount;
    // Feature interfaces

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

    // CREATE TABLE

    public function testCreateTableBasic(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Blueprint $table) {
            $table->id();
            $table->string('name', 255);
            $table->string('email', 255)->unique();
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'CREATE TABLE `users` (`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL, `name` VARCHAR(255) NOT NULL, `email` VARCHAR(255) NOT NULL, PRIMARY KEY (`id`), UNIQUE (`email`))',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testCreateTableAllColumnTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('test_types', function (Blueprint $table) {
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

        $this->assertStringContainsString('INT NOT NULL', $result->query);
        $this->assertStringContainsString('BIGINT NOT NULL', $result->query);
        $this->assertStringContainsString('DOUBLE NOT NULL', $result->query);
        $this->assertStringContainsString('TINYINT(1) NOT NULL', $result->query);
        $this->assertStringContainsString('TEXT NOT NULL', $result->query);
        $this->assertStringContainsString('DATETIME(3) NOT NULL', $result->query);
        $this->assertStringContainsString('TIMESTAMP(6) NOT NULL', $result->query);
        $this->assertStringContainsString('JSON NOT NULL', $result->query);
        $this->assertStringContainsString('BLOB NOT NULL', $result->query);
        $this->assertStringContainsString("ENUM('active','inactive') NOT NULL", $result->query);
    }

    public function testCreateTableWithNullableAndDefault(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Blueprint $table) {
            $table->id();
            $table->text('bio')->nullable();
            $table->boolean('active')->default(true);
            $table->integer('score')->default(0);
            $table->string('status')->default('draft');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`bio` TEXT NULL', $result->query);
        $this->assertStringContainsString("DEFAULT 1", $result->query);
        $this->assertStringContainsString('DEFAULT 0', $result->query);
        $this->assertStringContainsString("DEFAULT 'draft'", $result->query);
    }

    public function testCreateTableWithUnsigned(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->integer('age')->unsigned();
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INT UNSIGNED NOT NULL', $result->query);
    }

    public function testCreateTableWithTimestamps(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Blueprint $table) {
            $table->id();
            $table->timestamps();
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`created_at` DATETIME(3) NOT NULL', $result->query);
        $this->assertStringContainsString('`updated_at` DATETIME(3) NOT NULL', $result->query);
    }

    public function testCreateTableWithForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Blueprint $table) {
            $table->id();
            $table->foreignKey('user_id')
                ->references('id')->on('users')
                ->onDelete('CASCADE')->onUpdate('SET NULL');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString(
            'FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL',
            $result->query
        );
    }

    public function testCreateTableWithIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->string('email');
            $table->index(['name', 'email']);
            $table->uniqueIndex(['email']);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INDEX `idx_name_email` (`name`, `email`)', $result->query);
        $this->assertStringContainsString('UNIQUE INDEX `uniq_email` (`email`)', $result->query);
    }

    public function testCreateTableWithSpatialTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('locations', function (Blueprint $table) {
            $table->id();
            $table->point('coords', 4326);
            $table->linestring('path');
            $table->polygon('area');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('POINT SRID 4326 NOT NULL', $result->query);
        $this->assertStringContainsString('LINESTRING SRID 4326 NOT NULL', $result->query);
        $this->assertStringContainsString('POLYGON SRID 4326 NOT NULL', $result->query);
    }

    public function testCreateTableVectorThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Vector type is not supported in MySQL.');

        $schema = new Schema();
        $schema->create('embeddings', function (Blueprint $table) {
            $table->vector('embedding', 768);
        });
    }

    public function testCreateTableWithComment(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->string('name')->comment('User display name');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString("COMMENT 'User display name'", $result->query);
    }
    // ALTER TABLE

    public function testAlterAddColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addColumn('avatar_url', 'string', 255)->nullable()->after('email');
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` ADD COLUMN `avatar_url` VARCHAR(255) NULL AFTER `email`',
            $result->query
        );
    }

    public function testAlterModifyColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->modifyColumn('name', 'string', 500);
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` MODIFY COLUMN `name` VARCHAR(500) NOT NULL',
            $result->query
        );
    }

    public function testAlterRenameColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->renameColumn('bio', 'biography');
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` RENAME COLUMN `bio` TO `biography`',
            $result->query
        );
    }

    public function testAlterDropColumn(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->dropColumn('age');
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` DROP COLUMN `age`',
            $result->query
        );
    }

    public function testAlterAddIndex(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addIndex('idx_name', ['name']);
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` ADD INDEX `idx_name` (`name`)',
            $result->query
        );
    }

    public function testAlterDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->dropIndex('idx_old');
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` DROP INDEX `idx_old`',
            $result->query
        );
    }

    public function testAlterAddForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addForeignKey('dept_id')
                ->references('id')->on('departments');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString(
            'ADD FOREIGN KEY (`dept_id`) REFERENCES `departments` (`id`)',
            $result->query
        );
    }

    public function testAlterDropForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->dropForeignKey('fk_old');
        });
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `users` DROP FOREIGN KEY `fk_old`',
            $result->query
        );
    }

    public function testAlterMultipleOperations(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addColumn('avatar', 'string', 255)->nullable();
            $table->dropColumn('age');
            $table->renameColumn('bio', 'biography');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ADD COLUMN', $result->query);
        $this->assertStringContainsString('DROP COLUMN `age`', $result->query);
        $this->assertStringContainsString('RENAME COLUMN `bio` TO `biography`', $result->query);
    }
    // DROP TABLE

    public function testDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('users');
        $this->assertBindingCount($result);

        $this->assertEquals('DROP TABLE `users`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testDropTableIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('users');

        $this->assertEquals('DROP TABLE IF EXISTS `users`', $result->query);
    }
    // RENAME TABLE

    public function testRenameTable(): void
    {
        $schema = new Schema();
        $result = $schema->rename('users', 'members');
        $this->assertBindingCount($result);

        $this->assertEquals('RENAME TABLE `users` TO `members`', $result->query);
    }
    // TRUNCATE TABLE

    public function testTruncateTable(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('users');
        $this->assertBindingCount($result);

        $this->assertEquals('TRUNCATE TABLE `users`', $result->query);
    }
    // CREATE / DROP INDEX (standalone)

    public function testCreateIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email']);

        $this->assertEquals('CREATE INDEX `idx_email` ON `users` (`email`)', $result->query);
    }

    public function testCreateUniqueIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email'], unique: true);

        $this->assertEquals('CREATE UNIQUE INDEX `idx_email` ON `users` (`email`)', $result->query);
    }

    public function testCreateFulltextIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('posts', 'idx_body_ft', ['body'], type: 'fulltext');

        $this->assertEquals('CREATE FULLTEXT INDEX `idx_body_ft` ON `posts` (`body`)', $result->query);
    }

    public function testCreateSpatialIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('locations', 'idx_geo', ['coords'], type: 'spatial');

        $this->assertEquals('CREATE SPATIAL INDEX `idx_geo` ON `locations` (`coords`)', $result->query);
    }

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('users', 'idx_email');

        $this->assertEquals('DROP INDEX `idx_email` ON `users`', $result->query);
    }
    // CREATE / DROP VIEW

    public function testCreateView(): void
    {
        $schema = new Schema();
        $builder = (new SQLBuilder())->from('users')->filter([Query::equal('active', [true])]);
        $result = $schema->createView('active_users', $builder);

        $this->assertEquals(
            'CREATE VIEW `active_users` AS SELECT * FROM `users` WHERE `active` IN (?)',
            $result->query
        );
        $this->assertEquals([true], $result->bindings);
    }

    public function testCreateOrReplaceView(): void
    {
        $schema = new Schema();
        $builder = (new SQLBuilder())->from('users')->filter([Query::equal('active', [true])]);
        $result = $schema->createOrReplaceView('active_users', $builder);

        $this->assertEquals(
            'CREATE OR REPLACE VIEW `active_users` AS SELECT * FROM `users` WHERE `active` IN (?)',
            $result->query
        );
        $this->assertEquals([true], $result->bindings);
    }

    public function testDropView(): void
    {
        $schema = new Schema();
        $result = $schema->dropView('active_users');

        $this->assertEquals('DROP VIEW `active_users`', $result->query);
    }
    // FOREIGN KEY (standalone)

    public function testAddForeignKeyStandalone(): void
    {
        $schema = new Schema();
        $result = $schema->addForeignKey(
            'orders',
            'fk_user',
            'user_id',
            'users',
            'id',
            onDelete: 'CASCADE',
            onUpdate: 'SET NULL'
        );

        $this->assertEquals(
            'ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL',
            $result->query
        );
    }

    public function testAddForeignKeyNoActions(): void
    {
        $schema = new Schema();
        $result = $schema->addForeignKey('orders', 'fk_user', 'user_id', 'users', 'id');

        $this->assertEquals(
            'ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)',
            $result->query
        );
    }

    public function testDropForeignKeyStandalone(): void
    {
        $schema = new Schema();
        $result = $schema->dropForeignKey('orders', 'fk_user');

        $this->assertEquals(
            'ALTER TABLE `orders` DROP FOREIGN KEY `fk_user`',
            $result->query
        );
    }
    // STORED PROCEDURE

    public function testCreateProcedure(): void
    {
        $schema = new Schema();
        $result = $schema->createProcedure(
            'update_stats',
            params: [['IN', 'user_id', 'INT'], ['OUT', 'total', 'INT']],
            body: 'SELECT COUNT(*) INTO total FROM orders WHERE orders.user_id = user_id;'
        );

        $this->assertEquals(
            'CREATE PROCEDURE `update_stats`(IN `user_id` INT, OUT `total` INT) BEGIN SELECT COUNT(*) INTO total FROM orders WHERE orders.user_id = user_id; END',
            $result->query
        );
    }

    public function testDropProcedure(): void
    {
        $schema = new Schema();
        $result = $schema->dropProcedure('update_stats');

        $this->assertEquals('DROP PROCEDURE `update_stats`', $result->query);
    }
    // TRIGGER

    public function testCreateTrigger(): void
    {
        $schema = new Schema();
        $result = $schema->createTrigger(
            'trg_updated_at',
            'users',
            timing: 'BEFORE',
            event: 'UPDATE',
            body: 'SET NEW.updated_at = NOW(3);'
        );

        $this->assertEquals(
            'CREATE TRIGGER `trg_updated_at` BEFORE UPDATE ON `users` FOR EACH ROW BEGIN SET NEW.updated_at = NOW(3); END',
            $result->query
        );
    }

    public function testDropTrigger(): void
    {
        $schema = new Schema();
        $result = $schema->dropTrigger('trg_updated_at');

        $this->assertEquals('DROP TRIGGER `trg_updated_at`', $result->query);
    }

    // Schema edge cases

    public function testCreateTableWithMultiplePrimaryKeys(): void
    {
        $schema = new Schema();
        $result = $schema->create('order_items', function (Blueprint $table) {
            $table->integer('order_id')->primary();
            $table->integer('product_id')->primary();
            $table->integer('quantity');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PRIMARY KEY (`order_id`, `product_id`)', $result->query);
    }

    public function testCreateTableWithDefaultNull(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->string('name')->nullable()->default(null);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('DEFAULT NULL', $result->query);
    }

    public function testCreateTableWithNumericDefault(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->float('score')->default(0.5);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('DEFAULT 0.5', $result->query);
    }

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('users');

        $this->assertEquals('DROP TABLE IF EXISTS `users`', $result->query);
    }

    public function testCreateOrReplaceViewFromBuilder(): void
    {
        $schema = new Schema();
        $builder = (new SQLBuilder())->from('users');
        $result = $schema->createOrReplaceView('all_users', $builder);

        $this->assertStringStartsWith('CREATE OR REPLACE VIEW', $result->query);
    }

    public function testAlterMultipleColumnsAndIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addColumn('first_name', 'string', 100);
            $table->addColumn('last_name', 'string', 100);
            $table->dropColumn('name');
            $table->addIndex('idx_names', ['first_name', 'last_name']);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ADD COLUMN `first_name`', $result->query);
        $this->assertStringContainsString('ADD COLUMN `last_name`', $result->query);
        $this->assertStringContainsString('DROP COLUMN `name`', $result->query);
        $this->assertStringContainsString('ADD INDEX `idx_names`', $result->query);
    }

    public function testCreateTableForeignKeyWithAllActions(): void
    {
        $schema = new Schema();
        $result = $schema->create('comments', function (Blueprint $table) {
            $table->id();
            $table->foreignKey('post_id')
                ->references('id')->on('posts')
                ->onDelete('CASCADE')->onUpdate('RESTRICT');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON DELETE CASCADE', $result->query);
        $this->assertStringContainsString('ON UPDATE RESTRICT', $result->query);
    }

    public function testAddForeignKeyStandaloneNoActions(): void
    {
        $schema = new Schema();
        $result = $schema->addForeignKey('orders', 'fk_user', 'user_id', 'users', 'id');

        $this->assertStringNotContainsString('ON DELETE', $result->query);
        $this->assertStringNotContainsString('ON UPDATE', $result->query);
    }

    public function testDropTriggerByName(): void
    {
        $schema = new Schema();
        $result = $schema->dropTrigger('trg_old');

        $this->assertEquals('DROP TRIGGER `trg_old`', $result->query);
    }

    public function testCreateTableTimestampWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->timestamp('ts_col');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('TIMESTAMP NOT NULL', $result->query);
        $this->assertStringNotContainsString('TIMESTAMP(', $result->query);
    }

    public function testCreateTableDatetimeWithoutPrecision(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->datetime('dt_col');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('DATETIME NOT NULL', $result->query);
        $this->assertStringNotContainsString('DATETIME(', $result->query);
    }

    public function testCreateCompositeIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_multi', ['first_name', 'last_name']);

        $this->assertEquals('CREATE INDEX `idx_multi` ON `users` (`first_name`, `last_name`)', $result->query);
    }

    public function testAlterAddAndDropForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->alter('orders', function (Blueprint $table) {
            $table->addForeignKey('user_id')->references('id')->on('users');
            $table->dropForeignKey('fk_old_user');
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ADD FOREIGN KEY', $result->query);
        $this->assertStringContainsString('DROP FOREIGN KEY `fk_old_user`', $result->query);
    }

    public function testBlueprintAutoGeneratedIndexName(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->string('first');
            $table->string('last');
            $table->index(['first', 'last']);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INDEX `idx_first_last`', $result->query);
    }

    public function testBlueprintAutoGeneratedUniqueIndexName(): void
    {
        $schema = new Schema();
        $result = $schema->create('t', function (Blueprint $table) {
            $table->string('email');
            $table->uniqueIndex(['email']);
        });
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNIQUE INDEX `uniq_email`', $result->query);
    }

    public function testExactCreateTableWithColumnsAndIndexes(): void
    {
        $schema = new Schema();
        $result = $schema->create('products', function (Blueprint $table) {
            $table->id();
            $table->string('name', 100);
            $table->integer('price');
            $table->boolean('active')->default(true);
            $table->index(['name']);
            $table->uniqueIndex(['price']);
        });

        $this->assertSame(
            'CREATE TABLE `products` (`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL, `name` VARCHAR(100) NOT NULL, `price` INT NOT NULL, `active` TINYINT(1) NOT NULL DEFAULT 1, PRIMARY KEY (`id`), INDEX `idx_name` (`name`), UNIQUE INDEX `uniq_price` (`price`))',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAlterTableAddAndDropColumns(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->addColumn('phone', 'string', 20)->nullable();
            $table->dropColumn('legacy_field');
        });

        $this->assertSame(
            'ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20) NULL, DROP COLUMN `legacy_field`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactCreateTableWithForeignKey(): void
    {
        $schema = new Schema();
        $result = $schema->create('orders', function (Blueprint $table) {
            $table->id();
            $table->integer('customer_id');
            $table->foreignKey('customer_id')
                ->references('id')->on('customers')
                ->onDelete('CASCADE')->onUpdate('CASCADE');
        });

        $this->assertSame(
            'CREATE TABLE `orders` (`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL, `customer_id` INT NOT NULL, PRIMARY KEY (`id`), FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE ON UPDATE CASCADE)',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDropTable(): void
    {
        $schema = new Schema();
        $result = $schema->drop('sessions');

        $this->assertSame('DROP TABLE `sessions`', $result->query);
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }
}
