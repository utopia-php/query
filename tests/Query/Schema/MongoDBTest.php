<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\MongoDB as Builder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\MongoDB as Schema;
use Utopia\Query\Schema\Table;

class MongoDBTest extends TestCase
{
    public function testCreateCollection(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Table $table) {
            $table->id('id');
            $table->string('name');
            $table->string('email');
            $table->integer('age');
        });

        $op = $this->decode($result->query);
        $this->assertSame('createCollection', $op['command']);
        $this->assertSame('users', $op['collection']);
        $this->assertArrayHasKey('validator', $op);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        $this->assertArrayHasKey('$jsonSchema', $validator);
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        $this->assertSame('object', $jsonSchema['bsonType']);
        /** @var array<string, mixed> $properties */
        $properties = $jsonSchema['properties'];
        $this->assertArrayHasKey('id', $properties);
        $this->assertArrayHasKey('name', $properties);
    }

    public function testCreateCollectionWithTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Table $table) {
            $table->id('id');
            $table->string('title');
            $table->text('body');
            $table->integer('views');
            $table->float('rating');
            $table->boolean('published');
            $table->datetime('created_at');
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $props */
        $props = $jsonSchema['properties'];
        $this->assertSame('int', $props['id']['bsonType']);
        $this->assertSame('string', $props['title']['bsonType']);
        $this->assertSame('string', $props['body']['bsonType']);
        $this->assertSame('int', $props['views']['bsonType']);
        $this->assertSame('double', $props['rating']['bsonType']);
        $this->assertSame('bool', $props['published']['bsonType']);
        $this->assertSame('date', $props['created_at']['bsonType']);
    }

    public function testCreateCollectionWithEnumValidation(): void
    {
        $schema = new Schema();
        $result = $schema->create('tasks', function (Table $table) {
            $table->id('id');
            $table->enum('status', ['pending', 'active', 'completed']);
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $properties */
        $properties = $jsonSchema['properties'];
        $statusProp = $properties['status'];
        $this->assertSame('string', $statusProp['bsonType']);
        $this->assertSame(['pending', 'active', 'completed'], $statusProp['enum']);
    }

    public function testCreateCollectionWithRequired(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Table $table) {
            $table->id('id');
            $table->string('name');
            $table->string('email')->nullable();
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var list<string> $required */
        $required = $jsonSchema['required'];
        $this->assertContains('id', $required);
        $this->assertContains('name', $required);
        $this->assertNotContains('email', $required);
    }

    public function testCreateCollectionRejectsCompositePrimaryKey(): void
    {
        $schema = new Schema();

        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Composite primary keys are not supported in MongoDB');

        $schema->create('order_items', function (Table $table) {
            $table->integer('order_id');
            $table->integer('product_id');
            $table->primary(['order_id', 'product_id']);
        });
    }

    public function testDrop(): void
    {
        $schema = new Schema();
        $result = $schema->drop('users');

        $op = $this->decode($result->query);
        $this->assertSame('drop', $op['command']);
        $this->assertSame('users', $op['collection']);
    }

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('users');

        $op = $this->decode($result->query);
        $this->assertSame('drop', $op['command']);
        $this->assertSame('users', $op['collection']);
    }

    public function testRename(): void
    {
        $schema = new Schema();
        $result = $schema->rename('old_users', 'new_users');

        $op = $this->decode($result->query);
        $this->assertSame('renameCollection', $op['command']);
        $this->assertSame('old_users', $op['from']);
        $this->assertSame('new_users', $op['to']);
    }

    public function testTruncate(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('users');

        $op = $this->decode($result->query);
        $this->assertSame('deleteMany', $op['command']);
        $this->assertSame('users', $op['collection']);
    }

    public function testCreateIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email'], true);

        $op = $this->decode($result->query);
        $this->assertSame('createIndex', $op['command']);
        $this->assertSame('users', $op['collection']);
        /** @var array<string, mixed> $index */
        $index = $op['index'];
        $this->assertSame(['email' => 1], $index['key']);
        $this->assertSame('idx_email', $index['name']);
        $this->assertTrue($index['unique']);
    }

    public function testCreateCompoundIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex(
            'events',
            'idx_user_action',
            ['user_id', 'action'],
            orders: ['action' => 'desc'],
        );

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $index */
        $index = $op['index'];
        $this->assertSame(['user_id' => 1, 'action' => -1], $index['key']);
    }

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('users', 'idx_email');

        $op = $this->decode($result->query);
        $this->assertSame('dropIndex', $op['command']);
        $this->assertSame('users', $op['collection']);
        $this->assertSame('idx_email', $op['index']);
    }

    public function testAnalyzeTable(): void
    {
        $schema = new Schema();
        $result = $schema->analyzeTable('users');

        $op = $this->decode($result->query);
        $this->assertSame('collStats', $op['command']);
        $this->assertSame('users', $op['collection']);
    }

    public function testCreateDatabase(): void
    {
        $schema = new Schema();
        $result = $schema->createDatabase('mydb');

        $op = $this->decode($result->query);
        $this->assertSame('createDatabase', $op['command']);
        $this->assertSame('mydb', $op['database']);
    }

    public function testDropDatabase(): void
    {
        $schema = new Schema();
        $result = $schema->dropDatabase('mydb');

        $op = $this->decode($result->query);
        $this->assertSame('dropDatabase', $op['command']);
        $this->assertSame('mydb', $op['database']);
    }

    public function testAlter(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->string('phone');
            $table->boolean('verified');
        });

        $op = $this->decode($result->query);
        $this->assertSame('collMod', $op['command']);
        $this->assertSame('users', $op['collection']);
        $this->assertArrayHasKey('validator', $op);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, mixed> $props */
        $props = $jsonSchema['properties'];
        $this->assertArrayHasKey('phone', $props);
        $this->assertArrayHasKey('verified', $props);
    }

    public function testColumnComment(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Table $table) {
            $table->string('name')->comment('The display name');
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $properties */
        $properties = $jsonSchema['properties'];
        $nameProp = $properties['name'];
        $this->assertSame('The display name', $nameProp['description']);
    }

    public function testAlterWithMultipleColumns(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->string('phone');
            $table->integer('age');
            $table->boolean('verified');
        });

        $op = $this->decode($result->query);
        $this->assertSame('collMod', $op['command']);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, mixed> $props */
        $props = $jsonSchema['properties'];
        $this->assertArrayHasKey('phone', $props);
        $this->assertArrayHasKey('age', $props);
        $this->assertArrayHasKey('verified', $props);
        /** @var list<string> $required */
        $required = $jsonSchema['required'];
        $this->assertContains('phone', $required);
        $this->assertContains('age', $required);
        $this->assertContains('verified', $required);
    }

    public function testAlterWithColumnComment(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Table $table) {
            $table->string('phone')->comment('User phone number');
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $props */
        $props = $jsonSchema['properties'];
        $this->assertSame('User phone number', $props['phone']['description']);
    }

    public function testAlterDropColumnThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('MongoDB does not support dropping or renaming columns via schema');

        $schema = new Schema();
        $schema->alter('users', function (Table $table) {
            $table->dropColumn('old_field');
        });
    }

    public function testAlterRenameColumnThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('MongoDB does not support dropping or renaming columns via schema');

        $schema = new Schema();
        $schema->alter('users', function (Table $table) {
            $table->renameColumn('old_name', 'new_name');
        });
    }

    public function testCreateView(): void
    {
        $schema = new Schema();
        $builder = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('active', [true])]);

        $result = $schema->createView('active_users', $builder);

        $op = $this->decode($result->query);
        $this->assertSame('createView', $op['command']);
        $this->assertSame('active_users', $op['view']);
        $this->assertSame('users', $op['source']);
        $this->assertArrayHasKey('pipeline', $op);
        $this->assertSame([true], $result->bindings);
    }

    public function testCreateViewFromAggregation(): void
    {
        $schema = new Schema();
        $builder = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['user_id']);

        $result = $schema->createView('order_counts', $builder);

        $op = $this->decode($result->query);
        $this->assertSame('createView', $op['command']);
        $this->assertSame('order_counts', $op['view']);
        $this->assertSame('orders', $op['source']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $this->assertNotEmpty($pipeline);
    }

    public function testCreateCollectionWithAllBsonTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('all_types', function (Table $table) {
            $table->json('meta');
            $table->binary('data');
            $table->point('location');
            $table->linestring('path');
            $table->polygon('area');
            $table->addColumn('uid', ColumnType::Uuid7);
            $table->vector('embedding', 768);
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $props */
        $props = $jsonSchema['properties'];
        $this->assertSame('object', $props['meta']['bsonType']);
        $this->assertSame('binData', $props['data']['bsonType']);
        $this->assertSame('object', $props['location']['bsonType']);
        $this->assertSame('object', $props['path']['bsonType']);
        $this->assertSame('object', $props['area']['bsonType']);
        $this->assertSame('string', $props['uid']['bsonType']);
        $this->assertSame('array', $props['embedding']['bsonType']);
    }

    /**
     * @return array<string, mixed>
     */
    private function decode(string $json): array
    {
        /** @var array<string, mixed> */
        return \json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    }
}
