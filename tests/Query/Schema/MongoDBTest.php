<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\MongoDB as Builder;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Query;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\MongoDB as Schema;

class MongoDBTest extends TestCase
{
    public function testCreateCollection(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Blueprint $table) {
            $table->id('id');
            $table->string('name');
            $table->string('email');
            $table->integer('age');
        });

        $op = $this->decode($result->query);
        $this->assertEquals('createCollection', $op['command']);
        $this->assertEquals('users', $op['collection']);
        $this->assertArrayHasKey('validator', $op);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        $this->assertArrayHasKey('$jsonSchema', $validator);
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        $this->assertEquals('object', $jsonSchema['bsonType']);
        /** @var array<string, mixed> $properties */
        $properties = $jsonSchema['properties'];
        $this->assertArrayHasKey('id', $properties);
        $this->assertArrayHasKey('name', $properties);
    }

    public function testCreateCollectionWithTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('posts', function (Blueprint $table) {
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
        $this->assertEquals('int', $props['id']['bsonType']);
        $this->assertEquals('string', $props['title']['bsonType']);
        $this->assertEquals('string', $props['body']['bsonType']);
        $this->assertEquals('int', $props['views']['bsonType']);
        $this->assertEquals('double', $props['rating']['bsonType']);
        $this->assertEquals('bool', $props['published']['bsonType']);
        $this->assertEquals('date', $props['created_at']['bsonType']);
    }

    public function testCreateCollectionWithEnumValidation(): void
    {
        $schema = new Schema();
        $result = $schema->create('tasks', function (Blueprint $table) {
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
        $this->assertEquals('string', $statusProp['bsonType']);
        $this->assertEquals(['pending', 'active', 'completed'], $statusProp['enum']);
    }

    public function testCreateCollectionWithRequired(): void
    {
        $schema = new Schema();
        $result = $schema->create('users', function (Blueprint $table) {
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

    public function testDrop(): void
    {
        $schema = new Schema();
        $result = $schema->drop('users');

        $op = $this->decode($result->query);
        $this->assertEquals('drop', $op['command']);
        $this->assertEquals('users', $op['collection']);
    }

    public function testDropIfExists(): void
    {
        $schema = new Schema();
        $result = $schema->dropIfExists('users');

        $op = $this->decode($result->query);
        $this->assertEquals('drop', $op['command']);
        $this->assertEquals('users', $op['collection']);
    }

    public function testRename(): void
    {
        $schema = new Schema();
        $result = $schema->rename('old_users', 'new_users');

        $op = $this->decode($result->query);
        $this->assertEquals('renameCollection', $op['command']);
        $this->assertEquals('old_users', $op['from']);
        $this->assertEquals('new_users', $op['to']);
    }

    public function testTruncate(): void
    {
        $schema = new Schema();
        $result = $schema->truncate('users');

        $op = $this->decode($result->query);
        $this->assertEquals('deleteMany', $op['command']);
        $this->assertEquals('users', $op['collection']);
    }

    public function testCreateIndex(): void
    {
        $schema = new Schema();
        $result = $schema->createIndex('users', 'idx_email', ['email'], true);

        $op = $this->decode($result->query);
        $this->assertEquals('createIndex', $op['command']);
        $this->assertEquals('users', $op['collection']);
        /** @var array<string, mixed> $index */
        $index = $op['index'];
        $this->assertEquals(['email' => 1], $index['key']);
        $this->assertEquals('idx_email', $index['name']);
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
        $this->assertEquals(['user_id' => 1, 'action' => -1], $index['key']);
    }

    public function testDropIndex(): void
    {
        $schema = new Schema();
        $result = $schema->dropIndex('users', 'idx_email');

        $op = $this->decode($result->query);
        $this->assertEquals('dropIndex', $op['command']);
        $this->assertEquals('users', $op['collection']);
        $this->assertEquals('idx_email', $op['index']);
    }

    public function testAnalyzeTable(): void
    {
        $schema = new Schema();
        $result = $schema->analyzeTable('users');

        $op = $this->decode($result->query);
        $this->assertEquals('collStats', $op['command']);
        $this->assertEquals('users', $op['collection']);
    }

    public function testCreateDatabase(): void
    {
        $schema = new Schema();
        $result = $schema->createDatabase('mydb');

        $op = $this->decode($result->query);
        $this->assertEquals('createDatabase', $op['command']);
        $this->assertEquals('mydb', $op['database']);
    }

    public function testDropDatabase(): void
    {
        $schema = new Schema();
        $result = $schema->dropDatabase('mydb');

        $op = $this->decode($result->query);
        $this->assertEquals('dropDatabase', $op['command']);
        $this->assertEquals('mydb', $op['database']);
    }

    public function testAlter(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->string('phone');
            $table->boolean('verified');
        });

        $op = $this->decode($result->query);
        $this->assertEquals('collMod', $op['command']);
        $this->assertEquals('users', $op['collection']);
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
        $result = $schema->create('users', function (Blueprint $table) {
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
        $this->assertEquals('The display name', $nameProp['description']);
    }

    public function testAlterWithMultipleColumns(): void
    {
        $schema = new Schema();
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->string('phone');
            $table->integer('age');
            $table->boolean('verified');
        });

        $op = $this->decode($result->query);
        $this->assertEquals('collMod', $op['command']);
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
        $result = $schema->alter('users', function (Blueprint $table) {
            $table->string('phone')->comment('User phone number');
        });

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $validator */
        $validator = $op['validator'];
        /** @var array<string, mixed> $jsonSchema */
        $jsonSchema = $validator['$jsonSchema'];
        /** @var array<string, array<string, mixed>> $props */
        $props = $jsonSchema['properties'];
        $this->assertEquals('User phone number', $props['phone']['description']);
    }

    public function testAlterDropColumnThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('MongoDB does not support dropping or renaming columns via schema');

        $schema = new Schema();
        $schema->alter('users', function (Blueprint $table) {
            $table->dropColumn('old_field');
        });
    }

    public function testAlterRenameColumnThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('MongoDB does not support dropping or renaming columns via schema');

        $schema = new Schema();
        $schema->alter('users', function (Blueprint $table) {
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
        $this->assertEquals('createView', $op['command']);
        $this->assertEquals('active_users', $op['view']);
        $this->assertEquals('users', $op['source']);
        $this->assertArrayHasKey('pipeline', $op);
        $this->assertEquals([true], $result->bindings);
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
        $this->assertEquals('createView', $op['command']);
        $this->assertEquals('order_counts', $op['view']);
        $this->assertEquals('orders', $op['source']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $this->assertNotEmpty($pipeline);
    }

    public function testCreateCollectionWithAllBsonTypes(): void
    {
        $schema = new Schema();
        $result = $schema->create('all_types', function (Blueprint $table) {
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
        $this->assertEquals('object', $props['meta']['bsonType']);
        $this->assertEquals('binData', $props['data']['bsonType']);
        $this->assertEquals('object', $props['location']['bsonType']);
        $this->assertEquals('object', $props['path']['bsonType']);
        $this->assertEquals('object', $props['area']['bsonType']);
        $this->assertEquals('string', $props['uid']['bsonType']);
        $this->assertEquals('array', $props['embedding']['bsonType']);
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
