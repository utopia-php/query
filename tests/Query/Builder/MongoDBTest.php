<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\Feature\Aggregates;
use Utopia\Query\Builder\Feature\CTEs;
use Utopia\Query\Builder\Feature\Deletes;
use Utopia\Query\Builder\Feature\FullTextSearch;
use Utopia\Query\Builder\Feature\Hooks;
use Utopia\Query\Builder\Feature\Inserts;
use Utopia\Query\Builder\Feature\Joins;
use Utopia\Query\Builder\Feature\Selects;
use Utopia\Query\Builder\Feature\TableSampling;
use Utopia\Query\Builder\Feature\Unions;
use Utopia\Query\Builder\Feature\Updates;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Builder\Feature\Windows;
use Utopia\Query\Builder\MongoDB as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;

class MongoDBTest extends TestCase
{
    use AssertsBindingCount;

    public function testImplementsCompiler(): void
    {
        $this->assertInstanceOf(Compiler::class, new Builder());
    }

    public function testImplementsSelects(): void
    {
        $this->assertInstanceOf(Selects::class, new Builder());
    }

    public function testImplementsAggregates(): void
    {
        $this->assertInstanceOf(Aggregates::class, new Builder());
    }

    public function testImplementsJoins(): void
    {
        $this->assertInstanceOf(Joins::class, new Builder());
    }

    public function testImplementsUnions(): void
    {
        $this->assertInstanceOf(Unions::class, new Builder());
    }

    public function testImplementsCTEs(): void
    {
        $this->assertInstanceOf(CTEs::class, new Builder());
    }

    public function testImplementsInserts(): void
    {
        $this->assertInstanceOf(Inserts::class, new Builder());
    }

    public function testImplementsUpdates(): void
    {
        $this->assertInstanceOf(Updates::class, new Builder());
    }

    public function testImplementsDeletes(): void
    {
        $this->assertInstanceOf(Deletes::class, new Builder());
    }

    public function testImplementsHooks(): void
    {
        $this->assertInstanceOf(Hooks::class, new Builder());
    }

    public function testImplementsWindows(): void
    {
        $this->assertInstanceOf(Windows::class, new Builder());
    }

    public function testImplementsUpsert(): void
    {
        $this->assertInstanceOf(Upsert::class, new Builder());
    }

    public function testImplementsFullTextSearch(): void
    {
        $this->assertInstanceOf(FullTextSearch::class, new Builder());
    }

    public function testImplementsTableSampling(): void
    {
        $this->assertInstanceOf(TableSampling::class, new Builder());
    }

    public function testBasicSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('users', $op['collection']);
        $this->assertEquals('find', $op['operation']);
        $this->assertEquals(['name' => 1, 'email' => 1, '_id' => 0], $op['projection']);
        $this->assertEmpty($result->bindings);
    }

    public function testSelectAll(): void
    {
        $result = (new Builder())
            ->from('users')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
        $this->assertArrayNotHasKey('projection', $op);
    }

    public function testFilterEqual(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['status' => '?'], $op['filter']);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testFilterEqualMultipleValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active', 'pending'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['status' => ['$in' => ['?', '?']]], $op['filter']);
        $this->assertEquals(['active', 'pending'], $result->bindings);
    }

    public function testFilterNotEqual(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEqual('status', ['deleted'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['status' => ['$ne' => '?']], $op['filter']);
        $this->assertEquals(['deleted'], $result->bindings);
    }

    public function testFilterNotEqualMultipleValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEqual('status', ['deleted', 'banned'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['status' => ['$nin' => ['?', '?']]], $op['filter']);
        $this->assertEquals(['deleted', 'banned'], $result->bindings);
    }

    public function testFilterGreaterThan(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::greaterThan('age', 25)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['age' => ['$gt' => '?']], $op['filter']);
        $this->assertEquals([25], $result->bindings);
    }

    public function testFilterLessThan(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::lessThan('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['age' => ['$lt' => '?']], $op['filter']);
        $this->assertEquals([30], $result->bindings);
    }

    public function testFilterGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::greaterThanEqual('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['age' => ['$gte' => '?']], $op['filter']);
        $this->assertEquals([18], $result->bindings);
    }

    public function testFilterLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::lessThanEqual('age', 65)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['age' => ['$lte' => '?']], $op['filter']);
        $this->assertEquals([65], $result->bindings);
    }

    public function testFilterBetween(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['age' => ['$gte' => '?', '$lte' => '?']], $op['filter']);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testFilterNotBetween(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notBetween('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['age' => ['$lt' => '?']],
            ['age' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testFilterStartsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::startsWith('name', 'Al')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['^Al'], $result->bindings);
    }

    public function testFilterEndsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::endsWith('email', '.com')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['\.com$'], $result->bindings);
    }

    public function testFilterContains(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::contains('name', ['test'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['test'], $result->bindings);
    }

    public function testFilterNotContains(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notContains('name', ['test'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => ['$not' => ['$regex' => '?']]], $op['filter']);
        $this->assertEquals(['test'], $result->bindings);
    }

    public function testFilterRegex(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::regex('email', '^[a-z]+@test\\.com$')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['^[a-z]+@test\\.com$'], $result->bindings);
    }

    public function testFilterIsNull(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::isNull('deleted_at')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['deleted_at' => null], $op['filter']);
        $this->assertEmpty($result->bindings);
    }

    public function testFilterIsNotNull(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::isNotNull('email')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$ne' => null]], $op['filter']);
        $this->assertEmpty($result->bindings);
    }

    public function testFilterOr(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::or([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['status' => '?'],
            ['age' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['active', 18], $result->bindings);
    }

    public function testFilterAnd(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::and([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['status' => '?'],
            ['age' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['active', 18], $result->bindings);
    }

    public function testMultipleFiltersProduceAnd(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 25),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['status' => '?'],
            ['age' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['active', 25], $result->bindings);
    }

    public function testSortAscAndDesc(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => 1, 'age' => -1], $op['sort']);
    }

    public function testLimitAndOffset(): void
    {
        $result = (new Builder())
            ->from('users')
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(10, $op['limit']);
        $this->assertEquals(20, $op['skip']);
    }

    public function testInsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@test.com', 'age' => 30])
            ->insert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('users', $op['collection']);
        $this->assertEquals('insertMany', $op['operation']);
        /** @var list<array<string, mixed>> $documents */
        $documents = $op['documents'];
        $this->assertCount(1, $documents);
        $this->assertEquals(['name' => '?', 'email' => '?', 'age' => '?'], $documents[0]);
        $this->assertEquals(['Alice', 'alice@test.com', 30], $result->bindings);
    }

    public function testInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'age' => 25])
            ->insert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $documents */
        $documents = $op['documents'];
        $this->assertCount(2, $documents);
        $this->assertEquals(['Alice', 30, 'Bob', 25], $result->bindings);
    }

    public function testUpdateWithSet(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['city' => 'New York'])
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('users', $op['collection']);
        $this->assertEquals('updateMany', $op['operation']);
        $this->assertEquals(['$set' => ['city' => '?']], $op['update']);
        $this->assertEquals(['name' => '?'], $op['filter']);
        $this->assertEquals(['New York', 'Alice'], $result->bindings);
    }

    public function testUpdateWithIncrement(): void
    {
        $result = (new Builder())
            ->from('users')
            ->increment('login_count', 1)
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$inc' => ['login_count' => 1]], $op['update']);
        $this->assertEquals(['Alice'], $result->bindings);
    }

    public function testUpdateWithPush(): void
    {
        $result = (new Builder())
            ->from('users')
            ->push('tags', 'admin')
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$push' => ['tags' => '?']], $op['update']);
        $this->assertEquals(['admin', 'Alice'], $result->bindings);
    }

    public function testUpdateWithPull(): void
    {
        $result = (new Builder())
            ->from('users')
            ->pull('tags', 'guest')
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$pull' => ['tags' => '?']], $op['update']);
        $this->assertEquals(['guest', 'Alice'], $result->bindings);
    }

    public function testUpdateWithAddToSet(): void
    {
        $result = (new Builder())
            ->from('users')
            ->addToSet('roles', 'editor')
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$addToSet' => ['roles' => '?']], $op['update']);
        $this->assertEquals(['editor', 'Alice'], $result->bindings);
    }

    public function testUpdateWithUnset(): void
    {
        $result = (new Builder())
            ->from('users')
            ->unsetFields('deprecated_field')
            ->filter([Query::equal('name', ['Alice'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$unset' => ['deprecated_field' => '']], $op['update']);
        $this->assertEquals(['Alice'], $result->bindings);
    }

    public function testDelete(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['deleted'])])
            ->delete();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('users', $op['collection']);
        $this->assertEquals('deleteMany', $op['operation']);
        $this->assertEquals(['status' => '?'], $op['filter']);
        $this->assertEquals(['deleted'], $result->bindings);
    }

    public function testDeleteWithoutFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->delete();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('deleteMany', $op['operation']);
        $this->assertEmpty((array) $op['filter']);
    }

    public function testGroupByWithCount(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['country'])
            ->count('*', 'cnt')
            ->groupBy(['country'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals('$country', $groupBody['_id']);
        $this->assertEquals(['$sum' => 1], $groupBody['cnt']);
    }

    public function testGroupByWithMultipleAggregates(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->sum('amount', 'total')
            ->avg('amount', 'average')
            ->groupBy(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals(['$sum' => '$amount'], $groupBody['total']);
        $this->assertEquals(['$avg' => '$amount'], $groupBody['average']);
    }

    public function testGroupByWithHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupIdx = $this->findStageIndex($pipeline, '$group');
        $this->assertNotNull($groupIdx);

        // HAVING $match should come after $group
        $matchStages = [];
        for ($i = $groupIdx + 1; $i < \count($pipeline); $i++) {
            if (isset($pipeline[$i]['$match'])) {
                $matchStages[] = $pipeline[$i];
            }
        }
        $this->assertNotEmpty($matchStages);
    }

    public function testDistinct(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['country'])
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
    }

    public function testJoin(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['orders.id', 'users.name'])
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('users', $lookupBody['from']);
        $this->assertEquals('user_id', $lookupBody['localField']);
        $this->assertEquals('id', $lookupBody['foreignField']);
        $this->assertEquals('u', $lookupBody['as']);
    }

    public function testLeftJoin(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->leftJoin('users', 'orders.user_id', 'users.id', '=', 'u')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $unwindStage = $this->findStage($pipeline, '$unwind');
        $this->assertNotNull($unwindStage);
        $this->assertIsArray($unwindStage['$unwind']);
        /** @var array<string, mixed> $unwindBody */
        $unwindBody = $unwindStage['$unwind'];
        $this->assertTrue($unwindBody['preserveNullAndEmptyArrays']);
    }

    public function testUnionAll(): void
    {
        $first = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('country', ['US'])]);

        $second = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('country', ['UK'])]);

        $result = $first->unionAll($second)->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $unionStage = $this->findStage($pipeline, '$unionWith');
        $this->assertNotNull($unionStage);
        /** @var array<string, mixed> $unionBody */
        $unionBody = $unionStage['$unionWith'];
        $this->assertEquals('users', $unionBody['coll']);
    }

    public function testUpsert(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['email' => 'alice@test.com', 'name' => 'Alice Updated', 'age' => 31])
            ->onConflict(['email'], ['name', 'age'])
            ->upsert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('updateOne', $op['operation']);
        $this->assertEquals(['email' => '?'], $op['filter']);
        $this->assertEquals(['$set' => ['name' => '?', 'age' => '?']], $op['update']);
        /** @var array<string, mixed> $options */
        $options = $op['options'];
        $this->assertTrue($options['upsert']);
        $this->assertEquals(['alice@test.com', 'Alice Updated', 31], $result->bindings);
    }

    public function testInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@test.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('insertMany', $op['operation']);
        /** @var array<string, mixed> $options */
        $options = $op['options'];
        $this->assertFalse($options['ordered']);
    }

    public function testWindowFunction(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-amount'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        $this->assertArrayHasKey('rn', $output);
    }

    public function testFilterWhereInSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::equal('status', ['completed'])]);

        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filterWhereIn('id', $subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('orders', $lookupBody['from']);
    }

    public function testSortRandom(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $addFieldsStage = $this->findStage($pipeline, '$addFields');
        $this->assertNotNull($addFieldsStage);
        /** @var array<string, mixed> $addFieldsBody */
        $addFieldsBody = $addFieldsStage['$addFields'];
        $this->assertArrayHasKey('_rand', $addFieldsBody);
    }

    public function testTextSearch(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->filterSearch('content', 'mongodb tutorial')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        /** @var array<string, mixed> $matchStage */
        $matchStage = $pipeline[0];
        /** @var array<string, mixed> $matchBody */
        $matchBody = $matchStage['$match'];
        $this->assertArrayHasKey('$text', $matchBody);
        /** @var array<string, mixed> $textBody */
        $textBody = $matchBody['$text'];
        $this->assertEquals('?', $textBody['$search']);
        $this->assertEquals(['mongodb tutorial'], $result->bindings);
    }

    public function testNoTableThrowsException(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->select(['name'])->build();
    }

    public function testInsertWithoutRowsThrowsException(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->into('users')->insert();
    }

    public function testUpdateWithoutOperationsThrowsException(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->from('users')->update();
    }

    public function testReset(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])])
            ->push('tags', 'test')
            ->increment('counter', 1);

        $builder->reset();

        $this->expectException(ValidationException::class);
        $builder->build();
    }

    public function testFindOperationForSimpleQuery(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('country', ['US'])])
            ->sortAsc('name')
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
        $this->assertEquals(['name' => 1, '_id' => 0], $op['projection']);
        $this->assertEquals(['country' => '?'], $op['filter']);
        $this->assertEquals(['name' => 1], $op['sort']);
        $this->assertEquals(10, $op['limit']);
        $this->assertEquals(5, $op['skip']);
    }

    public function testAggregateOperationForGroupBy(): void
    {
        $result = (new Builder())
            ->from('users')
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
    }

    public function testClone(): void
    {
        $original = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])]);

        $cloned = $original->clone();
        $cloned->filter([Query::greaterThan('age', 25)]);

        $originalResult = $original->build();
        $clonedResult = $cloned->build();

        $this->assertCount(1, $originalResult->bindings);
        $this->assertCount(2, $clonedResult->bindings);
    }

    public function testMultipleGroupByColumns(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['country', 'city'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals([
            'country' => '$country',
            'city' => '$city',
        ], $groupBody['_id']);
    }

    public function testMinMaxAggregates(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->min('amount', 'min_amount')
            ->max('amount', 'max_amount')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals(['$min' => '$amount'], $groupBody['min_amount']);
        $this->assertEquals(['$max' => '$amount'], $groupBody['max_amount']);
    }

    public function testFilterEqualWithNull(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('deleted_at', [null])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['deleted_at' => null], $op['filter']);
        $this->assertEmpty($result->bindings);
    }

    public function testFilterContainsMultipleValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::contains('bio', ['php', 'java'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['bio' => ['$regex' => '?']],
            ['bio' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['php', 'java'], $result->bindings);
    }

    public function testFilterContainsAll(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::containsAll('bio', ['php', 'java'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['bio' => ['$regex' => '?']],
            ['bio' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['php', 'java'], $result->bindings);
    }

    public function testFilterNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notStartsWith('name', 'Test')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => ['$not' => ['$regex' => '?']]], $op['filter']);
        $this->assertEquals(['^Test'], $result->bindings);
    }

    public function testUpdateWithMultipleOperators(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'Alice Updated'])
            ->increment('login_count', 1)
            ->push('tags', 'updated')
            ->unsetFields('temp_field')
            ->filter([Query::equal('_id', ['abc123'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        $this->assertArrayHasKey('$set', $update);
        $this->assertArrayHasKey('$inc', $update);
        $this->assertArrayHasKey('$push', $update);
        $this->assertArrayHasKey('$unset', $update);
    }

    public function testPage(): void
    {
        $result = (new Builder())
            ->from('users')
            ->page(3, 10)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(10, $op['limit']);
        $this->assertEquals(20, $op['skip']);
    }

    public function testTableSampling(): void
    {
        $result = (new Builder())
            ->from('users')
            ->tablesample(100)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $sampleStage = $this->findStage($pipeline, '$sample');
        $this->assertNotNull($sampleStage);
        /** @var array<string, mixed> $sampleBody */
        $sampleBody = $sampleStage['$sample'];
        $this->assertEquals(100, $sampleBody['size']);
    }

    public function testFilterNotSearchThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('MongoDB does not support negated full-text search.');

        (new Builder())
            ->from('articles')
            ->filterNotSearch('content', 'bad term');
    }

    public function testFilterExistsSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::greaterThan('total', 100)]);

        $result = (new Builder())
            ->from('users')
            ->filterExists($subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('orders', $lookupBody['from']);
        /** @var string $lookupAs */
        $lookupAs = $lookupBody['as'];
        $this->assertStringStartsWith('_exists_', $lookupAs);

        $matchStages = [];
        foreach ($pipeline as $stage) {
            if (isset($stage['$match'])) {
                $matchStages[] = $stage;
            }
        }
        $this->assertNotEmpty($matchStages);

        $unsetStage = $this->findStage($pipeline, '$unset');
        $this->assertNotNull($unsetStage);
    }

    public function testFilterNotExistsSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterNotExists($subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $hasExistsMatch = false;
        foreach ($pipeline as $stage) {
            if (isset($stage['$match'])) {
                /** @var array<string, mixed> $matchBody */
                $matchBody = $stage['$match'];
                foreach ($matchBody as $key => $val) {
                    if (\str_starts_with($key, '_exists_') && \is_array($val) && isset($val['$size'])) {
                        $hasExistsMatch = true;
                        $this->assertEquals(0, $val['$size']);
                    }
                }
            }
        }
        $this->assertTrue($hasExistsMatch);
    }

    public function testFilterWhereNotInSubquery(): void
    {
        $subquery = (new Builder())
            ->from('banned_users')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterWhereNotIn('id', $subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $hasNotIn = false;
        foreach ($pipeline as $stage) {
            if (isset($stage['$match'])) {
                $json = \json_encode($stage['$match']);
                if ($json !== false && \str_contains($json, '$not')) {
                    $hasNotIn = true;
                }
            }
        }
        $this->assertTrue($hasNotIn);
    }

    public function testWindowFunctionWithSumAggregation(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id', 'amount'])
            ->selectWindow('SUM(amount)', 'running_total', ['user_id'], ['amount'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        $this->assertArrayHasKey('running_total', $output);
        /** @var array<string, mixed> $runningTotal */
        $runningTotal = $output['running_total'];
        $this->assertEquals('$amount', $runningTotal['$sum']);
        $this->assertArrayHasKey('window', $runningTotal);
    }

    public function testWindowFunctionWithAvg(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('AVG(price)', 'avg_price', ['category'], ['-price'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        $this->assertArrayHasKey('avg_price', $output);
        /** @var array<string, mixed> $avgPrice */
        $avgPrice = $output['avg_price'];
        $this->assertEquals('$price', $avgPrice['$avg']);
    }

    public function testWindowFunctionWithMin(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->selectWindow('MIN(amount)', 'min_amount', ['region'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        /** @var array<string, mixed> $minAmount */
        $minAmount = $output['min_amount'];
        $this->assertEquals('$amount', $minAmount['$min']);
    }

    public function testWindowFunctionWithMax(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->selectWindow('MAX(amount)', 'max_amount', ['region'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        /** @var array<string, mixed> $maxAmount */
        $maxAmount = $output['max_amount'];
        $this->assertEquals('$amount', $maxAmount['$max']);
    }

    public function testWindowFunctionWithCount(): void
    {
        $result = (new Builder())
            ->from('events')
            ->selectWindow('COUNT(id)', 'event_count', ['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        /** @var array<string, mixed> $eventCount */
        $eventCount = $output['event_count'];
        $this->assertEquals(1, $eventCount['$sum']);
    }

    public function testWindowFunctionUnsupportedThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Unsupported window function');

        (new Builder())
            ->from('orders')
            ->selectWindow('MEDIAN(amount)', 'med', ['user_id'])
            ->build();
    }

    public function testWindowFunctionUnsupportedNonParenthesizedThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Unsupported window function');

        (new Builder())
            ->from('orders')
            ->selectWindow('custom_func', 'cf', ['user_id'])
            ->build();
    }

    public function testWindowFunctionMultiplePartitionKeys(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('ROW_NUMBER()', 'rn', ['country', 'city'], ['amount'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        $this->assertEquals([
            'country' => '$country',
            'city' => '$city',
        ], $windowBody['partitionBy']);
    }

    public function testWindowFunctionRankAndDenseRank(): void
    {
        $result = (new Builder())
            ->from('scores')
            ->selectWindow('RANK()', 'rnk', ['category'], ['score'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, mixed> $output */
        $output = $windowBody['output'];
        $this->assertArrayHasKey('rnk', $output);

        $resultDense = (new Builder())
            ->from('scores')
            ->selectWindow('DENSE_RANK()', 'dense_rnk', ['category'], ['score'])
            ->build();
        $this->assertBindingCount($resultDense);

        $opDense = $this->decode($resultDense->query);
        /** @var list<array<string, mixed>> $pipelineDense */
        $pipelineDense = $opDense['pipeline'];
        $windowStageDense = $this->findStage($pipelineDense, '$setWindowFields');
        $this->assertNotNull($windowStageDense);
        /** @var array<string, mixed> $windowBodyDense */
        $windowBodyDense = $windowStageDense['$setWindowFields'];
        /** @var array<string, mixed> $outputDense */
        $outputDense = $windowBodyDense['output'];
        $this->assertArrayHasKey('dense_rnk', $outputDense);
    }

    public function testWindowFunctionWithOrderByAsc(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('ROW_NUMBER()', 'rn', null, ['created_at'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);
        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        /** @var array<string, int> $sortBy */
        $sortBy = $windowBody['sortBy'];
        $this->assertEquals(1, $sortBy['created_at']);
    }

    public function testFilterEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active', null])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['status' => ['$in' => ['?']]],
            ['status' => null],
        ]], $op['filter']);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testFilterNotEqualWithNullOnly(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEqual('status', [null])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['status' => ['$ne' => null]], $op['filter']);
        $this->assertEmpty($result->bindings);
    }

    public function testFilterNotEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEqual('status', ['deleted', null])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['status' => ['$nin' => ['?']]],
            ['status' => ['$ne' => null]],
        ]], $op['filter']);
        $this->assertEquals(['deleted'], $result->bindings);
    }

    public function testFilterNotContainsMultipleValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notContains('bio', ['spam', 'junk'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['bio' => ['$not' => ['$regex' => '?']]],
            ['bio' => ['$not' => ['$regex' => '?']]],
        ]], $op['filter']);
        $this->assertEquals(['spam', 'junk'], $result->bindings);
    }

    public function testFilterFieldExists(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::exists(['email'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$ne' => null]], $op['filter']);
    }

    public function testFilterFieldNotExists(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notExists(['email'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => null], $op['filter']);
    }

    public function testFilterFieldExistsMultiple(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::exists(['email', 'phone'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['email' => ['$ne' => null]],
            ['phone' => ['$ne' => null]],
        ]], $op['filter']);
    }

    public function testFilterFieldNotExistsMultiple(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notExists(['email', 'phone'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['email' => null],
            ['phone' => null],
        ]], $op['filter']);
    }

    public function testContainsAnyOnArray(): void
    {
        $query = Query::containsAny('tags', ['php', 'js']);
        $query->setOnArray(true);

        $result = (new Builder())
            ->from('users')
            ->filter([$query])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['tags' => ['$in' => ['?', '?']]], $op['filter']);
        $this->assertEquals(['php', 'js'], $result->bindings);
    }

    public function testContainsAnyOnString(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::containsAny('bio', ['php', 'js'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['bio' => ['$regex' => '?']],
            ['bio' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['php', 'js'], $result->bindings);
    }

    public function testUpsertSelectThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('upsertSelect() is not supported in MongoDB builder.');

        (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->onConflict(['email'], ['name'])
            ->upsertSelect();
    }

    public function testUpsertWithoutExplicitUpdateColumns(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['email' => 'alice@test.com', 'name' => 'Alice', 'age' => 30])
            ->onConflict(['email'], [])
            ->upsert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('updateOne', $op['operation']);
        $this->assertEquals(['email' => '?'], $op['filter']);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        /** @var array<string, mixed> $setDoc */
        $setDoc = $update['$set'];
        $this->assertArrayHasKey('name', $setDoc);
        $this->assertArrayHasKey('age', $setDoc);
        $this->assertArrayNotHasKey('email', $setDoc);
    }

    public function testUpsertMissingConflictKeyThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage("Conflict key 'email' not found in row data.");

        (new Builder())
            ->into('users')
            ->set(['name' => 'Alice'])
            ->onConflict(['email'], ['name'])
            ->upsert();
    }

    public function testAggregateWithLimitAndOffset(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['user_id'])
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $skipStage = $this->findStage($pipeline, '$skip');
        $this->assertNotNull($skipStage);
        $this->assertEquals(20, $skipStage['$skip']);

        $limitStage = $this->findStage($pipeline, '$limit');
        $this->assertNotNull($limitStage);
        $this->assertEquals(10, $limitStage['$limit']);
    }

    public function testAggregateDefaultSortDoesNotThrow(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
    }

    public function testAggregationWithNoAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertArrayHasKey('count', $groupBody);
    }

    public function testUnsupportedAggregationThrows(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Unsupported aggregation for MongoDB');

        (new Builder())
            ->from('orders')
            ->queries([new Query(\Utopia\Query\Method::CountDistinct, 'id', ['cd'])])
            ->build();
    }

    public function testBeforeBuildCallback(): void
    {
        $result = (new Builder())
            ->from('users')
            ->beforeBuild(function (Builder $builder) {
                $builder->filter([Query::equal('injected', ['yes'])]);
            })
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['injected' => '?'], $op['filter']);
        $this->assertEquals(['yes'], $result->bindings);
    }

    public function testAfterBuildCallback(): void
    {
        $result = (new Builder())
            ->from('users')
            ->afterBuild(function ($result) {
                return $result;
            })
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
    }

    public function testFilterNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEndsWith('email', '.com')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$not' => ['$regex' => '?']]], $op['filter']);
        $this->assertEquals(['\.com$'], $result->bindings);
    }

    public function testEmptyHavingReturnsEmpty(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->having([Query::and([])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
    }

    public function testHavingWithMultipleConditions(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->groupBy(['user_id'])
            ->having([
                Query::greaterThan('cnt', 5),
                Query::greaterThan('total', 100),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupIdx = $this->findStageIndex($pipeline, '$group');
        $this->assertNotNull($groupIdx);

        $havingMatches = [];
        for ($i = $groupIdx + 1; $i < \count($pipeline); $i++) {
            if (isset($pipeline[$i]['$match'])) {
                $havingMatches[] = $pipeline[$i];
            }
        }
        $this->assertNotEmpty($havingMatches);
    }

    public function testUnionWithFindOperation(): void
    {
        $first = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('country', ['US'])])
            ->sortAsc('name')
            ->limit(10)
            ->offset(5);

        $second = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('country', ['UK'])]);

        $result = $first->unionAll($second)->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $unionStage = $this->findStage($pipeline, '$unionWith');
        $this->assertNotNull($unionStage);
    }

    public function testEmptyOrLogical(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::or([])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertArrayHasKey('filter', $op);
    }

    /**
     * @return array<string, mixed>
     */
    private function decode(string $json): array
    {
        /** @var array<string, mixed> */
        return \json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @param list<array<string, mixed>> $pipeline
     * @return array<string, mixed>|null
     */
    private function findStage(array $pipeline, string $stageName): ?array
    {
        foreach ($pipeline as $stage) {
            if (isset($stage[$stageName])) {
                return $stage;
            }
        }

        return null;
    }

    /**
     * @param list<array<string, mixed>> $pipeline
     */
    private function findStageIndex(array $pipeline, string $stageName): ?int
    {
        foreach ($pipeline as $idx => $stage) {
            if (isset($stage[$stageName])) {
                return $idx;
            }
        }

        return null;
    }

    /**
     * @param list<array<string, mixed>> $pipeline
     * @return list<array<string, mixed>>
     */
    private function findAllStages(array $pipeline, string $stageName): array
    {
        $found = [];
        foreach ($pipeline as $stage) {
            if (isset($stage[$stageName])) {
                $found[] = $stage;
            }
        }

        return $found;
    }

    public function testJoinWithWhereGroupByHavingOrderLimitOffset(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->filter([Query::greaterThan('amount', 10)])
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->groupBy(['u.country'])
            ->having([Query::greaterThan('cnt', 2)])
            ->sortDesc('total')
            ->limit(20)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);

        $matchStage = $this->findStage($pipeline, '$match');
        $this->assertNotNull($matchStage);

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);

        $sortStage = $this->findStage($pipeline, '$sort');
        $this->assertNotNull($sortStage);

        $skipStage = $this->findStage($pipeline, '$skip');
        $this->assertNotNull($skipStage);
        $this->assertEquals(5, $skipStage['$skip']);

        $limitStage = $this->findStage($pipeline, '$limit');
        $this->assertNotNull($limitStage);
        $this->assertEquals(20, $limitStage['$limit']);

        $lookupIdx = $this->findStageIndex($pipeline, '$lookup');
        $matchIdx = $this->findStageIndex($pipeline, '$match');
        $groupIdx = $this->findStageIndex($pipeline, '$group');
        $sortIdx = $this->findStageIndex($pipeline, '$sort');
        $skipIdx = $this->findStageIndex($pipeline, '$skip');
        $limitIdx = $this->findStageIndex($pipeline, '$limit');

        $this->assertNotNull($lookupIdx);
        $this->assertNotNull($matchIdx);
        $this->assertNotNull($groupIdx);
        $this->assertNotNull($sortIdx);
        $this->assertNotNull($skipIdx);
        $this->assertNotNull($limitIdx);

        $this->assertLessThan($matchIdx, $lookupIdx);
        $this->assertLessThan($groupIdx, $matchIdx);
        $this->assertLessThan($sortIdx, $groupIdx);
        $this->assertLessThan($skipIdx, $sortIdx);
        $this->assertLessThan($limitIdx, $skipIdx);
    }

    public function testMultipleJoinsWithWhere(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->join('products', 'orders.product_id', 'products.id', '=', 'p')
            ->filter([Query::greaterThan('amount', 50)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStages = $this->findAllStages($pipeline, '$lookup');
        $this->assertCount(2, $lookupStages);

        /** @var array<string, mixed> $lookup1 */
        $lookup1 = $lookupStages[0]['$lookup'];
        $this->assertEquals('users', $lookup1['from']);
        $this->assertEquals('u', $lookup1['as']);

        /** @var array<string, mixed> $lookup2 */
        $lookup2 = $lookupStages[1]['$lookup'];
        $this->assertEquals('products', $lookup2['from']);
        $this->assertEquals('p', $lookup2['as']);

        $this->assertEquals([50], $result->bindings);
    }

    public function testLeftJoinAndInnerJoinCombined(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->leftJoin('users', 'orders.user_id', 'users.id', '=', 'u')
            ->join('categories', 'orders.category_id', 'categories.id', '=', 'c')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $unwindStages = $this->findAllStages($pipeline, '$unwind');
        $this->assertCount(2, $unwindStages);

        /** @var array<string, mixed>|string $firstUnwind */
        $firstUnwind = $unwindStages[0]['$unwind'];
        $this->assertIsArray($firstUnwind);
        $this->assertTrue($firstUnwind['preserveNullAndEmptyArrays']);

        /** @var string $secondUnwind */
        $secondUnwind = $unwindStages[1]['$unwind'];
        $this->assertEquals('$c', $secondUnwind);
    }

    public function testJoinWithAggregateGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->count('*', 'order_count')
            ->sum('orders.amount', 'total_amount')
            ->groupBy(['u.name'])
            ->having([Query::greaterThan('order_count', 3)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals(['$sum' => 1], $groupBody['order_count']);
        $this->assertEquals(['$sum' => '$orders.amount'], $groupBody['total_amount']);

        $groupIdx = $this->findStageIndex($pipeline, '$group');
        $this->assertNotNull($groupIdx);
        $havingMatches = [];
        for ($i = $groupIdx + 1; $i < \count($pipeline); $i++) {
            if (isset($pipeline[$i]['$match'])) {
                $havingMatches[] = $pipeline[$i];
            }
        }
        $this->assertNotEmpty($havingMatches);
    }

    public function testJoinWithWindowFunction(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->selectWindow('ROW_NUMBER()', 'rn', ['u.country'], ['-orders.amount'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);

        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);

        $lookupIdx = $this->findStageIndex($pipeline, '$lookup');
        $windowIdx = $this->findStageIndex($pipeline, '$setWindowFields');
        $this->assertNotNull($lookupIdx);
        $this->assertNotNull($windowIdx);
        $this->assertLessThan($windowIdx, $lookupIdx);
    }

    public function testJoinWithDistinct(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->select(['u.country'])
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
    }

    public function testFilterWhereInSubqueryWithJoin(): void
    {
        $subquery = (new Builder())
            ->from('premium_users')
            ->select(['user_id'])
            ->filter([Query::equal('tier', ['gold'])]);

        $result = (new Builder())
            ->from('orders')
            ->join('products', 'orders.product_id', 'products.id', '=', 'p')
            ->filterWhereIn('user_id', $subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStages = $this->findAllStages($pipeline, '$lookup');
        $this->assertGreaterThanOrEqual(2, \count($lookupStages));

        $this->assertEquals(['gold'], $result->bindings);
    }

    public function testFilterWhereInSubqueryWithAggregate(): void
    {
        $subquery = (new Builder())
            ->from('active_users')
            ->select(['id'])
            ->filter([Query::equal('status', ['active'])]);

        $result = (new Builder())
            ->from('orders')
            ->filterWhereIn('user_id', $subquery)
            ->count('*', 'order_count')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals(['$sum' => 1], $groupBody['order_count']);

        $this->assertEquals(['active'], $result->bindings);
    }

    public function testExistsSubqueryWithRegularFilter(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::greaterThan('total', 100)]);

        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->filterExists($subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);

        $matchStages = $this->findAllStages($pipeline, '$match');
        $this->assertGreaterThanOrEqual(2, \count($matchStages));

        $this->assertEquals([100, 'active'], $result->bindings);
    }

    public function testNotExistsSubqueryWithRegularFilter(): void
    {
        $subquery = (new Builder())
            ->from('banned_ips')
            ->select(['ip']);

        $result = (new Builder())
            ->from('logins')
            ->filter([Query::greaterThan('timestamp', '2024-01-01')])
            ->filterNotExists($subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $hasExistsMatch = false;
        foreach ($pipeline as $stage) {
            if (isset($stage['$match'])) {
                /** @var array<string, mixed> $matchBody */
                $matchBody = $stage['$match'];
                foreach ($matchBody as $key => $val) {
                    if (\str_starts_with($key, '_exists_') && \is_array($val) && isset($val['$size'])) {
                        $hasExistsMatch = true;
                        $this->assertEquals(0, $val['$size']);
                    }
                }
            }
        }
        $this->assertTrue($hasExistsMatch);

        $this->assertContains('2024-01-01', $result->bindings);
    }

    public function testUnionAllOfTwoComplexQueries(): void
    {
        $second = (new Builder())
            ->from('archived_orders')
            ->select(['id', 'amount'])
            ->filter([
                Query::greaterThan('amount', 200),
                Query::equal('status', ['archived']),
            ])
            ->sortDesc('amount')
            ->limit(5);

        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'amount'])
            ->filter([
                Query::greaterThan('amount', 100),
                Query::equal('status', ['active']),
            ])
            ->sortDesc('amount')
            ->limit(10)
            ->unionAll($second)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $unionStage = $this->findStage($pipeline, '$unionWith');
        $this->assertNotNull($unionStage);
        /** @var array<string, mixed> $unionBody */
        $unionBody = $unionStage['$unionWith'];
        $this->assertEquals('archived_orders', $unionBody['coll']);
        $this->assertArrayHasKey('pipeline', $unionBody);
    }

    public function testUnionAllOfThreeQueries(): void
    {
        $second = (new Builder())
            ->from('eu_users')
            ->select(['name'])
            ->filter([Query::equal('region', ['EU'])]);

        $third = (new Builder())
            ->from('asia_users')
            ->select(['name'])
            ->filter([Query::equal('region', ['ASIA'])]);

        $result = (new Builder())
            ->from('us_users')
            ->select(['name'])
            ->filter([Query::equal('region', ['US'])])
            ->unionAll($second)
            ->unionAll($third)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $unionStages = $this->findAllStages($pipeline, '$unionWith');
        $this->assertCount(2, $unionStages);

        /** @var array<string, mixed> $union1Body */
        $union1Body = $unionStages[0]['$unionWith'];
        $this->assertEquals('eu_users', $union1Body['coll']);

        /** @var array<string, mixed> $union2Body */
        $union2Body = $unionStages[1]['$unionWith'];
        $this->assertEquals('asia_users', $union2Body['coll']);

        $this->assertEquals(['US', 'EU', 'ASIA'], $result->bindings);
    }

    public function testUnionWithOrderByAndLimit(): void
    {
        $second = (new Builder())
            ->from('archived_users')
            ->select(['name', 'score']);

        $result = (new Builder())
            ->from('users')
            ->select(['name', 'score'])
            ->unionAll($second)
            ->sortDesc('score')
            ->limit(50)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $unionIdx = $this->findStageIndex($pipeline, '$unionWith');
        $sortIdx = $this->findStageIndex($pipeline, '$sort');
        $limitIdx = $this->findStageIndex($pipeline, '$limit');

        $this->assertNotNull($unionIdx);
        $this->assertNotNull($sortIdx);
        $this->assertNotNull($limitIdx);

        $this->assertLessThan($sortIdx, $unionIdx);
        $this->assertLessThan($limitIdx, $sortIdx);
    }

    public function testWindowFunctionWithWhereAndOrder(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->filter([Query::greaterThan('amount', 0)])
            ->selectWindow('ROW_NUMBER()', 'rn', ['region'], ['-amount'])
            ->sortDesc('rn')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $matchIdx = $this->findStageIndex($pipeline, '$match');
        $windowIdx = $this->findStageIndex($pipeline, '$setWindowFields');
        $sortIdx = $this->findStageIndex($pipeline, '$sort');

        $this->assertNotNull($matchIdx);
        $this->assertNotNull($windowIdx);
        $this->assertNotNull($sortIdx);

        $this->assertLessThan($windowIdx, $matchIdx);
        $this->assertLessThan($sortIdx, $windowIdx);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-amount'])
            ->selectWindow('SUM(amount)', 'running_sum', ['user_id'], ['created_at'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $windowStages = $this->findAllStages($pipeline, '$setWindowFields');
        $this->assertCount(2, $windowStages);

        /** @var array<string, mixed> $window1Body */
        $window1Body = $windowStages[0]['$setWindowFields'];
        /** @var array<string, mixed> $output1 */
        $output1 = $window1Body['output'];
        $this->assertArrayHasKey('rn', $output1);

        /** @var array<string, mixed> $window2Body */
        $window2Body = $windowStages[1]['$setWindowFields'];
        /** @var array<string, mixed> $output2 */
        $output2 = $window2Body['output'];
        $this->assertArrayHasKey('running_sum', $output2);
    }

    public function testWindowFunctionMultiplePartitionAndSortKeys(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->selectWindow('RANK()', 'rnk', ['region', 'department', 'team'], ['-revenue', 'name'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);

        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        $this->assertEquals([
            'region' => '$region',
            'department' => '$department',
            'team' => '$team',
        ], $windowBody['partitionBy']);

        /** @var array<string, int> $sortBy */
        $sortBy = $windowBody['sortBy'];
        $this->assertEquals(-1, $sortBy['revenue']);
        $this->assertEquals(1, $sortBy['name']);
    }

    public function testGroupByMultipleColumnsMultipleAggregates(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->avg('amount', 'average')
            ->groupBy(['region', 'year', 'quarter'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];

        $this->assertEquals([
            'region' => '$region',
            'year' => '$year',
            'quarter' => '$quarter',
        ], $groupBody['_id']);

        $this->assertEquals(['$sum' => 1], $groupBody['cnt']);
        $this->assertEquals(['$sum' => '$amount'], $groupBody['total']);
        $this->assertEquals(['$avg' => '$amount'], $groupBody['average']);

        $projectStage = $this->findStage($pipeline, '$project');
        $this->assertNotNull($projectStage);
        /** @var array<string, mixed> $projectBody */
        $projectBody = $projectStage['$project'];
        $this->assertEquals(0, $projectBody['_id']);
        $this->assertEquals('$_id.region', $projectBody['region']);
        $this->assertEquals('$_id.year', $projectBody['year']);
        $this->assertEquals('$_id.quarter', $projectBody['quarter']);
        $this->assertEquals(1, $projectBody['cnt']);
        $this->assertEquals(1, $projectBody['total']);
        $this->assertEquals(1, $projectBody['average']);
    }

    public function testMultipleAggregatesWithoutGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total_count')
            ->sum('amount', 'total_amount')
            ->avg('amount', 'avg_amount')
            ->min('amount', 'min_amount')
            ->max('amount', 'max_amount')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];

        $this->assertNull($groupBody['_id']);
        $this->assertEquals(['$sum' => 1], $groupBody['total_count']);
        $this->assertEquals(['$sum' => '$amount'], $groupBody['total_amount']);
        $this->assertEquals(['$avg' => '$amount'], $groupBody['avg_amount']);
        $this->assertEquals(['$min' => '$amount'], $groupBody['min_amount']);
        $this->assertEquals(['$max' => '$amount'], $groupBody['max_amount']);
    }

    public function testBeforeBuildCallbackAddingFiltersWithMainFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('role', ['admin'])])
            ->beforeBuild(function (Builder $builder) {
                $builder->filter([Query::equal('active', [true])]);
            })
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['role' => '?'],
            ['active' => '?'],
        ]], $op['filter']);
        $this->assertEquals(['admin', true], $result->bindings);
    }

    public function testAfterBuildCallbackModifyingResult(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->afterBuild(function (BuildResult $result) {
                /** @var array<string, mixed> $op */
                $op = \json_decode($result->query, true);
                $op['custom_flag'] = true;

                return new BuildResult(
                    \json_encode($op, JSON_THROW_ON_ERROR),
                    $result->bindings,
                    $result->readOnly
                );
            })
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertTrue($op['custom_flag']);
        $this->assertEquals('find', $op['operation']);
    }

    public function testInsertMultipleRowsDocumentStructure(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30, 'city' => 'NYC'])
            ->set(['name' => 'Bob', 'age' => 25, 'city' => 'LA'])
            ->set(['name' => 'Charlie', 'age' => 35, 'city' => 'SF'])
            ->insert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $documents */
        $documents = $op['documents'];
        $this->assertCount(3, $documents);

        foreach ($documents as $doc) {
            $this->assertEquals(['name' => '?', 'age' => '?', 'city' => '?'], $doc);
        }

        $this->assertEquals(['Alice', 30, 'NYC', 'Bob', 25, 'LA', 'Charlie', 35, 'SF'], $result->bindings);
    }

    public function testUpdateWithComplexMultiConditionFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'suspended'])
            ->filter([Query::or([
                Query::and([
                    Query::equal('role', ['admin']),
                    Query::lessThan('last_login', '2023-01-01'),
                ]),
                Query::and([
                    Query::equal('role', ['user']),
                    Query::equal('verified', [false]),
                ]),
            ])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('updateMany', $op['operation']);
        $this->assertEquals(['$set' => ['status' => '?']], $op['update']);

        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$or', $filter);
        /** @var list<array<string, mixed>> $orConditions */
        $orConditions = $filter['$or'];
        $this->assertCount(2, $orConditions);
        $this->assertArrayHasKey('$and', $orConditions[0]);
        $this->assertArrayHasKey('$and', $orConditions[1]);
    }

    public function testDeleteWithComplexOrAndFilter(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([Query::or([
                Query::and([
                    Query::lessThan('timestamp', '2022-01-01'),
                    Query::equal('type', ['error']),
                ]),
                Query::equal('status', ['expired']),
            ])])
            ->delete();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('deleteMany', $op['operation']);

        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$or', $filter);
        /** @var list<array<string, mixed>> $orConditions */
        $orConditions = $filter['$or'];
        $this->assertCount(2, $orConditions);
        $this->assertArrayHasKey('$and', $orConditions[0]);
    }

    public function testFilterOrWithEqualAndGreaterThanStructure(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::or([
                Query::equal('a', [1]),
                Query::greaterThan('b', 5),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['a' => '?'],
            ['b' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals([1, 5], $result->bindings);
    }

    public function testFilterAndWithEqualAndLessThanStructure(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::and([
                Query::equal('a', [1]),
                Query::lessThan('b', 10),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['a' => '?'],
            ['b' => ['$lt' => '?']],
        ]], $op['filter']);
        $this->assertEquals([1, 10], $result->bindings);
    }

    public function testNestedOrInsideAndInsideOr(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::or([
                Query::and([
                    Query::equal('status', ['active']),
                    Query::greaterThan('age', 18),
                ]),
                Query::and([
                    Query::lessThan('score', 50),
                    Query::notEqual('role', 'guest'),
                ]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$or', $filter);
        /** @var list<array<string, mixed>> $orConditions */
        $orConditions = $filter['$or'];
        $this->assertCount(2, $orConditions);
        $this->assertArrayHasKey('$and', $orConditions[0]);
        $this->assertArrayHasKey('$and', $orConditions[1]);

        /** @var list<array<string, mixed>> $and1 */
        $and1 = $orConditions[0]['$and'];
        $this->assertCount(2, $and1);
        $this->assertEquals(['status' => '?'], $and1[0]);
        $this->assertEquals(['age' => ['$gt' => '?']], $and1[1]);

        /** @var list<array<string, mixed>> $and2 */
        $and2 = $orConditions[1]['$and'];
        $this->assertCount(2, $and2);
        $this->assertEquals(['score' => ['$lt' => '?']], $and2[0]);
        $this->assertEquals(['role' => ['$ne' => '?']], $and2[1]);
    }

    public function testTripleNestingAndOfOrFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::and([
                Query::or([
                    Query::equal('status', ['active']),
                    Query::greaterThan('score', 100),
                ]),
                Query::or([
                    Query::lessThan('age', 30),
                    Query::between('balance', 0, 1000),
                ]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$and', $filter);
        /** @var list<array<string, mixed>> $andConditions */
        $andConditions = $filter['$and'];
        $this->assertCount(2, $andConditions);

        $this->assertArrayHasKey('$or', $andConditions[0]);
        $this->assertArrayHasKey('$or', $andConditions[1]);

        /** @var list<array<string, mixed>> $or1 */
        $or1 = $andConditions[0]['$or'];
        $this->assertEquals(['status' => '?'], $or1[0]);
        $this->assertEquals(['score' => ['$gt' => '?']], $or1[1]);

        /** @var list<array<string, mixed>> $or2 */
        $or2 = $andConditions[1]['$or'];
        $this->assertEquals(['age' => ['$lt' => '?']], $or2[0]);
        $this->assertEquals(['balance' => ['$gte' => '?', '$lte' => '?']], $or2[1]);
    }

    public function testIsNullWithEqualCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::isNull('deleted_at'),
                Query::equal('status', ['active']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['deleted_at' => null],
            ['status' => '?'],
        ]], $op['filter']);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testIsNotNullWithGreaterThanCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::isNotNull('email'),
                Query::greaterThan('login_count', 0),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['email' => ['$ne' => null]],
            ['login_count' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals([0], $result->bindings);
    }

    public function testBetweenWithNotEqualCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::between('age', 18, 65),
                Query::notEqual('status', 'banned'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['age' => ['$gte' => '?', '$lte' => '?']],
            ['status' => ['$ne' => '?']],
        ]], $op['filter']);
        $this->assertEquals([18, 65, 'banned'], $result->bindings);
    }

    public function testContainsWithStartsWithCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::contains('name', ['test']),
                Query::startsWith('email', 'admin'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['name' => ['$regex' => '?']],
            ['email' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['test', '^admin'], $result->bindings);
    }

    public function testNotContainsWithContainsCombined(): void
    {
        $result = (new Builder())
            ->from('posts')
            ->filter([
                Query::notContains('body', ['spam']),
                Query::contains('body', ['valuable']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['body' => ['$not' => ['$regex' => '?']]],
            ['body' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['spam', 'valuable'], $result->bindings);
    }

    public function testMultipleEqualOnDifferentFields(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::equal('name', ['Alice']),
                Query::equal('city', ['NYC']),
                Query::equal('role', ['admin']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['name' => '?'],
            ['city' => '?'],
            ['role' => '?'],
        ]], $op['filter']);
        $this->assertEquals(['Alice', 'NYC', 'admin'], $result->bindings);
    }

    public function testEqualMultiValueInEquivalent(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('x', [1, 2, 3])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['x' => ['$in' => ['?', '?', '?']]], $op['filter']);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testNotEqualMultiValueNinEquivalent(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::notEqual('x', [1, 2, 3])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['x' => ['$nin' => ['?', '?', '?']]], $op['filter']);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testEqualBooleanValue(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['active' => '?'], $op['filter']);
        $this->assertEquals([true], $result->bindings);
    }

    public function testEqualEmptyStringValue(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', [''])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => '?'], $op['filter']);
        $this->assertEquals([''], $result->bindings);
    }

    public function testRegexWithOtherFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::regex('name', '^[A-Z]'),
                Query::greaterThan('age', 18),
                Query::equal('status', ['active']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['name' => ['$regex' => '?']],
            ['age' => ['$gt' => '?']],
            ['status' => '?'],
        ]], $op['filter']);
        $this->assertEquals(['^[A-Z]', 18, 'active'], $result->bindings);
    }

    public function testContainsAllWithMultipleValues(): void
    {
        $result = (new Builder())
            ->from('posts')
            ->filter([Query::containsAll('tags', ['php', 'mongodb', 'testing'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$and' => [
            ['tags' => ['$regex' => '?']],
            ['tags' => ['$regex' => '?']],
            ['tags' => ['$regex' => '?']],
        ]], $op['filter']);
        $this->assertEquals(['php', 'mongodb', 'testing'], $result->bindings);
    }

    public function testFilterOnDottedNestedField(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('address.city', ['NYC'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['address.city' => '?'], $op['filter']);
        $this->assertEquals(['NYC'], $result->bindings);
    }

    public function testComplexQueryBindingOrder(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('amount', 100),
                Query::between('created_at', '2024-01-01', '2024-12-31'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([
            'active',
            100,
            '2024-01-01',
            '2024-12-31',
        ], $result->bindings);
    }

    public function testJoinFilterHavingBindingPositions(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id', '=', 'u')
            ->filter([Query::greaterThan('amount', 50)])
            ->count('*', 'cnt')
            ->groupBy(['u.name'])
            ->having([Query::greaterThan('cnt', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([50, 10], $result->bindings);
    }

    public function testUnionBindingsInBothBranches(): void
    {
        $second = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::equal('status', ['cancelled'])]);

        $result = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::equal('status', ['active'])])
            ->unionAll($second)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 'cancelled'], $result->bindings);
    }

    public function testSubqueryBindingsWithOuterQueryBindings(): void
    {
        $subquery = (new Builder())
            ->from('vip_users')
            ->select(['id'])
            ->filter([Query::equal('level', ['platinum'])]);

        $result = (new Builder())
            ->from('orders')
            ->filter([Query::greaterThan('total', 500)])
            ->filterWhereIn('user_id', $subquery)
            ->build();
        $this->assertBindingCount($result);

        $this->assertContains('platinum', $result->bindings);
        $this->assertContains(500, $result->bindings);
    }

    public function testUpdateWithFilterBindingsAndSetValueBindings(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'banned', 'reason' => 'violation'])
            ->filter([
                Query::equal('role', ['user']),
                Query::lessThan('karma', -10),
            ])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(['banned', 'violation', 'user', -10], $result->bindings);
    }

    public function testInsertMultipleRowsBindingPositions(): void
    {
        $result = (new Builder())
            ->into('items')
            ->set(['a' => 1, 'b' => 'x'])
            ->set(['a' => 2, 'b' => 'y'])
            ->set(['a' => 3, 'b' => 'z'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals([1, 'x', 2, 'y', 3, 'z'], $result->bindings);
    }

    public function testSelectEmptyArray(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select([])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
        // Empty select still creates a projection with only _id suppressed
        $this->assertEquals(['_id' => 0], $op['projection']);
    }

    public function testSelectStar(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['*'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
        $this->assertArrayHasKey('projection', $op);
        /** @var array<string, int> $projection */
        $projection = $op['projection'];
        $this->assertEquals(1, $projection['*']);
    }

    public function testSelectManyColumns(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['a', 'b', 'c', 'd', 'e'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, int> $projection */
        $projection = $op['projection'];
        $this->assertEquals(1, $projection['a']);
        $this->assertEquals(1, $projection['b']);
        $this->assertEquals(1, $projection['c']);
        $this->assertEquals(1, $projection['d']);
        $this->assertEquals(1, $projection['e']);
        $this->assertEquals(0, $projection['_id']);
    }

    public function testCompoundSort(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('a')
            ->sortDesc('b')
            ->sortAsc('c')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['a' => 1, 'b' => -1, 'c' => 1], $op['sort']);
    }

    public function testLimitOne(): void
    {
        $result = (new Builder())
            ->from('users')
            ->limit(1)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(1, $op['limit']);
    }

    public function testOffsetZero(): void
    {
        $result = (new Builder())
            ->from('users')
            ->offset(0)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(0, $op['skip']);
    }

    public function testLargeLimit(): void
    {
        $result = (new Builder())
            ->from('users')
            ->limit(1000000)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(1000000, $op['limit']);
    }

    public function testGroupByThreeColumns(): void
    {
        $result = (new Builder())
            ->from('data')
            ->count('*', 'cnt')
            ->groupBy(['a', 'b', 'c'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertEquals([
            'a' => '$a',
            'b' => '$b',
            'c' => '$c',
        ], $groupBody['_id']);
    }

    public function testDistinctWithoutExplicitSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);
    }

    public function testDistinctWithSelectAndSort(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['country', 'city'])
            ->distinct()
            ->sortAsc('country')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);

        $sortStage = $this->findStage($pipeline, '$sort');
        $this->assertNotNull($sortStage);
        /** @var array<string, int> $sortBody */
        $sortBody = $sortStage['$sort'];
        $this->assertEquals(1, $sortBody['country']);
    }

    public function testCountStarWithoutGroupByWholeCollection(): void
    {
        $result = (new Builder())
            ->from('users')
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];
        $groupStage = $this->findStage($pipeline, '$group');
        $this->assertNotNull($groupStage);
        /** @var array<string, mixed> $groupBody */
        $groupBody = $groupStage['$group'];
        $this->assertNull($groupBody['_id']);
        $this->assertEquals(['$sum' => 1], $groupBody['total']);
    }

    public function testReadOnlyFlagOnBuild(): void
    {
        $buildResult = (new Builder())
            ->from('users')
            ->select(['name'])
            ->build();
        $this->assertBindingCount($buildResult);
        $this->assertTrue($buildResult->readOnly);
    }

    public function testReadOnlyFlagOnInsert(): void
    {
        $insertResult = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice'])
            ->insert();
        $this->assertBindingCount($insertResult);
        $this->assertFalse($insertResult->readOnly);
    }

    public function testReadOnlyFlagOnUpdate(): void
    {
        $updateResult = (new Builder())
            ->from('users')
            ->set(['name' => 'Alice'])
            ->update();
        $this->assertBindingCount($updateResult);
        $this->assertFalse($updateResult->readOnly);
    }

    public function testReadOnlyFlagOnDelete(): void
    {
        $deleteResult = (new Builder())
            ->from('users')
            ->delete();
        $this->assertBindingCount($deleteResult);
        $this->assertFalse($deleteResult->readOnly);
    }

    public function testCloneThenModifyOriginalUnchanged(): void
    {
        $original = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])]);

        $cloned = $original->clone();
        $cloned->filter([Query::greaterThan('age', 25)]);
        $cloned->sortDesc('age');
        $cloned->limit(5);

        $originalResult = $original->build();
        $clonedResult = $cloned->build();

        $this->assertCount(1, $originalResult->bindings);
        $this->assertCount(2, $clonedResult->bindings);

        $originalOp = $this->decode($originalResult->query);
        $this->assertEquals('find', $originalOp['operation']);
        $this->assertArrayNotHasKey('sort', $originalOp);
        $this->assertArrayNotHasKey('limit', $originalOp);
    }

    public function testResetThenRebuild(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(10);

        $builder->reset();

        $builder->from('orders')
            ->select(['id'])
            ->filter([Query::greaterThan('total', 100)])
            ->build();

        $result = $builder->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('orders', $op['collection']);
        $this->assertEquals(['total' => ['$gt' => '?']], $op['filter']);
        $this->assertEquals([100], $result->bindings);
    }

    public function testMultipleSetCallsForUpdate(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'First'])
            ->set(['name' => 'Second', 'email' => 'test@test.com'])
            ->filter([Query::equal('id', ['abc'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        $this->assertArrayHasKey('$set', $update);
        /** @var array<string, string> $setDoc */
        $setDoc = $update['$set'];
        $this->assertEquals('?', $setDoc['name']);
    }

    public function testEmptyOrLogicalProducesExprFalse(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::or([])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$expr', $filter);
        $this->assertFalse($filter['$expr']);
    }

    public function testEmptyAndLogicalProducesNoFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::and([])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        // Empty AND returns [], buildFilter returns [], buildFind skips it
        $this->assertArrayNotHasKey('filter', $op);
    }

    public function testTextSearchIsFirstPipelineStage(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->filterSearch('content', 'mongodb')
            ->filter([Query::equal('status', ['published'])])
            ->sortDesc('score')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        /** @var array<string, mixed> $firstStage */
        $firstStage = $pipeline[0];
        $this->assertArrayHasKey('$match', $firstStage);
        /** @var array<string, mixed> $matchBody */
        $matchBody = $firstStage['$match'];
        $this->assertArrayHasKey('$text', $matchBody);

        $this->assertEquals(['mongodb', 'published'], $result->bindings);
    }

    public function testTableSamplingBeforeFilters(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->tablesample(500)
            ->filter([Query::equal('level', ['error'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $sampleIdx = $this->findStageIndex($pipeline, '$sample');
        $matchIdx = $this->findStageIndex($pipeline, '$match');

        $this->assertNotNull($sampleIdx);
        $this->assertNotNull($matchIdx);
        $this->assertLessThan($matchIdx, $sampleIdx);
    }

    public function testSortRandomWithFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->sortRandom()
            ->limit(5)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $addFieldsStage = $this->findStage($pipeline, '$addFields');
        $this->assertNotNull($addFieldsStage);

        $sortStage = $this->findStage($pipeline, '$sort');
        $this->assertNotNull($sortStage);
        /** @var array<string, int> $sortBody */
        $sortBody = $sortStage['$sort'];
        $this->assertEquals(1, $sortBody['_rand']);

        $unsetStage = $this->findStage($pipeline, '$unset');
        $this->assertNotNull($unsetStage);
        $this->assertEquals('_rand', $unsetStage['$unset']);
    }

    public function testUpdateWithSetAndPushAndIncrement(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['last_login' => '2024-06-15'])
            ->push('activity_log', 'logged_in')
            ->increment('login_count', 1)
            ->addToSet('badges', 'frequent_user')
            ->pull('temp_flags', 'old_flag')
            ->unsetFields('deprecated', 'legacy')
            ->filter([Query::equal('_id', ['user123'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        $this->assertArrayHasKey('$set', $update);
        $this->assertArrayHasKey('$push', $update);
        $this->assertArrayHasKey('$inc', $update);
        $this->assertArrayHasKey('$addToSet', $update);
        $this->assertArrayHasKey('$pull', $update);
        $this->assertArrayHasKey('$unset', $update);

        /** @var array<string, string> $unsetDoc */
        $unsetDoc = $update['$unset'];
        $this->assertArrayHasKey('deprecated', $unsetDoc);
        $this->assertArrayHasKey('legacy', $unsetDoc);
    }

    public function testUpsertWithMultipleConflictKeys(): void
    {
        $result = (new Builder())
            ->into('metrics')
            ->set(['date' => '2024-06-15', 'metric' => 'pageviews', 'value' => 1500])
            ->onConflict(['date', 'metric'], ['value'])
            ->upsert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('updateOne', $op['operation']);
        $this->assertEquals(['date' => '?', 'metric' => '?'], $op['filter']);
        $this->assertEquals(['$set' => ['value' => '?']], $op['update']);
        $this->assertEquals(['2024-06-15', 'pageviews', 1500], $result->bindings);
    }

    public function testDeleteWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('sessions')
            ->filter([
                Query::lessThan('expires_at', '2024-01-01'),
                Query::notEqual('persistent', true),
                Query::isNull('user_id'),
            ])
            ->delete();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('deleteMany', $op['operation']);

        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayHasKey('$and', $filter);
        /** @var list<array<string, mixed>> $andConditions */
        $andConditions = $filter['$and'];
        $this->assertCount(3, $andConditions);

        $this->assertEquals(['expires_at' => ['$lt' => '?']], $andConditions[0]);
        $this->assertEquals(['persistent' => ['$ne' => '?']], $andConditions[1]);
        $this->assertEquals(['user_id' => null], $andConditions[2]);

        $this->assertEquals(['2024-01-01', true], $result->bindings);
    }

    public function testUpdateWithNoFilterProducesEmptyStdclass(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['updated_at' => '2024-06-15'])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('updateMany', $op['operation']);
        $this->assertEmpty((array) $op['filter']);
    }

    public function testInsertOrIgnorePreservesOrderedFalse(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'A', 'email' => 'a@b.com'])
            ->set(['name' => 'B', 'email' => 'b@b.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('insertMany', $op['operation']);
        /** @var array<string, mixed> $options */
        $options = $op['options'];
        $this->assertFalse($options['ordered']);
        /** @var list<array<string, mixed>> $documents */
        $documents = $op['documents'];
        $this->assertCount(2, $documents);
    }

    public function testPipelineStageOrderWithAllFeatures(): void
    {
        $subquery = (new Builder())
            ->from('vips')
            ->select(['user_id']);

        $unionBranch = (new Builder())
            ->from('archived')
            ->select(['name']);

        $result = (new Builder())
            ->from('users')
            ->filterSearch('bio', 'developer')
            ->join('profiles', 'users.id', 'profiles.user_id', '=', 'p')
            ->filterWhereIn('id', $subquery)
            ->filter([Query::equal('active', [true])])
            ->count('*', 'cnt')
            ->groupBy(['region'])
            ->having([Query::greaterThan('cnt', 5)])
            ->unionAll($unionBranch)
            ->sortDesc('cnt')
            ->limit(20)
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $stageTypes = [];
        foreach ($pipeline as $stage) {
            $stageTypes[] = \array_key_first($stage);
        }

        $textMatchPos = \array_search('$match', $stageTypes);
        $this->assertNotFalse($textMatchPos);
        $this->assertEquals(0, $textMatchPos);
    }

    public function testWindowFunctionWithNullPartition(): void
    {
        $result = (new Builder())
            ->from('events')
            ->selectWindow('ROW_NUMBER()', 'global_rn', null, ['timestamp'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);

        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        $this->assertArrayNotHasKey('partitionBy', $windowBody);
        $this->assertArrayHasKey('sortBy', $windowBody);
    }

    public function testWindowFunctionWithEmptyPartition(): void
    {
        $result = (new Builder())
            ->from('events')
            ->selectWindow('DENSE_RANK()', 'rnk', [], ['score'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);

        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        $this->assertArrayNotHasKey('partitionBy', $windowBody);
    }

    public function testWindowFunctionWithNullOrderBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->selectWindow('SUM(amount)', 'total', ['category'], null)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $windowStage = $this->findStage($pipeline, '$setWindowFields');
        $this->assertNotNull($windowStage);

        /** @var array<string, mixed> $windowBody */
        $windowBody = $windowStage['$setWindowFields'];
        $this->assertArrayNotHasKey('sortBy', $windowBody);
    }

    public function testGroupByProjectReshape(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->sum('amount', 'total_sales')
            ->groupBy(['region'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $projectStage = $this->findStage($pipeline, '$project');
        $this->assertNotNull($projectStage);
        /** @var array<string, mixed> $projectBody */
        $projectBody = $projectStage['$project'];
        $this->assertEquals(0, $projectBody['_id']);
        $this->assertEquals('$_id', $projectBody['region']);
        $this->assertEquals(1, $projectBody['total_sales']);
    }

    public function testCrossJoinThrowsUnsupportedException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Cross/natural joins are not supported');

        (new Builder())
            ->from('users')
            ->crossJoin('roles')
            ->build();
    }

    public function testNaturalJoinThrowsUnsupportedException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Cross/natural joins are not supported');

        (new Builder())
            ->from('users')
            ->naturalJoin('roles')
            ->build();
    }

    public function testFilterEndsWithSpecialChars(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::endsWith('email', '.co.uk')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['email' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['\.co\.uk$'], $result->bindings);
    }

    public function testFilterStartsWithSpecialChars(): void
    {
        $result = (new Builder())
            ->from('files')
            ->filter([Query::startsWith('path', '/var/log.')])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['path' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['^\/var\/log\.'], $result->bindings);
    }

    public function testFilterContainsWithSpecialCharsEscaped(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::contains('message', ['file.txt'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['message' => ['$regex' => '?']], $op['filter']);
        $this->assertEquals(['file\.txt'], $result->bindings);
    }

    public function testFilterGreaterThanEqualWithFloat(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([Query::greaterThanEqual('price', 9.99)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['price' => ['$gte' => '?']], $op['filter']);
        $this->assertEquals([9.99], $result->bindings);
    }

    public function testFilterLessThanEqualWithZero(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([Query::lessThanEqual('stock', 0)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['stock' => ['$lte' => '?']], $op['filter']);
        $this->assertEquals([0], $result->bindings);
    }

    public function testInsertSingleRowBindingStructure(): void
    {
        $result = (new Builder())
            ->into('logs')
            ->set(['level' => 'info', 'message' => 'test', 'timestamp' => 12345])
            ->insert();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $documents */
        $documents = $op['documents'];
        $this->assertCount(1, $documents);
        $this->assertEquals(['level' => '?', 'message' => '?', 'timestamp' => '?'], $documents[0]);
        $this->assertEquals(['info', 'test', 12345], $result->bindings);
    }

    public function testFindOperationHasNoProjectionWhenNoneSelected(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('find', $op['operation']);
        $this->assertArrayNotHasKey('projection', $op);
    }

    public function testFindOperationHasNoSortWhenNoneSorted(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertArrayNotHasKey('sort', $op);
    }

    public function testFindOperationHasNoSkipWhenNoOffset(): void
    {
        $result = (new Builder())
            ->from('users')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertArrayNotHasKey('skip', $op);
    }

    public function testFindOperationHasNoLimitWhenNoLimit(): void
    {
        $result = (new Builder())
            ->from('users')
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertArrayNotHasKey('limit', $op);
    }

    public function testSelectIdFieldSuppressesIdExclusion(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['_id', 'name'])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, int> $projection */
        $projection = $op['projection'];
        $this->assertEquals(1, $projection['_id']);
        $this->assertEquals(1, $projection['name']);
    }

    public function testIncrementWithFloat(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->increment('balance', 99.50)
            ->filter([Query::equal('id', ['acc1'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        /** @var array<string, float> $incDoc */
        $incDoc = $update['$inc'];
        $this->assertEquals(99.50, $incDoc['balance']);
    }

    public function testIncrementWithNegativeValue(): void
    {
        $result = (new Builder())
            ->from('counters')
            ->increment('value', -5)
            ->filter([Query::equal('name', ['test'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        /** @var array<string, int> $incDoc */
        $incDoc = $update['$inc'];
        $this->assertEquals(-5, $incDoc['value']);
    }

    public function testUnsetMultipleFields(): void
    {
        $result = (new Builder())
            ->from('users')
            ->unsetFields('field_a', 'field_b', 'field_c')
            ->filter([Query::equal('id', ['x'])])
            ->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        /** @var array<string, string> $unsetDoc */
        $unsetDoc = $update['$unset'];
        $this->assertCount(3, $unsetDoc);
        $this->assertEquals('', $unsetDoc['field_a']);
        $this->assertEquals('', $unsetDoc['field_b']);
        $this->assertEquals('', $unsetDoc['field_c']);
    }

    public function testResetClearsMongoSpecificState(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->push('tags', 'a')
            ->pull('tags', 'b')
            ->addToSet('roles', 'admin')
            ->increment('counter', 1)
            ->unsetFields('temp')
            ->filterSearch('bio', 'test')
            ->tablesample(50);

        $builder->reset();
        $builder->from('items')->set(['name' => 'item1']);

        $result = $builder->update();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('items', $op['collection']);
        /** @var array<string, mixed> $update */
        $update = $op['update'];
        $this->assertArrayHasKey('$set', $update);
        $this->assertArrayNotHasKey('$push', $update);
        $this->assertArrayNotHasKey('$pull', $update);
        $this->assertArrayNotHasKey('$addToSet', $update);
        $this->assertArrayNotHasKey('$inc', $update);
        $this->assertArrayNotHasKey('$unset', $update);
    }

    public function testSingleFilterDoesNotWrapInAnd(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ['Alice'])])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['name' => '?'], $op['filter']);
        /** @var array<string, mixed> $filter */
        $filter = $op['filter'];
        $this->assertArrayNotHasKey('$and', $filter);
    }

    public function testPageCalculation(): void
    {
        $result = (new Builder())
            ->from('users')
            ->page(5, 20)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(20, $op['limit']);
        $this->assertEquals(80, $op['skip']);
    }

    public function testTextSearchAndTableSamplingCombined(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->filterSearch('content', 'tutorial')
            ->tablesample(200)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $this->assertArrayHasKey('$match', $pipeline[0]);
        /** @var array<string, mixed> $firstMatch */
        $firstMatch = $pipeline[0]['$match'];
        $this->assertArrayHasKey('$text', $firstMatch);

        $this->assertArrayHasKey('$sample', $pipeline[1]);
        /** @var array<string, mixed> $sampleBody */
        $sampleBody = $pipeline[1]['$sample'];
        $this->assertEquals(200, $sampleBody['size']);
    }

    public function testNotBetweenStructure(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([Query::notBetween('price', 10.0, 50.0)])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['$or' => [
            ['price' => ['$lt' => '?']],
            ['price' => ['$gt' => '?']],
        ]], $op['filter']);
        $this->assertEquals([10.0, 50.0], $result->bindings);
    }

    public function testContainsAnyOnArrayUsesIn(): void
    {
        $query = Query::containsAny('tags', ['a', 'b', 'c']);
        $query->setOnArray(true);

        $result = (new Builder())
            ->from('posts')
            ->filter([$query])
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals(['tags' => ['$in' => ['?', '?', '?']]], $op['filter']);
        $this->assertEquals(['a', 'b', 'c'], $result->bindings);
    }

    public function testFilterWhereNotInSubqueryStructure(): void
    {
        $subquery = (new Builder())
            ->from('blacklist')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterWhereNotIn('id', $subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('blacklist', $lookupBody['from']);
        $this->assertEquals('_sub_0', $lookupBody['as']);

        $unsetStage = $this->findStage($pipeline, '$unset');
        $this->assertNotNull($unsetStage);
    }

    public function testBuildIdempotent(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(10);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1->query, $result2->query);
        $this->assertEquals($result1->bindings, $result2->bindings);
    }

    public function testExistsSubqueryAddsLimitOnePipeline(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterExists($subquery)
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        /** @var list<array<string, mixed>> $subPipeline */
        $subPipeline = $lookupBody['pipeline'];

        $hasLimit = false;
        foreach ($subPipeline as $stage) {
            if (isset($stage['$limit'])) {
                $hasLimit = true;
                $this->assertEquals(1, $stage['$limit']);
            }
        }
        $this->assertTrue($hasLimit);
    }

    public function testJoinStripTablePrefix(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users._id', '=', 'u')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('user_id', $lookupBody['localField']);
        $this->assertEquals('_id', $lookupBody['foreignField']);
    }

    public function testJoinDefaultAliasUsesTableName(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $lookupStage = $this->findStage($pipeline, '$lookup');
        $this->assertNotNull($lookupStage);
        /** @var array<string, mixed> $lookupBody */
        $lookupBody = $lookupStage['$lookup'];
        $this->assertEquals('users', $lookupBody['as']);
    }

    public function testSortRandomWithSortAscCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('name')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $op = $this->decode($result->query);
        $this->assertEquals('aggregate', $op['operation']);

        /** @var list<array<string, mixed>> $pipeline */
        $pipeline = $op['pipeline'];

        $sortStage = $this->findStage($pipeline, '$sort');
        $this->assertNotNull($sortStage);
        /** @var array<string, int> $sortBody */
        $sortBody = $sortStage['$sort'];
        $this->assertEquals(1, $sortBody['name']);
        $this->assertEquals(1, $sortBody['_rand']);
    }
}
