<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\Case\Builder as CaseBuilder;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\Feature\Aggregates;
use Utopia\Query\Builder\Feature\CTEs;
use Utopia\Query\Builder\Feature\Deletes;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\Hooks;
use Utopia\Query\Builder\Feature\Inserts;
use Utopia\Query\Builder\Feature\Joins;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\Locking;
use Utopia\Query\Builder\Feature\Selects;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Builder\Feature\Transactions;
use Utopia\Query\Builder\Feature\Unions;
use Utopia\Query\Builder\Feature\Updates;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Builder\Feature\VectorSearch;
use Utopia\Query\Builder\Feature\Windows;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Compiler;
use Utopia\Query\Exception;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook;
use Utopia\Query\Hook\Attribute;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;
use Utopia\Query\Query;

class ClickHouseTest extends TestCase
{
    use AssertsBindingCount;
    public function testImplementsCompiler(): void
    {
        $builder = new Builder();
        $this->assertInstanceOf(Compiler::class, $builder);
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

    public function testBasicSelect(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['name', 'timestamp'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `name`, `timestamp` FROM `events`', $result->query);
    }

    public function testFilterAndSort(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('count', 10),
            ])
            ->sortDesc('timestamp')
            ->limit(100)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `status` IN (?) AND `count` > ? ORDER BY `timestamp` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['active', 10, 100], $result->bindings);
    }

    public function testRegexUsesMatchFunction(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', '^/api/v[0-9]+')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`path`, ?)', $result->query);
        $this->assertEquals(['^/api/v[0-9]+'], $result->bindings);
    }

    public function testSearchThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search is not supported by this dialect.');

        (new Builder())
            ->from('logs')
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    public function testNotSearchThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search is not supported by this dialect.');

        (new Builder())
            ->from('logs')
            ->filter([Query::notSearch('content', 'hello')])
            ->build();
    }

    public function testRandomOrderUsesLowercaseRand(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand()', $result->query);
    }

    public function testFinalKeyword(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL', $result->query);
    }

    public function testFinalWithFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` FINAL WHERE `status` IN (?) LIMIT ?',
            $result->query
        );
        $this->assertEquals(['active', 10], $result->bindings);
    }

    public function testSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1', $result->query);
    }

    public function testSampleWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.5', $result->query);
    }

    public function testPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?)',
            $result->query
        );
        $this->assertEquals(['click'], $result->bindings);
    }

    public function testPrewhereWithMultipleConditions(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([
                Query::equal('event_type', ['click']),
                Query::greaterThan('timestamp', '2024-01-01'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?) AND `timestamp` > ?',
            $result->query
        );
        $this->assertEquals(['click', '2024-01-01'], $result->bindings);
    }

    public function testPrewhereWithWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?) WHERE `count` > ?',
            $result->query
        );
        $this->assertEquals(['click', 5], $result->bindings);
    }

    public function testPrewhereWithJoinAndWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.user_id', 'users.id')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` JOIN `users` ON `events`.`user_id` = `users`.`id` PREWHERE `event_type` IN (?) WHERE `users`.`age` > ?',
            $result->query
        );
        $this->assertEquals(['click', 18], $result->bindings);
    }

    public function testFinalSamplePrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sortDesc('timestamp')
            ->limit(100)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `count` > ? ORDER BY `timestamp` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['click', 5, 100], $result->bindings);
    }

    public function testAggregation(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'total')
            ->sum('duration', 'total_duration')
            ->groupBy(['event_type'])
            ->having([Query::greaterThan('total', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, SUM(`duration`) AS `total_duration` FROM `events` GROUP BY `event_type` HAVING `total` > ?',
            $result->query
        );
        $this->assertEquals([10], $result->bindings);
    }

    public function testJoin(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.user_id', 'users.id')
            ->leftJoin('sessions', 'events.session_id', 'sessions.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` JOIN `users` ON `events`.`user_id` = `users`.`id` LEFT JOIN `sessions` ON `events`.`session_id` = `sessions`.`id`',
            $result->query
        );
    }

    public function testDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->distinct()
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT `user_id` FROM `events`', $result->query);
    }

    public function testUnion(): void
    {
        $other = (new Builder())->from('events_archive')->filter([Query::equal('year', [2023])]);

        $result = (new Builder())
            ->from('events')
            ->filter([Query::equal('year', [2024])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `events` WHERE `year` IN (?)) UNION (SELECT * FROM `events_archive` WHERE `year` IN (?))',
            $result->query
        );
        $this->assertEquals([2024, 2023], $result->bindings);
    }

    public function testToRawSql(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->toRawSql();

        $this->assertEquals(
            "SELECT * FROM `events` FINAL WHERE `status` IN ('active') LIMIT 10",
            $sql
        );
    }

    public function testResetClearsClickHouseState(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('count', 5)]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('logs')->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testFluentChainingReturnsSameInstance(): void
    {
        $builder = new Builder();

        $this->assertSame($builder, $builder->from('t'));
        $this->assertSame($builder, $builder->final());
        $this->assertSame($builder, $builder->sample(0.1));
        $this->assertSame($builder, $builder->prewhere([]));
        $this->assertSame($builder, $builder->select(['a']));
        $this->assertSame($builder, $builder->filter([]));
        $this->assertSame($builder, $builder->sortAsc('a'));
        $this->assertSame($builder, $builder->limit(1));
        $this->assertSame($builder, $builder->reset());
    }

    public function testAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->addHook(new AttributeMap(['$id' => '_uid']))
            ->filter([Query::equal('$id', ['abc'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `_uid` IN (?)',
            $result->query
        );
    }

    public function testConditionProvider(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('_tenant = ?', ['t1']);
            }
        };

        $result = (new Builder())
            ->from('events')
            ->addHook($hook)
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `status` IN (?) AND _tenant = ?',
            $result->query
        );
        $this->assertEquals(['active', 't1'], $result->bindings);
    }

    public function testPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        // prewhere bindings come before where bindings
        $this->assertEquals(['click', 5, 10], $result->bindings);
    }

    public function testCombinedPrewhereWhereJoinGroupBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.user_id', 'users.id')
            ->prewhere([Query::equal('event_type', ['purchase'])])
            ->filter([Query::greaterThan('events.amount', 100)])
            ->count('*', 'total')
            ->select(['users.country'])
            ->groupBy(['users.country'])
            ->having([Query::greaterThan('total', 5)])
            ->sortDesc('total')
            ->limit(50)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;

        // Verify clause ordering
        $this->assertStringContainsString('SELECT', $query);
        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('JOIN `users`', $query);
        $this->assertStringContainsString('PREWHERE `event_type` IN (?)', $query);
        $this->assertStringContainsString('WHERE `events`.`amount` > ?', $query);
        $this->assertStringContainsString('GROUP BY `users`.`country`', $query);
        $this->assertStringContainsString('HAVING `total` > ?', $query);
        $this->assertStringContainsString('ORDER BY `total` DESC', $query);
        $this->assertStringContainsString('LIMIT ?', $query);

        // Verify ordering: PREWHERE before WHERE
        $this->assertLessThan(strpos($query, 'WHERE'), strpos($query, 'PREWHERE'));
    }
    // 1. PREWHERE comprehensive (40+ tests)

    public function testPrewhereEmptyArray(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testPrewhereSingleEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `status` IN (?)', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testPrewhereSingleNotEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notEqual('status', 'deleted')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `status` != ?', $result->query);
        $this->assertEquals(['deleted'], $result->bindings);
    }

    public function testPrewhereLessThan(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::lessThan('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` < ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testPrewhereLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::lessThanEqual('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` <= ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testPrewhereGreaterThan(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThan('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `score` > ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testPrewhereGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThanEqual('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `score` >= ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testPrewhereBetween(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testPrewhereNotBetween(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notBetween('age', 0, 17)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([0, 17], $result->bindings);
    }

    public function testPrewhereStartsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::startsWith('path', '/api')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE startsWith(`path`, ?)', $result->query);
        $this->assertEquals(['/api'], $result->bindings);
    }

    public function testPrewhereNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notStartsWith('path', '/admin')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE NOT startsWith(`path`, ?)', $result->query);
        $this->assertEquals(['/admin'], $result->bindings);
    }

    public function testPrewhereEndsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::endsWith('file', '.csv')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE endsWith(`file`, ?)', $result->query);
        $this->assertEquals(['.csv'], $result->bindings);
    }

    public function testPrewhereNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notEndsWith('file', '.tmp')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE NOT endsWith(`file`, ?)', $result->query);
        $this->assertEquals(['.tmp'], $result->bindings);
    }

    public function testPrewhereContainsSingle(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::contains('name', ['foo'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE position(`name`, ?) > 0', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testPrewhereContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::contains('name', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (position(`name`, ?) > 0 OR position(`name`, ?) > 0)', $result->query);
        $this->assertEquals(['foo', 'bar'], $result->bindings);
    }

    public function testPrewhereContainsAny(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::containsAny('tag', ['a', 'b', 'c'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (position(`tag`, ?) > 0 OR position(`tag`, ?) > 0 OR position(`tag`, ?) > 0)', $result->query);
        $this->assertEquals(['a', 'b', 'c'], $result->bindings);
    }

    public function testPrewhereContainsAll(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::containsAll('tag', ['x', 'y'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (position(`tag`, ?) > 0 AND position(`tag`, ?) > 0)', $result->query);
        $this->assertEquals(['x', 'y'], $result->bindings);
    }

    public function testPrewhereNotContainsSingle(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notContains('name', ['bad'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE position(`name`, ?) = 0', $result->query);
        $this->assertEquals(['bad'], $result->bindings);
    }

    public function testPrewhereNotContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notContains('name', ['bad', 'ugly'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (position(`name`, ?) = 0 AND position(`name`, ?) = 0)', $result->query);
        $this->assertEquals(['bad', 'ugly'], $result->bindings);
    }

    public function testPrewhereIsNull(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::isNull('deleted_at')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `deleted_at` IS NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testPrewhereIsNotNull(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::isNotNull('email')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `email` IS NOT NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testPrewhereExists(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::exists(['col_a', 'col_b'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`col_a` IS NOT NULL AND `col_b` IS NOT NULL)', $result->query);
    }

    public function testPrewhereNotExists(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notExists(['col_a'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`col_a` IS NULL)', $result->query);
    }

    public function testPrewhereRegex(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::regex('path', '^/api')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE match(`path`, ?)', $result->query);
        $this->assertEquals(['^/api'], $result->bindings);
    }

    public function testPrewhereAndLogical(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::and([
                Query::equal('a', [1]),
                Query::equal('b', [2]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`a` IN (?) AND `b` IN (?))', $result->query);
        $this->assertEquals([1, 2], $result->bindings);
    }

    public function testPrewhereOrLogical(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::or([
                Query::equal('a', [1]),
                Query::equal('b', [2]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`a` IN (?) OR `b` IN (?))', $result->query);
        $this->assertEquals([1, 2], $result->bindings);
    }

    public function testPrewhereNestedAndOr(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::and([
                Query::or([
                    Query::equal('x', [1]),
                    Query::equal('y', [2]),
                ]),
                Query::greaterThan('z', 0),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE ((`x` IN (?) OR `y` IN (?)) AND `z` > ?)', $result->query);
        $this->assertEquals([1, 2, 0], $result->bindings);
    }

    public function testPrewhereRawExpression(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::raw('toDate(created) > ?', ['2024-01-01'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE toDate(created) > ?', $result->query);
        $this->assertEquals(['2024-01-01'], $result->bindings);
    }

    public function testPrewhereMultipleCallsAdditive(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('a', [1])])
            ->prewhere([Query::equal('b', [2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `a` IN (?) AND `b` IN (?)', $result->query);
        $this->assertEquals([1, 2], $result->bindings);
    }

    public function testPrewhereWithWhereFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` FINAL PREWHERE `type` IN (?) WHERE `count` > ?',
            $result->query
        );
    }

    public function testPrewhereWithWhereSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` SAMPLE 0.5 PREWHERE `type` IN (?) WHERE `count` > ?',
            $result->query
        );
    }

    public function testPrewhereWithWhereFinalSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.3)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.3 PREWHERE `type` IN (?) WHERE `count` > ?',
            $result->query
        );
        $this->assertEquals(['click', 5], $result->bindings);
    }

    public function testPrewhereWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->groupBy(['type'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
        $this->assertStringContainsString('GROUP BY `type`', $result->query);
    }

    public function testPrewhereWithHaving(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->groupBy(['type'])
            ->having([Query::greaterThan('total', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
        $this->assertStringContainsString('HAVING `total` > ?', $result->query);
    }

    public function testPrewhereWithOrderBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sortAsc('name')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) ORDER BY `name` ASC',
            $result->query
        );
    }

    public function testPrewhereWithLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['click', 10, 20], $result->bindings);
    }

    public function testPrewhereWithUnion(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
        $this->assertStringContainsString('UNION (SELECT', $result->query);
    }

    public function testPrewhereWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->distinct()
            ->select(['user_id'])
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
    }

    public function testPrewhereWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sum('amount', 'total_amount')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(`amount`) AS `total_amount`', $result->query);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
    }

    public function testPrewhereBindingOrderWithProvider(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant_id = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['click', 5, 't1'], $result->bindings);
    }

    public function testPrewhereBindingOrderWithCursor(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->cursorAfter('abc123')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        // prewhere, where filter, cursor
        $this->assertEquals('click', $result->bindings[0]);
        $this->assertEquals(5, $result->bindings[1]);
        $this->assertEquals('abc123', $result->bindings[2]);
    }

    public function testPrewhereBindingOrderComplex(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->count('*', 'total')
            ->groupBy(['type'])
            ->having([Query::greaterThan('total', 10)])
            ->limit(50)
            ->offset(100)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        // prewhere, filter, provider, cursor, having, limit, offset, union
        $this->assertEquals('click', $result->bindings[0]);
        $this->assertEquals(5, $result->bindings[1]);
        $this->assertEquals('t1', $result->bindings[2]);
        $this->assertEquals('cur1', $result->bindings[3]);
    }

    public function testPrewhereWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->addHook(new AttributeMap([
                '$id' => '_uid',
            ]))
            ->prewhere([Query::equal('$id', ['abc'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` PREWHERE `_uid` IN (?)', $result->query);
        $this->assertEquals(['abc'], $result->bindings);
    }

    public function testPrewhereOnlyNoWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThan('ts', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        // "PREWHERE" contains "WHERE" as a substring, so we check there is no standalone WHERE clause
        $withoutPrewhere = str_replace('PREWHERE', '', $result->query);
        $this->assertStringNotContainsString('WHERE', $withoutPrewhere);
    }

    public function testPrewhereWithEmptyWhereFilter(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['a'])])
            ->filter([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $withoutPrewhere = str_replace('PREWHERE', '', $result->query);
        $this->assertStringNotContainsString('WHERE', $withoutPrewhere);
    }

    public function testPrewhereAppearsAfterJoinsBeforeWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $joinPos = strpos($query, 'JOIN');
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');

        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testPrewhereMultipleFiltersInSingleCall(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([
                Query::equal('a', [1]),
                Query::greaterThan('b', 2),
                Query::lessThan('c', 3),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `a` IN (?) AND `b` > ? AND `c` < ?',
            $result->query
        );
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testPrewhereResetClearsPrewhereQueries(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('PREWHERE', $result->query);
    }

    public function testPrewhereInToRawSqlOutput(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->toRawSql();

        $this->assertEquals(
            "SELECT * FROM `events` PREWHERE `type` IN ('click') WHERE `count` > 5",
            $sql
        );
    }
    // 2. FINAL comprehensive (20+ tests)

    public function testFinalBasicSelect(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->select(['name', 'ts'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `name`, `ts` FROM `events` FINAL', $result->query);
    }

    public function testFinalWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
    }

    public function testFinalWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
    }

    public function testFinalWithGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('GROUP BY `type`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
    }

    public function testFinalWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->distinct()
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT `user_id` FROM `events` FINAL', $result->query);
    }

    public function testFinalWithSort(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sortAsc('name')
            ->sortDesc('ts')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL ORDER BY `name` ASC, `ts` DESC', $result->query);
    }

    public function testFinalWithLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([10, 20], $result->bindings);
    }

    public function testFinalWithCursor(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testFinalWithUnion(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('UNION (SELECT', $result->query);
    }

    public function testFinalWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL PREWHERE `type` IN (?)', $result->query);
    }

    public function testFinalWithSampleAlone(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.25)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.25', $result->query);
    }

    public function testFinalWithPrewhereSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.5 PREWHERE `type` IN (?)', $result->query);
    }

    public function testFinalFullPipeline(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->select(['name'])
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->sortDesc('ts')
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('SELECT `name`', $query);
        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('OFFSET', $query);
    }

    public function testFinalCalledMultipleTimesIdempotent(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->final()
            ->final()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL', $result->query);
        // Ensure FINAL appears only once
        $this->assertEquals(1, substr_count($result->query, 'FINAL'));
    }

    public function testFinalInToRawSql(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->final()
            ->filter([Query::equal('status', ['ok'])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `events` FINAL WHERE `status` IN ('ok')", $sql);
    }

    public function testFinalPositionAfterTableBeforeJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $finalPos = strpos($query, 'FINAL');
        $joinPos = strpos($query, 'JOIN');

        $this->assertLessThan($joinPos, $finalPos);
    }

    public function testFinalWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addHook(new class () implements Attribute {
                public function resolve(string $attribute): string
                {
                    return 'col_' . $attribute;
                }
            })
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('`col_status`', $result->query);
    }

    public function testFinalWithConditionProvider(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('deleted = ?', $result->query);
    }

    public function testFinalResetClearsFlag(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testFinalWithWhenConditional(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);

        $result2 = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->final())
            ->build();

        $this->assertStringNotContainsString('FINAL', $result2->query);
    }
    // 3. SAMPLE comprehensive (23 tests)

    public function testSample10Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.1)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1', $result->query);
    }

    public function testSample50Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.5)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5', $result->query);
    }

    public function testSample1Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.01)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.01', $result->query);
    }

    public function testSample99Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.99)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.99', $result->query);
    }

    public function testSampleWithFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.2)
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.2 WHERE `status` IN (?)', $result->query);
    }

    public function testSampleWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.3)
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.3', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
    }

    public function testSampleWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->count('*', 'cnt')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.1', $result->query);
        $this->assertStringContainsString('COUNT(*)', $result->query);
    }

    public function testSampleWithGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->having([Query::greaterThan('cnt', 2)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('HAVING', $result->query);
    }

    public function testSampleWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->distinct()
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
    }

    public function testSampleWithSort(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->sortDesc('ts')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 ORDER BY `ts` DESC', $result->query);
    }

    public function testSampleWithLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 LIMIT ? OFFSET ?', $result->query);
    }

    public function testSampleWithCursor(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->cursorAfter('xyz')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testSampleWithUnion(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testSampleWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1 PREWHERE `type` IN (?)', $result->query);
    }

    public function testSampleWithFinalKeyword(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.1', $result->query);
    }

    public function testSampleWithFinalPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.2)
            ->prewhere([Query::equal('t', ['a'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.2 PREWHERE `t` IN (?)', $result->query);
    }

    public function testSampleFullPipeline(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->select(['name'])
            ->filter([Query::greaterThan('count', 0)])
            ->sortDesc('ts')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('SAMPLE 0.1', $query);
        $this->assertStringContainsString('SELECT `name`', $query);
        $this->assertStringContainsString('WHERE `count` > ?', $query);
    }

    public function testSampleInToRawSql(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->filter([Query::equal('x', [1])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `events` SAMPLE 0.1 WHERE `x` IN (1)", $sql);
    }

    public function testSamplePositionAfterFinalBeforeJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $samplePos = strpos($query, 'SAMPLE');
        $joinPos = strpos($query, 'JOIN');
        $finalPos = strpos($query, 'FINAL');

        $this->assertLessThan($samplePos, $finalPos);
        $this->assertLessThan($joinPos, $samplePos);
    }

    public function testSampleResetClearsFraction(): void
    {
        $builder = (new Builder())->from('events')->sample(0.5);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('SAMPLE', $result->query);
    }

    public function testSampleWithWhenConditional(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);

        $result2 = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->sample(0.5))
            ->build();

        $this->assertStringNotContainsString('SAMPLE', $result2->query);
    }

    public function testSampleCalledMultipleTimesLastWins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->sample(0.5)
            ->sample(0.9)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.9', $result->query);
    }

    public function testSampleWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->addHook(new class () implements Attribute {
                public function resolve(string $attribute): string
                {
                    return 'r_' . $attribute;
                }
            })
            ->filter([Query::equal('col', ['v'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('`r_col`', $result->query);
    }
    // 4. ClickHouse regex: match() function (20 tests)

    public function testRegexBasicPattern(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', 'error|warn')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result->query);
        $this->assertEquals(['error|warn'], $result->bindings);
    }

    public function testRegexWithEmptyPattern(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', '')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result->query);
        $this->assertEquals([''], $result->bindings);
    }

    public function testRegexWithSpecialChars(): void
    {
        $pattern = '^/api/v[0-9]+\\.json$';
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', $pattern)])
            ->build();
        $this->assertBindingCount($result);

        // Bindings preserve the pattern exactly as provided
        $this->assertEquals([$pattern], $result->bindings);
    }

    public function testRegexWithVeryLongPattern(): void
    {
        $longPattern = str_repeat('a', 1000);
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', $longPattern)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result->query);
        $this->assertEquals([$longPattern], $result->bindings);
    }

    public function testRegexCombinedWithOtherFilters(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([
                Query::regex('path', '^/api'),
                Query::equal('status', [200]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE match(`path`, ?) AND `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['^/api', 200], $result->bindings);
    }

    public function testRegexInPrewhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` PREWHERE match(`path`, ?)', $result->query);
        $this->assertEquals(['^/api'], $result->bindings);
    }

    public function testRegexInPrewhereAndWhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->filter([Query::regex('msg', 'err')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `logs` PREWHERE match(`path`, ?) WHERE match(`msg`, ?)',
            $result->query
        );
        $this->assertEquals(['^/api', 'err'], $result->bindings);
    }

    public function testRegexWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->addHook(new class () implements Attribute {
                public function resolve(string $attribute): string
                {
                    return 'col_' . $attribute;
                }
            })
            ->filter([Query::regex('msg', 'test')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`col_msg`, ?)', $result->query);
    }

    public function testRegexBindingPreserved(): void
    {
        $pattern = '(foo|bar)\\d+';
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', $pattern)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([$pattern], $result->bindings);
    }

    public function testMultipleRegexFilters(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([
                Query::regex('path', '^/api'),
                Query::regex('msg', 'error'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE match(`path`, ?) AND match(`msg`, ?)',
            $result->query
        );
    }

    public function testRegexInAndLogical(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::and([
                Query::regex('path', '^/api'),
                Query::greaterThan('status', 399),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE (match(`path`, ?) AND `status` > ?)',
            $result->query
        );
    }

    public function testRegexInOrLogical(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::or([
                Query::regex('path', '^/api'),
                Query::regex('path', '^/web'),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE (match(`path`, ?) OR match(`path`, ?))',
            $result->query
        );
    }

    public function testRegexInNestedLogical(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::and([
                Query::or([
                    Query::regex('path', '^/api'),
                    Query::regex('path', '^/web'),
                ]),
                Query::equal('status', [500]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('match(`path`, ?)', $result->query);
        $this->assertStringContainsString('`status` IN (?)', $result->query);
    }

    public function testRegexWithFinal(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->final()
            ->filter([Query::regex('path', '^/api')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `logs` FINAL', $result->query);
        $this->assertStringContainsString('match(`path`, ?)', $result->query);
    }

    public function testRegexWithSample(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->sample(0.5)
            ->filter([Query::regex('path', '^/api')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('match(`path`, ?)', $result->query);
    }

    public function testRegexInToRawSql(): void
    {
        $sql = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', '^/api')])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `logs` WHERE match(`path`, '^/api')", $sql);
    }

    public function testRegexCombinedWithContains(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([
                Query::regex('path', '^/api'),
                Query::contains('msg', ['error']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('match(`path`, ?)', $result->query);
        $this->assertStringContainsString('position(`msg`, ?) > 0', $result->query);
    }

    public function testRegexCombinedWithStartsWith(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([
                Query::regex('path', 'complex.*pattern'),
                Query::startsWith('msg', 'ERR'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('match(`path`, ?)', $result->query);
        $this->assertStringContainsString('startsWith(`msg`, ?)', $result->query);
    }

    public function testRegexPrewhereWithRegexWhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->filter([Query::regex('msg', 'error')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE match(`path`, ?)', $result->query);
        $this->assertStringContainsString('WHERE match(`msg`, ?)', $result->query);
        $this->assertEquals(['^/api', 'error'], $result->bindings);
    }

    public function testRegexCombinedWithPrewhereContainsRegex(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([
                Query::regex('path', '^/api'),
                Query::equal('level', ['error']),
            ])
            ->filter([Query::regex('msg', 'timeout')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['^/api', 'error', 'timeout'], $result->bindings);
    }
    // 5. Search exception (10 tests)

    public function testSearchThrowsExceptionMessage(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search is not supported by this dialect.');

        (new Builder())
            ->from('logs')
            ->filter([Query::search('content', 'hello world')])
            ->build();
    }

    public function testNotSearchThrowsExceptionMessage(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search is not supported by this dialect.');

        (new Builder())
            ->from('logs')
            ->filter([Query::notSearch('content', 'hello world')])
            ->build();
    }

    public function testSearchExceptionContainsHelpfulText(): void
    {
        try {
            (new Builder())
                ->from('logs')
                ->filter([Query::search('content', 'test')])
                ->build();
            $this->fail('Expected Exception was not thrown');
        } catch (Exception $e) {
            $this->assertStringContainsString('Full-text search', $e->getMessage());
        }
    }

    public function testSearchInLogicalAndThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->filter([Query::and([
                Query::equal('status', ['active']),
                Query::search('content', 'hello'),
            ])])
            ->build();
    }

    public function testSearchInLogicalOrThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->filter([Query::or([
                Query::equal('status', ['active']),
                Query::search('content', 'hello'),
            ])])
            ->build();
    }

    public function testSearchCombinedWithValidFiltersFailsOnSearch(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->filter([
                Query::equal('status', ['active']),
                Query::search('content', 'hello'),
            ])
            ->build();
    }

    public function testSearchInPrewhereThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->prewhere([Query::search('content', 'hello')])
            ->build();
    }

    public function testNotSearchInPrewhereThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->prewhere([Query::notSearch('content', 'hello')])
            ->build();
    }

    public function testSearchWithFinalStillThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->final()
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    public function testSearchWithSampleStillThrows(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('logs')
            ->sample(0.5)
            ->filter([Query::search('content', 'hello')])
            ->build();
    }
    // 6. ClickHouse rand() (10 tests)

    public function testRandomSortProducesLowercaseRand(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('rand()', $result->query);
        $this->assertStringNotContainsString('RAND()', $result->query);
    }

    public function testRandomSortCombinedWithAsc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortAsc('name')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY `name` ASC, rand()', $result->query);
    }

    public function testRandomSortCombinedWithDesc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortDesc('ts')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY `ts` DESC, rand()', $result->query);
    }

    public function testRandomSortCombinedWithAscAndDesc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortAsc('name')
            ->sortDesc('ts')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY `name` ASC, `ts` DESC, rand()', $result->query);
    }

    public function testRandomSortWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` FINAL ORDER BY rand()', $result->query);
    }

    public function testRandomSortWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 ORDER BY rand()', $result->query);
    }

    public function testRandomSortWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) ORDER BY rand()',
            $result->query
        );
    }

    public function testRandomSortWithLimit(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand() LIMIT ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testRandomSortWithFiltersAndJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->filter([Query::equal('status', ['active'])])
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
        $this->assertStringContainsString('ORDER BY rand()', $result->query);
    }

    public function testRandomSortAlone(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand()', $result->query);
        $this->assertEquals([], $result->bindings);
    }
    // 7. All filter types work correctly (31 tests)

    public function testFilterEqualSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('a', ['x'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $result->query);
        $this->assertEquals(['x'], $result->bindings);
    }

    public function testFilterEqualMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('a', ['x', 'y', 'z'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?, ?, ?)', $result->query);
        $this->assertEquals(['x', 'y', 'z'], $result->bindings);
    }

    public function testFilterNotEqualSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('a', 'x')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` != ?', $result->query);
        $this->assertEquals(['x'], $result->bindings);
    }

    public function testFilterNotEqualMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('a', ['x', 'y'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT IN (?, ?)', $result->query);
        $this->assertEquals(['x', 'y'], $result->bindings);
    }

    public function testFilterLessThanValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::lessThan('a', 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` < ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testFilterLessThanEqualValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::lessThanEqual('a', 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` <= ?', $result->query);
    }

    public function testFilterGreaterThanValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('a', 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` > ?', $result->query);
    }

    public function testFilterGreaterThanEqualValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThanEqual('a', 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` >= ?', $result->query);
    }

    public function testFilterBetweenValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::between('a', 1, 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([1, 10], $result->bindings);
    }

    public function testFilterNotBetweenValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notBetween('a', 1, 10)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT BETWEEN ? AND ?', $result->query);
    }

    public function testFilterStartsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::startsWith('a', 'foo')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE startsWith(`a`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testFilterNotStartsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notStartsWith('a', 'foo')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE NOT startsWith(`a`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testFilterEndsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::endsWith('a', 'bar')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE endsWith(`a`, ?)', $result->query);
        $this->assertEquals(['bar'], $result->bindings);
    }

    public function testFilterNotEndsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEndsWith('a', 'bar')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE NOT endsWith(`a`, ?)', $result->query);
        $this->assertEquals(['bar'], $result->bindings);
    }

    public function testFilterContainsSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('a', ['foo'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE position(`a`, ?) > 0', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testFilterContainsMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('a', ['foo', 'bar'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (position(`a`, ?) > 0 OR position(`a`, ?) > 0)', $result->query);
        $this->assertEquals(['foo', 'bar'], $result->bindings);
    }

    public function testFilterContainsAnyValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::containsAny('a', ['x', 'y'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (position(`a`, ?) > 0 OR position(`a`, ?) > 0)', $result->query);
    }

    public function testFilterContainsAllValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::containsAll('a', ['x', 'y'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (position(`a`, ?) > 0 AND position(`a`, ?) > 0)', $result->query);
        $this->assertEquals(['x', 'y'], $result->bindings);
    }

    public function testFilterNotContainsSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notContains('a', ['foo'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE position(`a`, ?) = 0', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testFilterNotContainsMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notContains('a', ['foo', 'bar'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (position(`a`, ?) = 0 AND position(`a`, ?) = 0)', $result->query);
    }

    public function testFilterIsNullValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::isNull('a')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IS NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testFilterIsNotNullValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::isNotNull('a')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IS NOT NULL', $result->query);
    }

    public function testFilterExistsValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::exists(['a', 'b'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IS NOT NULL AND `b` IS NOT NULL)', $result->query);
    }

    public function testFilterNotExistsValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notExists(['a', 'b'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IS NULL AND `b` IS NULL)', $result->query);
    }

    public function testFilterAndLogical(): void
    {
        $result = (new Builder())->from('t')->filter([
            Query::and([Query::equal('a', [1]), Query::equal('b', [2])]),
        ])->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) AND `b` IN (?))', $result->query);
    }

    public function testFilterOrLogical(): void
    {
        $result = (new Builder())->from('t')->filter([
            Query::or([Query::equal('a', [1]), Query::equal('b', [2])]),
        ])->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR `b` IN (?))', $result->query);
    }

    public function testFilterRaw(): void
    {
        $result = (new Builder())->from('t')->filter([Query::raw('x > ? AND y < ?', [1, 2])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE x > ? AND y < ?', $result->query);
        $this->assertEquals([1, 2], $result->bindings);
    }

    public function testFilterDeeplyNestedLogical(): void
    {
        $result = (new Builder())->from('t')->filter([
            Query::and([
                Query::or([
                    Query::equal('a', [1]),
                    Query::and([
                        Query::greaterThan('b', 2),
                        Query::lessThan('c', 3),
                    ]),
                ]),
                Query::equal('d', [4]),
            ]),
        ])->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`a` IN (?) OR (`b` > ? AND `c` < ?))', $result->query);
        $this->assertStringContainsString('`d` IN (?)', $result->query);
    }

    public function testFilterWithFloats(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('price', 9.99)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals([9.99], $result->bindings);
    }

    public function testFilterWithNegativeNumbers(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('temp', -40)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals([-40], $result->bindings);
    }

    public function testFilterWithEmptyStrings(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('name', [''])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals([''], $result->bindings);
    }
    // 8. Aggregation with ClickHouse features (15 tests)

    public function testAggregationCountWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) AS `total` FROM `events` FINAL', $result->query);
    }

    public function testAggregationSumWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->sum('amount', 'total_amount')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`amount`) AS `total_amount` FROM `events` SAMPLE 0.1', $result->query);
    }

    public function testAggregationAvgWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->avg('price', 'avg_price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('AVG(`price`) AS `avg_price`', $result->query);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
    }

    public function testAggregationMinWithPrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->filter([Query::greaterThan('amount', 0)])
            ->min('price', 'min_price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MIN(`price`) AS `min_price`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testAggregationMaxWithAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['sale'])])
            ->max('price', 'max_price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MAX(`price`) AS `max_price`', $result->query);
        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testMultipleAggregationsWithPrewhereGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->groupBy(['region'])
            ->having([Query::greaterThan('cnt', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('SUM(`amount`) AS `total`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('GROUP BY `region`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
    }

    public function testAggregationWithJoinFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('COUNT(*)', $result->query);
    }

    public function testAggregationWithDistinctSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->distinct()
            ->count('user_id', 'unique_users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
    }

    public function testAggregationWithAliasPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'click_count')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `click_count`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testAggregationWithoutAliasFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
        $this->assertStringContainsString('FINAL', $result->query);
    }

    public function testCountStarAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testAggregationAllFeaturesUnion(): void
    {
        $other = (new Builder())->from('archive')->count('*', 'total');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testAggregationAttributeResolverPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->addHook(new AttributeMap([
                'amt' => 'amount_cents',
            ]))
            ->prewhere([Query::equal('type', ['sale'])])
            ->sum('amt', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(`amount_cents`)', $result->query);
    }

    public function testAggregationConditionProviderPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->count('*', 'cnt')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testGroupByHavingPrewhereFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['sale'])])
            ->count('*', 'cnt')
            ->groupBy(['region'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('FINAL', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('GROUP BY', $query);
        $this->assertStringContainsString('HAVING', $query);
    }
    // 9. Join with ClickHouse features (15 tests)

    public function testJoinWithFinalFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` FINAL JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result->query
        );
    }

    public function testJoinWithSampleFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `events` SAMPLE 0.5 JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result->query
        );
    }

    public function testJoinWithPrewhereFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testJoinWithPrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testJoinAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('JOIN', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
    }

    public function testLeftJoinWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->leftJoin('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `users`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testRightJoinWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->rightJoin('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN `users`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testCrossJoinWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->crossJoin('config')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('CROSS JOIN `config`', $result->query);
    }

    public function testMultipleJoinsWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->leftJoin('sessions', 'events.sid', 'sessions.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `sessions`', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testJoinAggregationPrewhereGroupBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['sale'])])
            ->count('*', 'cnt')
            ->groupBy(['users.country'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('GROUP BY', $result->query);
    }

    public function testJoinPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['click', 18], $result->bindings);
    }

    public function testJoinAttributeResolverPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->addHook(new AttributeMap([
                'uid' => 'user_id',
            ]))
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('uid', ['abc'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `user_id` IN (?)', $result->query);
    }

    public function testJoinConditionProviderPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testJoinPrewhereUnion(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testJoinClauseOrdering(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;

        $fromPos = strpos($query, 'FROM');
        $finalPos = strpos($query, 'FINAL');
        $samplePos = strpos($query, 'SAMPLE');
        $joinPos = strpos($query, 'JOIN');
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');

        $this->assertLessThan($finalPos, $fromPos);
        $this->assertLessThan($samplePos, $finalPos);
        $this->assertLessThan($joinPos, $samplePos);
        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
    }
    // 10. Union with ClickHouse features (10 tests)

    public function testUnionMainHasFinal(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('UNION (SELECT * FROM `archive`)', $result->query);
    }

    public function testUnionMainHasSample(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testUnionMainHasPrewhere(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testUnionMainHasAllClickHouseFeatures(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testUnionAllWithPrewhere(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->unionAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION ALL', $result->query);
    }

    public function testUnionBindingOrderWithPrewhere(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::equal('year', [2024])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        // prewhere, where, union
        $this->assertEquals(['click', 2024, 2023], $result->bindings);
    }

    public function testMultipleUnionsWithPrewhere(): void
    {
        $other1 = (new Builder())->from('archive1');
        $other2 = (new Builder())->from('archive2');
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other1)
            ->union($other2)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertEquals(2, substr_count($result->query, 'UNION'));
    }

    public function testUnionJoinPrewhere(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testUnionAggregationPrewhereFinal(): void
    {
        $other = (new Builder())->from('archive')->count('*', 'total');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('COUNT(*)', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testUnionWithComplexMainQuery(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->select(['name', 'count'])
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->sortDesc('count')
            ->limit(10)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('SELECT `name`, `count`', $query);
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('UNION', $query);
    }
    // 11. toRawSql with ClickHouse features (15 tests)

    public function testToRawSqlWithFinalFeature(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->final()
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` FINAL', $sql);
    }

    public function testToRawSqlWithSampleFeature(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1', $sql);
    }

    public function testToRawSqlWithPrewhereFeature(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `events` PREWHERE `type` IN ('click')", $sql);
    }

    public function testToRawSqlWithPrewhereWhere(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->toRawSql();

        $this->assertEquals(
            "SELECT * FROM `events` PREWHERE `type` IN ('click') WHERE `count` > 5",
            $sql
        );
    }

    public function testToRawSqlWithAllFeatures(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->toRawSql();

        $this->assertEquals(
            "SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `type` IN ('click') WHERE `count` > 5",
            $sql
        );
    }

    public function testToRawSqlAllFeaturesCombined(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sortDesc('ts')
            ->limit(10)
            ->offset(20)
            ->toRawSql();

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $sql);
        $this->assertStringContainsString("PREWHERE `type` IN ('click')", $sql);
        $this->assertStringContainsString('WHERE `count` > 5', $sql);
        $this->assertStringContainsString('ORDER BY `ts` DESC', $sql);
        $this->assertStringContainsString('LIMIT 10', $sql);
        $this->assertStringContainsString('OFFSET 20', $sql);
    }

    public function testToRawSqlWithStringBindings(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->filter([Query::equal('name', ['hello world'])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `events` WHERE `name` IN ('hello world')", $sql);
    }

    public function testToRawSqlWithNumericBindings(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->filter([Query::greaterThan('count', 42)])
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` WHERE `count` > 42', $sql);
    }

    public function testToRawSqlWithBooleanBindings(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->filter([Query::equal('active', [true])])
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` WHERE `active` IN (1)', $sql);
    }

    public function testToRawSqlWithNullBindings(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->filter([Query::raw('x = ?', [null])])
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` WHERE x = NULL', $sql);
    }

    public function testToRawSqlWithFloatBindings(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->filter([Query::greaterThan('price', 9.99)])
            ->toRawSql();

        $this->assertEquals('SELECT * FROM `events` WHERE `price` > 9.99', $sql);
    }

    public function testToRawSqlCalledTwiceGivesSameResult(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)]);

        $sql1 = $builder->toRawSql();
        $sql2 = $builder->toRawSql();

        $this->assertEquals($sql1, $sql2);
    }

    public function testToRawSqlWithUnionPrewhere(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $sql = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->toRawSql();

        $this->assertStringContainsString("PREWHERE `type` IN ('click')", $sql);
        $this->assertStringContainsString('UNION', $sql);
    }

    public function testToRawSqlWithJoinPrewhere(): void
    {
        $sql = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->toRawSql();

        $this->assertStringContainsString('JOIN `users`', $sql);
        $this->assertStringContainsString("PREWHERE `type` IN ('click')", $sql);
    }

    public function testToRawSqlWithRegexMatch(): void
    {
        $sql = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', '^/api')])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `logs` WHERE match(`path`, '^/api')", $sql);
    }
    // 12. Reset comprehensive (15 tests)

    public function testResetClearsPrewhereState(): void
    {
        $builder = (new Builder())->from('events')->prewhere([Query::equal('type', ['click'])]);
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('PREWHERE', $result->query);
    }

    public function testResetClearsFinalState(): void
    {
        $builder = (new Builder())->from('events')->final();
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testResetClearsSampleState(): void
    {
        $builder = (new Builder())->from('events')->sample(0.5);
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('SAMPLE', $result->query);
    }

    public function testResetClearsAllThreeTogether(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])]);
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `events`', $result->query);
    }

    public function testResetPreservesAttributeResolver(): void
    {
        $hook = new class () implements Attribute {
            public function resolve(string $attribute): string
            {
                return 'r_' . $attribute;
            }
        };
        $builder = (new Builder())
            ->from('events')
            ->addHook($hook)
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->filter([Query::equal('col', ['v'])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`r_col`', $result->query);
    }

    public function testResetPreservesConditionProviders(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testResetClearsTable(): void
    {
        $builder = (new Builder())->from('events');
        $builder->build();
        $builder->reset();

        $result = $builder->from('logs')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `logs`', $result->query);
        $this->assertStringNotContainsString('events', $result->query);
    }

    public function testResetClearsFilters(): void
    {
        $builder = (new Builder())->from('events')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('WHERE', $result->query);
    }

    public function testResetClearsUnions(): void
    {
        $other = (new Builder())->from('archive');
        $builder = (new Builder())->from('events')->union($other);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('UNION', $result->query);
    }

    public function testResetClearsBindings(): void
    {
        $builder = (new Builder())->from('events')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testBuildAfterResetMinimalOutput(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sortDesc('ts')
            ->limit(10);
        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testResetRebuildWithPrewhere(): void
    {
        $builder = new Builder();
        $builder->from('events')->final()->build();
        $builder->reset();

        $result = $builder->from('events')->prewhere([Query::equal('x', [1])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testResetRebuildWithFinal(): void
    {
        $builder = new Builder();
        $builder->from('events')->prewhere([Query::equal('x', [1])])->build();
        $builder->reset();

        $result = $builder->from('events')->final()->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringNotContainsString('PREWHERE', $result->query);
    }

    public function testResetRebuildWithSample(): void
    {
        $builder = new Builder();
        $builder->from('events')->final()->build();
        $builder->reset();

        $result = $builder->from('events')->sample(0.5)->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testMultipleResets(): void
    {
        $builder = new Builder();

        $builder->from('a')->final()->build();
        $builder->reset();
        $builder->from('b')->sample(0.5)->build();
        $builder->reset();
        $builder->from('c')->prewhere([Query::equal('x', [1])])->build();
        $builder->reset();

        $result = $builder->from('d')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `d`', $result->query);
        $this->assertEquals([], $result->bindings);
    }
    // 13. when() with ClickHouse features (10 tests)

    public function testWhenTrueAddsPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
    }

    public function testWhenFalseDoesNotAddPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('PREWHERE', $result->query);
    }

    public function testWhenTrueAddsFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
    }

    public function testWhenFalseDoesNotAddFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->final())
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testWhenTrueAddsSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
    }

    public function testWhenWithBothPrewhereAndFilter(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(
                true,
                fn (Builder $b) => $b
                ->prewhere([Query::equal('type', ['click'])])
                ->filter([Query::greaterThan('count', 5)])
            )
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testWhenNestedWithClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(
                true,
                fn (Builder $b) => $b
                ->final()
                ->when(true, fn (Builder $b2) => $b2->sample(0.5))
            )
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result->query);
    }

    public function testWhenChainedMultipleTimesWithClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->when(true, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testWhenAddsJoinAndPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(
                true,
                fn (Builder $b) => $b
                ->join('users', 'events.uid', 'users.id')
                ->prewhere([Query::equal('type', ['click'])])
            )
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testWhenCombinedWithRegularWhen(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
    }
    // 14. Condition provider with ClickHouse (10 tests)

    public function testProviderWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('deleted = ?', $result->query);
    }

    public function testProviderWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('deleted = ?', $result->query);
    }

    public function testProviderWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('deleted = ?', $result->query);
    }

    public function testProviderPrewhereWhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        // prewhere, filter, provider
        $this->assertEquals(['click', 5, 't1'], $result->bindings);
    }

    public function testMultipleProvidersPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['o1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['click', 't1', 'o1'], $result->bindings);
    }

    public function testProviderPrewhereCursorLimitBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        // prewhere, provider, cursor, limit
        $this->assertEquals('click', $result->bindings[0]);
        $this->assertEquals('t1', $result->bindings[1]);
        $this->assertEquals('cur1', $result->bindings[2]);
        $this->assertEquals(10, $result->bindings[3]);
    }

    public function testProviderAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testProviderPrewhereAggregation(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->count('*', 'cnt')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*)', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testProviderJoinsPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('tenant = ?', $result->query);
    }

    public function testProviderReferencesTableNameFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition($table . '.deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('events.deleted = ?', $result->query);
        $this->assertStringContainsString('FINAL', $result->query);
    }
    // 15. Cursor with ClickHouse features (8 tests)

    public function testCursorAfterWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testCursorBeforeWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorBefore('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('`_cursor` < ?', $result->query);
    }

    public function testCursorPrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testCursorWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testCursorWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }

    public function testCursorPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('click', $result->bindings[0]);
        $this->assertEquals('cur1', $result->bindings[1]);
    }

    public function testCursorPrewhereProviderBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('click', $result->bindings[0]);
        $this->assertEquals('t1', $result->bindings[1]);
        $this->assertEquals('cur1', $result->bindings[2]);
    }

    public function testCursorFullClickHousePipeline(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('`_cursor` > ?', $query);
        $this->assertStringContainsString('LIMIT', $query);
    }
    // 16. page() with ClickHouse features (5 tests)

    public function testPageWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->page(2, 25)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals(['click', 25, 25], $result->bindings);
    }

    public function testPageWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->page(3, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals([10, 20], $result->bindings);
    }

    public function testPageWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->page(1, 50)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertEquals([50, 0], $result->bindings);
    }

    public function testPageWithAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->page(2, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('LIMIT', $result->query);
        $this->assertStringContainsString('OFFSET', $result->query);
    }

    public function testPageWithComplexClickHouseQuery(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->sortDesc('ts')
            ->page(5, 20)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('FINAL', $query);
        $this->assertStringContainsString('SAMPLE', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('OFFSET', $query);
    }
    // 17. Fluent chaining comprehensive (5 tests)

    public function testAllClickHouseMethodsReturnSameInstance(): void
    {
        $builder = new Builder();
        $this->assertSame($builder, $builder->final());
        $this->assertSame($builder, $builder->sample(0.5));
        $this->assertSame($builder, $builder->prewhere([]));
        $this->assertSame($builder, $builder->reset());
    }

    public function testChainingClickHouseMethodsWithBaseMethods(): void
    {
        $builder = new Builder();
        $result = $builder
            ->from('events')
            ->final()
            ->sample(0.1)
            ->select(['name'])
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->sortDesc('ts')
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        $this->assertNotEmpty($result->query);
    }

    public function testChainingOrderDoesNotMatterForOutput(): void
    {
        $result1 = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();

        $result2 = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sample(0.1)
            ->filter([Query::greaterThan('count', 5)])
            ->final()
            ->build();

        $this->assertEquals($result1->query, $result2->query);
    }

    public function testSameComplexQueryDifferentOrders(): void
    {
        $result1 = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sortDesc('ts')
            ->limit(10)
            ->build();

        $result2 = (new Builder())
            ->from('events')
            ->sortDesc('ts')
            ->limit(10)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sample(0.1)
            ->final()
            ->build();

        $this->assertEquals($result1->query, $result2->query);
    }

    public function testFluentResetThenRebuild(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1);
        $builder->build();

        $result = $builder->reset()
            ->from('logs')
            ->sample(0.5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `logs` SAMPLE 0.5', $result->query);
        $this->assertStringNotContainsString('FINAL', $result->query);
    }
    // 18. SQL clause ordering verification (10 tests)

    public function testClauseOrderSelectFromFinalSampleJoinPrewhereWhereGroupByHavingOrderByLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->count('*', 'cnt')
            ->select(['users.name'])
            ->groupBy(['users.name'])
            ->having([Query::greaterThan('cnt', 5)])
            ->sortDesc('cnt')
            ->limit(50)
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;

        $selectPos = strpos($query, 'SELECT');
        $fromPos = strpos($query, 'FROM');
        $finalPos = strpos($query, 'FINAL');
        $samplePos = strpos($query, 'SAMPLE');
        $joinPos = strpos($query, 'JOIN');
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');
        $groupByPos = strpos($query, 'GROUP BY');
        $havingPos = strpos($query, 'HAVING');
        $orderByPos = strpos($query, 'ORDER BY');
        $limitPos = strpos($query, 'LIMIT');
        $offsetPos = strpos($query, 'OFFSET');

        $this->assertLessThan($fromPos, $selectPos);
        $this->assertLessThan($finalPos, $fromPos);
        $this->assertLessThan($samplePos, $finalPos);
        $this->assertLessThan($joinPos, $samplePos);
        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
        $this->assertLessThan($groupByPos, $wherePos);
        $this->assertLessThan($havingPos, $groupByPos);
        $this->assertLessThan($orderByPos, $havingPos);
        $this->assertLessThan($limitPos, $orderByPos);
        $this->assertLessThan($offsetPos, $limitPos);
    }

    public function testFinalComesAfterTableBeforeJoin(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $tablePos = strpos($query, '`events`');
        $finalPos = strpos($query, 'FINAL');
        $joinPos = strpos($query, 'JOIN');

        $this->assertLessThan($finalPos, $tablePos);
        $this->assertLessThan($joinPos, $finalPos);
    }

    public function testSampleComesAfterFinalBeforeJoin(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $finalPos = strpos($query, 'FINAL');
        $samplePos = strpos($query, 'SAMPLE');
        $joinPos = strpos($query, 'JOIN');

        $this->assertLessThan($samplePos, $finalPos);
        $this->assertLessThan($joinPos, $samplePos);
    }

    public function testPrewhereComesAfterJoinBeforeWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $joinPos = strpos($query, 'JOIN');
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');

        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testPrewhereBeforeGroupBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $prewherePos = strpos($query, 'PREWHERE');
        $groupByPos = strpos($query, 'GROUP BY');

        $this->assertLessThan($groupByPos, $prewherePos);
    }

    public function testPrewhereBeforeOrderBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sortDesc('ts')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $prewherePos = strpos($query, 'PREWHERE');
        $orderByPos = strpos($query, 'ORDER BY');

        $this->assertLessThan($orderByPos, $prewherePos);
    }

    public function testPrewhereBeforeLimit(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $prewherePos = strpos($query, 'PREWHERE');
        $limitPos = strpos($query, 'LIMIT');

        $this->assertLessThan($limitPos, $prewherePos);
    }

    public function testFinalSampleBeforePrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $finalPos = strpos($query, 'FINAL');
        $samplePos = strpos($query, 'SAMPLE');
        $prewherePos = strpos($query, 'PREWHERE');

        $this->assertLessThan($samplePos, $finalPos);
        $this->assertLessThan($prewherePos, $samplePos);
    }

    public function testWhereBeforeHaving(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([Query::greaterThan('count', 0)])
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $wherePos = strpos($query, 'WHERE');
        $havingPos = strpos($query, 'HAVING');

        $this->assertLessThan($havingPos, $wherePos);
    }

    public function testFullQueryAllClausesAllPositions(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->distinct()
            ->select(['name'])
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->count('*', 'cnt')
            ->groupBy(['name'])
            ->having([Query::greaterThan('cnt', 5)])
            ->sortDesc('cnt')
            ->limit(50)
            ->offset(10)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;

        // All elements present
        $this->assertStringContainsString('SELECT DISTINCT', $query);
        $this->assertStringContainsString('FINAL', $query);
        $this->assertStringContainsString('SAMPLE', $query);
        $this->assertStringContainsString('JOIN', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('GROUP BY', $query);
        $this->assertStringContainsString('HAVING', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('OFFSET', $query);
        $this->assertStringContainsString('UNION', $query);
    }
    // 19. Batch mode with ClickHouse (5 tests)

    public function testQueriesMethodWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->queries([
                Query::equal('status', ['active']),
                Query::orderDesc('ts'),
                Query::limit(10),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
        $this->assertStringContainsString('ORDER BY', $result->query);
        $this->assertStringContainsString('LIMIT', $result->query);
    }

    public function testQueriesMethodWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->queries([
                Query::equal('status', ['active']),
                Query::limit(10),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
    }

    public function testQueriesMethodWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->queries([
                Query::equal('status', ['active']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SAMPLE 0.5', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testQueriesMethodWithAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->queries([
                Query::equal('status', ['active']),
                Query::orderDesc('ts'),
                Query::limit(10),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('ORDER BY', $result->query);
    }

    public function testQueriesComparedToFluentApiSameSql(): void
    {
        $resultA = (new Builder())
            ->from('events')
            ->filter([Query::equal('status', ['active'])])
            ->sortDesc('ts')
            ->limit(10)
            ->build();

        $resultB = (new Builder())
            ->from('events')
            ->queries([
                Query::equal('status', ['active']),
                Query::orderDesc('ts'),
                Query::limit(10),
            ])
            ->build();

        $this->assertEquals($resultA->query, $resultB->query);
        $this->assertEquals($resultA->bindings, $resultB->bindings);
    }
    // 20. Edge cases (10 tests)

    public function testEmptyTableNameWithFinal(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->final()
            ->build();
    }

    public function testEmptyTableNameWithSample(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->sample(0.5)
            ->build();
    }

    public function testPrewhereWithEmptyFilterValues(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', [])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
    }

    public function testVeryLongTableNameWithFinalSample(): void
    {
        $longName = str_repeat('a', 200);
        $result = (new Builder())
            ->from($longName)
            ->final()
            ->sample(0.1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`' . $longName . '`', $result->query);
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result->query);
    }

    public function testMultipleBuildsConsistentOutput(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)]);

        $result1 = $builder->build();
        $result2 = $builder->build();
        $result3 = $builder->build();

        $this->assertEquals($result1->query, $result2->query);
        $this->assertEquals($result2->query, $result3->query);
        $this->assertEquals($result1->bindings, $result2->bindings);
        $this->assertEquals($result2->bindings, $result3->bindings);
    }

    public function testBuildResetsBindingsButNotClickHouseState(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])]);

        $result1 = $builder->build();
        $result2 = $builder->build();

        // ClickHouse state persists
        $this->assertStringContainsString('FINAL', $result2->query);
        $this->assertStringContainsString('SAMPLE', $result2->query);
        $this->assertStringContainsString('PREWHERE', $result2->query);

        // Bindings are consistent
        $this->assertEquals($result1->bindings, $result2->bindings);
    }

    public function testSampleWithAllBindingTypes(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->filter([Query::greaterThan('count', 5)])
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->having([Query::greaterThan('cnt', 10)])
            ->limit(50)
            ->offset(100)
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        // Verify all binding types present
        $this->assertNotEmpty($result->bindings);
        $this->assertGreaterThan(5, count($result->bindings));
    }

    public function testPrewhereAppearsCorrectlyWithoutJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);

        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testPrewhereAppearsCorrectlyWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $joinPos = strpos($query, 'JOIN');
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');

        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testFinalSampleTextInOutputWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->join('users', 'events.uid', 'users.id')
            ->leftJoin('sessions', 'events.sid', 'sessions.id')
            ->build();
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('JOIN `users`', $query);
        $this->assertStringContainsString('LEFT JOIN `sessions`', $query);

        // FINAL SAMPLE appears before JOINs
        $finalSamplePos = strpos($query, 'FINAL SAMPLE 0.1');
        $joinPos = strpos($query, 'JOIN');
        $this->assertLessThan($joinPos, $finalSamplePos);
    }
    // 1. Spatial/Vector/ElemMatch Exception Tests

    public function testFilterCrossesThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::crosses('attr', [1])])->build();
    }

    public function testFilterNotCrossesThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::notCrosses('attr', [1])])->build();
    }

    public function testFilterDistanceEqualThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::distanceEqual('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceNotEqualThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::distanceNotEqual('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceGreaterThanThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::distanceGreaterThan('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceLessThanThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::distanceLessThan('attr', [0, 0], 1)])->build();
    }

    public function testFilterIntersectsThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::intersects('attr', [1])])->build();
    }

    public function testFilterNotIntersectsThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::notIntersects('attr', [1])])->build();
    }

    public function testFilterOverlapsThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::overlaps('attr', [1])])->build();
    }

    public function testFilterNotOverlapsThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::notOverlaps('attr', [1])])->build();
    }

    public function testFilterTouchesThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::touches('attr', [1])])->build();
    }

    public function testFilterNotTouchesThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::notTouches('attr', [1])])->build();
    }

    public function testFilterVectorDotThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorDot('attr', [1.0, 2.0])])->build();
    }

    public function testFilterVectorCosineThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorCosine('attr', [1.0, 2.0])])->build();
    }

    public function testFilterVectorEuclideanThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorEuclidean('attr', [1.0, 2.0])])->build();
    }

    public function testFilterElemMatchThrowsException(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::elemMatch('attr', [Query::equal('x', [1])])])->build();
    }
    // 2. SAMPLE Boundary Values

    public function testSampleZero(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->sample(0.0);
    }

    public function testSampleOne(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->sample(1.0);
    }

    public function testSampleNegative(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->sample(-0.5);
    }

    public function testSampleGreaterThanOne(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->sample(2.0);
    }

    public function testSampleVerySmall(): void
    {
        $result = (new Builder())->from('t')->sample(0.001)->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('SAMPLE 0.001', $result->query);
    }
    // 3. Standalone Compiler Method Tests

    public function testCompileFilterStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::greaterThan('age', 18));
        $this->assertEquals('`age` > ?', $sql);
        $this->assertEquals([18], $builder->getBindings());
    }

    public function testCompileOrderAscStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderAsc('name'));
        $this->assertEquals('`name` ASC', $sql);
    }

    public function testCompileOrderDescStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderDesc('name'));
        $this->assertEquals('`name` DESC', $sql);
    }

    public function testCompileOrderRandomStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderRandom());
        $this->assertEquals('rand()', $sql);
    }

    public function testCompileOrderExceptionStandalone(): void
    {
        $builder = new Builder();
        $this->expectException(UnsupportedException::class);
        $builder->compileOrder(Query::limit(10));
    }

    public function testCompileLimitStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileLimit(Query::limit(10));
        $this->assertEquals('LIMIT ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testCompileOffsetStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOffset(Query::offset(5));
        $this->assertEquals('OFFSET ?', $sql);
        $this->assertEquals([5], $builder->getBindings());
    }

    public function testCompileSelectStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileSelect(Query::select(['a', 'b']));
        $this->assertEquals('`a`, `b`', $sql);
    }

    public function testCompileSelectEmptyStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileSelect(Query::select([]));
        $this->assertEquals('', $sql);
    }

    public function testCompileCursorAfterStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileCursor(Query::cursorAfter('abc'));
        $this->assertEquals('`_cursor` > ?', $sql);
        $this->assertEquals(['abc'], $builder->getBindings());
    }

    public function testCompileCursorBeforeStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileCursor(Query::cursorBefore('xyz'));
        $this->assertEquals('`_cursor` < ?', $sql);
        $this->assertEquals(['xyz'], $builder->getBindings());
    }

    public function testCompileAggregateCountStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::count('*', 'total'));
        $this->assertEquals('COUNT(*) AS `total`', $sql);
    }

    public function testCompileAggregateSumStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::sum('price'));
        $this->assertEquals('SUM(`price`)', $sql);
    }

    public function testCompileAggregateAvgWithAliasStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::avg('score', 'avg_score'));
        $this->assertEquals('AVG(`score`) AS `avg_score`', $sql);
    }

    public function testCompileGroupByStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileGroupBy(Query::groupBy(['status', 'country']));
        $this->assertEquals('`status`, `country`', $sql);
    }

    public function testCompileGroupByEmptyStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileGroupBy(Query::groupBy([]));
        $this->assertEquals('', $sql);
    }

    public function testCompileJoinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileJoin(Query::join('orders', 'u.id', 'o.uid'));
        $this->assertEquals('JOIN `orders` ON `u`.`id` = `o`.`uid`', $sql);
    }

    public function testCompileJoinExceptionStandalone(): void
    {
        $builder = new Builder();
        $this->expectException(UnsupportedException::class);
        $builder->compileJoin(Query::equal('x', [1]));
    }
    // 4. Union with ClickHouse Features on Both Sides

    public function testUnionBothWithClickHouseFeatures(): void
    {
        $sub = (new Builder())->from('archive')
            ->final()
            ->sample(0.5)
            ->filter([Query::equal('status', ['closed'])]);
        $result = (new Builder())->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
        $this->assertStringContainsString('FROM `archive` FINAL SAMPLE 0.5', $result->query);
    }

    public function testUnionAllBothWithFinal(): void
    {
        $sub = (new Builder())->from('b')->final();
        $result = (new Builder())->from('a')->final()
            ->unionAll($sub)
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `a` FINAL', $result->query);
        $this->assertStringContainsString('UNION ALL (SELECT * FROM `b` FINAL)', $result->query);
    }
    // 5. PREWHERE Binding Order Exhaustive Tests

    public function testPrewhereBindingOrderWithFilterAndHaving(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->groupBy(['type'])
            ->having([Query::greaterThan('total', 10)])
            ->build();
        $this->assertBindingCount($result);
        // Binding order: prewhere, filter, having
        $this->assertEquals(['click', 5, 10], $result->bindings);
    }

    public function testPrewhereBindingOrderWithProviderAndCursor(): void
    {
        $result = (new Builder())->from('t')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);
        // Binding order: prewhere, filter(none), provider, cursor
        $this->assertEquals(['click', 't1', 'abc'], $result->bindings);
    }

    public function testPrewhereMultipleFiltersBindingOrder(): void
    {
        $result = (new Builder())->from('t')
            ->prewhere([
                Query::equal('type', ['a']),
                Query::greaterThan('priority', 3),
            ])
            ->filter([Query::lessThan('age', 30)])
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);
        // prewhere bindings first, then filter, then limit
        $this->assertEquals(['a', 3, 30, 10], $result->bindings);
    }
    // 6. Search Exception in PREWHERE Interaction

    public function testSearchInFilterThrowsExceptionWithMessage(): void
    {
        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Full-text search');
        (new Builder())->from('t')->filter([Query::search('content', 'hello')])->build();
    }

    public function testSearchInPrewhereThrowsExceptionWithMessage(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->prewhere([Query::search('content', 'hello')])->build();
    }
    // 7. Join Combinations with FINAL/SAMPLE

    public function testLeftJoinWithFinalAndSample(): void
    {
        $result = (new Builder())->from('events')
            ->final()
            ->sample(0.1)
            ->leftJoin('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 LEFT JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result->query
        );
    }

    public function testRightJoinWithFinalFeature(): void
    {
        $result = (new Builder())->from('events')
            ->final()
            ->rightJoin('users', 'events.uid', 'users.id')
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `events` FINAL', $result->query);
        $this->assertStringContainsString('RIGHT JOIN', $result->query);
    }

    public function testCrossJoinWithPrewhereFeature(): void
    {
        $result = (new Builder())->from('events')
            ->crossJoin('colors')
            ->prewhere([Query::equal('type', ['a'])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('CROSS JOIN `colors`', $result->query);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result->query);
        $this->assertEquals(['a'], $result->bindings);
    }

    public function testJoinWithNonDefaultOperator(): void
    {
        $result = (new Builder())->from('t')
            ->join('other', 'a', 'b', '!=')
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('JOIN `other` ON `a` != `b`', $result->query);
    }
    // 8. Condition Provider Position Verification

    public function testConditionProviderInWhereNotPrewhere(): void
    {
        $result = (new Builder())->from('t')
            ->prewhere([Query::equal('type', ['click'])])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);
        $query = $result->query;
        $prewherePos = strpos($query, 'PREWHERE');
        $wherePos = strpos($query, 'WHERE');
        // Provider should be in WHERE which comes after PREWHERE
        $this->assertNotFalse($prewherePos);
        $this->assertNotFalse($wherePos);
        $this->assertGreaterThan($prewherePos, $wherePos);
        $this->assertStringContainsString('WHERE _tenant = ?', $query);
    }

    public function testConditionProviderWithNoFiltersClickHouse(): void
    {
        $result = (new Builder())->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_deleted = ?', [0]);
                }
            })
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE _deleted = ?', $result->query);
        $this->assertEquals([0], $result->bindings);
    }
    // 9. Page Boundary Values

    public function testPageZero(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->page(0, 10)->build();
    }

    public function testPageNegative(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())->from('t')->page(-1, 10)->build();
    }

    public function testPageLargeNumber(): void
    {
        $result = (new Builder())->from('t')->page(1000000, 25)->build();
        $this->assertBindingCount($result);
        $this->assertEquals([25, 24999975], $result->bindings);
    }
    // 10. Build Without From

    public function testBuildWithoutFrom(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())->filter([Query::equal('x', [1])])->build();
    }
    // 11. toRawSql Edge Cases for ClickHouse

    public function testToRawSqlWithFinalAndSampleEdge(): void
    {
        $sql = (new Builder())->from('events')
            ->final()
            ->sample(0.1)
            ->filter([Query::equal('type', ['click'])])
            ->toRawSql();
        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.1', $sql);
        $this->assertStringContainsString("'click'", $sql);
    }

    public function testToRawSqlWithPrewhereEdge(): void
    {
        $sql = (new Builder())->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->toRawSql();
        $this->assertStringContainsString('PREWHERE', $sql);
        $this->assertStringContainsString("'click'", $sql);
        $this->assertStringContainsString('5', $sql);
    }

    public function testToRawSqlWithUnionEdge(): void
    {
        $sub = (new Builder())->from('b')->filter([Query::equal('x', [1])]);
        $sql = (new Builder())->from('a')->final()
            ->filter([Query::equal('y', [2])])
            ->union($sub)
            ->toRawSql();
        $this->assertStringContainsString('FINAL', $sql);
        $this->assertStringContainsString('UNION', $sql);
    }

    public function testToRawSqlWithBoolFalse(): void
    {
        $sql = (new Builder())->from('t')->filter([Query::equal('active', [false])])->toRawSql();
        $this->assertStringContainsString('0', $sql);
    }

    public function testToRawSqlWithNull(): void
    {
        $sql = (new Builder())->from('t')->filter([Query::raw('col = ?', [null])])->toRawSql();
        $this->assertStringContainsString('NULL', $sql);
    }

    public function testToRawSqlMixedTypes(): void
    {
        $sql = (new Builder())->from('t')
            ->filter([
                Query::equal('name', ['str']),
                Query::greaterThan('age', 42),
                Query::lessThan('score', 9.99),
            ])
            ->toRawSql();
        $this->assertStringContainsString("'str'", $sql);
        $this->assertStringContainsString('42', $sql);
        $this->assertStringContainsString('9.99', $sql);
    }
    // 12. Having with Multiple Sub-Queries

    public function testHavingMultipleSubQueries(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([
                Query::greaterThan('total', 5),
                Query::lessThan('total', 100),
            ])
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('HAVING `total` > ? AND `total` < ?', $result->query);
        $this->assertContains(5, $result->bindings);
        $this->assertContains(100, $result->bindings);
    }

    public function testHavingWithOrLogic(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([Query::or([
                Query::greaterThan('total', 100),
                Query::lessThan('total', 5),
            ])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('HAVING (`total` > ? OR `total` < ?)', $result->query);
    }
    // 13. Reset Property-by-Property Verification

    public function testResetClearsClickHouseProperties(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->limit(10);

        $builder->reset()->from('other');
        $result = $builder->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `other`', $result->query);
        $this->assertEquals([], $result->bindings);
        $this->assertStringNotContainsString('FINAL', $result->query);
        $this->assertStringNotContainsString('SAMPLE', $result->query);
        $this->assertStringNotContainsString('PREWHERE', $result->query);
    }

    public function testResetFollowedByUnion(): void
    {
        $builder = (new Builder())->from('a')
            ->final()
            ->union((new Builder())->from('old'));
        $builder->reset()->from('b');
        $result = $builder->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `b`', $result->query);
        $this->assertStringNotContainsString('UNION', $result->query);
        $this->assertStringNotContainsString('FINAL', $result->query);
    }

    public function testConditionProviderPersistsAfterReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->final()
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            });
        $builder->build();
        $builder->reset()->from('other');
        $result = $builder->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `other`', $result->query);
        $this->assertStringNotContainsString('FINAL', $result->query);
        $this->assertStringContainsString('_tenant = ?', $result->query);
    }
    // 14. Exact Full SQL Assertions

    public function testFinalSamplePrewhereFilterExactSql(): void
    {
        $result = (new Builder())->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('event_type', ['purchase'])])
            ->filter([Query::greaterThan('amount', 100)])
            ->sortDesc('amount')
            ->limit(50)
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `amount` > ? ORDER BY `amount` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['purchase', 100, 50], $result->bindings);
    }

    public function testKitchenSinkExactSql(): void
    {
        $sub = (new Builder())->from('archive')->final()->filter([Query::equal('status', ['closed'])]);
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->distinct()
            ->count('*', 'total')
            ->select(['event_type'])
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('event_type', ['purchase'])])
            ->filter([Query::greaterThan('amount', 100)])
            ->groupBy(['event_type'])
            ->having([Query::greaterThan('total', 5)])
            ->sortDesc('total')
            ->limit(50)
            ->offset(10)
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals(
            '(SELECT DISTINCT COUNT(*) AS `total`, `event_type` FROM `events` FINAL SAMPLE 0.1 JOIN `users` ON `events`.`uid` = `users`.`id` PREWHERE `event_type` IN (?) WHERE `amount` > ? GROUP BY `event_type` HAVING `total` > ? ORDER BY `total` DESC LIMIT ? OFFSET ?) UNION (SELECT * FROM `archive` FINAL WHERE `status` IN (?))',
            $result->query
        );
        $this->assertEquals(['purchase', 100, 5, 50, 10, 'closed'], $result->bindings);
    }
    // 15. Query::compile() Integration Tests

    public function testQueryCompileFilterViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::greaterThan('age', 18)->compile($builder);
        $this->assertEquals('`age` > ?', $sql);
    }

    public function testQueryCompileRegexViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::regex('path', '^/api')->compile($builder);
        $this->assertEquals('match(`path`, ?)', $sql);
    }

    public function testQueryCompileOrderRandomViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::orderRandom()->compile($builder);
        $this->assertEquals('rand()', $sql);
    }

    public function testQueryCompileLimitViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::limit(10)->compile($builder);
        $this->assertEquals('LIMIT ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testQueryCompileSelectViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::select(['a', 'b'])->compile($builder);
        $this->assertEquals('`a`, `b`', $sql);
    }

    public function testQueryCompileJoinViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::join('orders', 'u.id', 'o.uid')->compile($builder);
        $this->assertEquals('JOIN `orders` ON `u`.`id` = `o`.`uid`', $sql);
    }

    public function testQueryCompileGroupByViaClickHouse(): void
    {
        $builder = new Builder();
        $sql = Query::groupBy(['status'])->compile($builder);
        $this->assertEquals('`status`', $sql);
    }
    // 16. Binding Type Assertions with assertSame

    public function testBindingTypesPreservedInt(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('age', 18)])->build();
        $this->assertBindingCount($result);
        $this->assertSame([18], $result->bindings);
    }

    public function testBindingTypesPreservedFloat(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('score', 9.5)])->build();
        $this->assertBindingCount($result);
        $this->assertSame([9.5], $result->bindings);
    }

    public function testBindingTypesPreservedBool(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('active', [true])])->build();
        $this->assertBindingCount($result);
        $this->assertSame([true], $result->bindings);
    }

    public function testBindingTypesPreservedNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('val', [null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `val` IS NULL', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', ['a', null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` IN (?) OR `col` IS NULL)', $result->query);
        $this->assertSame(['a'], $result->bindings);
    }

    public function testNotEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', [null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NOT NULL', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testNotEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', 'b', null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` NOT IN (?, ?) AND `col` IS NOT NULL)', $result->query);
        $this->assertSame(['a', 'b'], $result->bindings);
    }

    public function testBindingTypesPreservedString(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('name', ['hello'])])->build();
        $this->assertBindingCount($result);
        $this->assertSame(['hello'], $result->bindings);
    }
    // 17. Raw Inside Logical Groups

    public function testRawInsideLogicalAnd(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::and([
                Query::greaterThan('x', 1),
                Query::raw('custom_func(y) > ?', [5]),
            ])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`x` > ? AND custom_func(y) > ?)', $result->query);
        $this->assertEquals([1, 5], $result->bindings);
    }

    public function testRawInsideLogicalOr(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::or([
                Query::equal('a', [1]),
                Query::raw('b IS NOT NULL', []),
            ])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR b IS NOT NULL)', $result->query);
        $this->assertEquals([1], $result->bindings);
    }
    // 18. Negative/Zero Limit and Offset

    public function testNegativeLimit(): void
    {
        $result = (new Builder())->from('t')->limit(-1)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([-1], $result->bindings);
    }

    public function testNegativeOffset(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(-5)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testLimitZero(): void
    {
        $result = (new Builder())->from('t')->limit(0)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([0], $result->bindings);
    }
    // 19. Multiple Limits/Offsets/Cursors First Wins

    public function testMultipleLimitsFirstWins(): void
    {
        $result = (new Builder())->from('t')->limit(10)->limit(20)->build();
        $this->assertBindingCount($result);
        $this->assertEquals([10], $result->bindings);
    }

    public function testMultipleOffsetsFirstWins(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(5)->offset(50)->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testCursorAfterAndBeforeFirstWins(): void
    {
        $result = (new Builder())->from('t')->cursorAfter('a')->cursorBefore('b')->sortAsc('_cursor')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
    }
    // 20. Distinct + Union

    public function testDistinctWithUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())->from('a')->distinct()->union($other)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('(SELECT DISTINCT * FROM `a`) UNION (SELECT * FROM `b`)', $result->query);
    }
    // DML: INSERT (same as standard SQL)

    public function testInsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('events')
            ->set(['name' => 'click', 'timestamp' => '2024-01-01'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `events` (`name`, `timestamp`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['click', '2024-01-01'], $result->bindings);
    }

    public function testInsertBatch(): void
    {
        $result = (new Builder())
            ->into('events')
            ->set(['name' => 'click', 'ts' => '2024-01-01'])
            ->set(['name' => 'view', 'ts' => '2024-01-02'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `events` (`name`, `ts`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['click', '2024-01-01', 'view', '2024-01-02'], $result->bindings);
    }
    // ClickHouse does not implement Upsert

    public function testDoesNotImplementUpsert(): void
    {
        $interfaces = \class_implements(Builder::class);
        $this->assertIsArray($interfaces);
        $this->assertArrayNotHasKey(Upsert::class, $interfaces);
    }
    // DML: UPDATE uses ALTER TABLE ... UPDATE

    public function testUpdateUsesAlterTable(): void
    {
        $result = (new Builder())
            ->from('events')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('status', ['old'])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `events` UPDATE `status` = ? WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['archived', 'old'], $result->bindings);
    }

    public function testUpdateWithFilterHook(): void
    {
        $hook = new class () implements Filter, Hook {
            public function filter(string $table): Condition
            {
                return new Condition('`_tenant` = ?', ['tenant_123']);
            }
        };

        $result = (new Builder())
            ->from('events')
            ->set(['status' => 'active'])
            ->filter([Query::equal('id', [1])])
            ->addHook($hook)
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `events` UPDATE `status` = ? WHERE `id` IN (?) AND `_tenant` = ?',
            $result->query
        );
        $this->assertEquals(['active', 1, 'tenant_123'], $result->bindings);
    }

    public function testUpdateWithoutWhereThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('ClickHouse UPDATE requires a WHERE clause');

        (new Builder())
            ->from('events')
            ->set(['status' => 'active'])
            ->update();
    }
    // DML: DELETE uses ALTER TABLE ... DELETE

    public function testDeleteUsesAlterTable(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([Query::lessThan('timestamp', '2024-01-01')])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `events` DELETE WHERE `timestamp` < ?',
            $result->query
        );
        $this->assertEquals(['2024-01-01'], $result->bindings);
    }

    public function testDeleteWithFilterHook(): void
    {
        $hook = new class () implements Filter, Hook {
            public function filter(string $table): Condition
            {
                return new Condition('`_tenant` = ?', ['tenant_123']);
            }
        };

        $result = (new Builder())
            ->from('events')
            ->filter([Query::equal('status', ['deleted'])])
            ->addHook($hook)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `events` DELETE WHERE `status` IN (?) AND `_tenant` = ?',
            $result->query
        );
        $this->assertEquals(['deleted', 'tenant_123'], $result->bindings);
    }

    public function testDeleteWithoutWhereThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('ClickHouse DELETE requires a WHERE clause');

        (new Builder())
            ->from('events')
            ->delete();
    }
    //  INTERSECT / EXCEPT (supported in ClickHouse)

    public function testIntersect(): void
    {
        $other = (new Builder())->from('admins');
        $result = (new Builder())
            ->from('users')
            ->intersect($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) INTERSECT (SELECT * FROM `admins`)',
            $result->query
        );
    }

    public function testExcept(): void
    {
        $other = (new Builder())->from('banned');
        $result = (new Builder())
            ->from('users')
            ->except($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) EXCEPT (SELECT * FROM `banned`)',
            $result->query
        );
    }
    //  Feature interfaces (not implemented)

    public function testDoesNotImplementLocking(): void
    {
        $interfaces = \class_implements(Builder::class);
        $this->assertIsArray($interfaces);
        $this->assertArrayNotHasKey(Locking::class, $interfaces);
    }

    public function testDoesNotImplementTransactions(): void
    {
        $interfaces = \class_implements(Builder::class);
        $this->assertIsArray($interfaces);
        $this->assertArrayNotHasKey(Transactions::class, $interfaces);
    }
    //  INSERT...SELECT (supported in ClickHouse)

    public function testInsertSelect(): void
    {
        $source = (new Builder())
            ->from('events')
            ->select(['name', 'timestamp'])
            ->filter([Query::equal('type', ['click'])]);

        $result = (new Builder())
            ->into('archived_events')
            ->fromSelect(['name', 'timestamp'], $source)
            ->insertSelect();

        $this->assertEquals(
            'INSERT INTO `archived_events` (`name`, `timestamp`) SELECT `name`, `timestamp` FROM `events` WHERE `type` IN (?)',
            $result->query
        );
        $this->assertEquals(['click'], $result->bindings);
    }
    //  CTEs (supported in ClickHouse)

    public function testCteWith(): void
    {
        $cte = (new Builder())
            ->from('events')
            ->filter([Query::equal('type', ['click'])]);

        $result = (new Builder())
            ->with('clicks', $cte)
            ->from('clicks')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'WITH `clicks` AS (SELECT * FROM `events` WHERE `type` IN (?)) SELECT * FROM `clicks`',
            $result->query
        );
        $this->assertEquals(['click'], $result->bindings);
    }
    //  setRaw with bindings (ClickHouse)

    public function testSetRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setRaw('count', 'count + ?', [1])
            ->filter([Query::equal('id', [42])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `events` UPDATE `count` = count + ? WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals([1, 42], $result->bindings);
    }
    //  Hints feature interface

    public function testImplementsHints(): void
    {
        $this->assertInstanceOf(Hints::class, new Builder());
    }

    public function testHintAppendsSettings(): void
    {
        $result = (new Builder())
            ->from('events')
            ->hint('max_threads=4')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4', $result->query);
    }

    public function testMultipleHints(): void
    {
        $result = (new Builder())
            ->from('events')
            ->hint('max_threads=4')
            ->hint('max_memory_usage=1000000000')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4, max_memory_usage=1000000000', $result->query);
    }

    public function testSettingsMethod(): void
    {
        $result = (new Builder())
            ->from('events')
            ->settings(['max_threads' => '4', 'max_memory_usage' => '1000000000'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4, max_memory_usage=1000000000', $result->query);
    }
    //  Window functions

    public function testImplementsWindows(): void
    {
        $this->assertInstanceOf(Windows::class, new Builder());
    }

    public function testSelectWindowRowNumber(): void
    {
        $result = (new Builder())
            ->from('events')
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['timestamp'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `timestamp` ASC) AS `rn`', $result->query);
    }
    //  Does NOT implement Spatial/VectorSearch/Json

    public function testDoesNotImplementSpatial(): void
    {
        $builder = new Builder();
        $this->assertNotInstanceOf(Spatial::class, $builder); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementVectorSearch(): void
    {
        $builder = new Builder();
        $this->assertNotInstanceOf(VectorSearch::class, $builder); // @phpstan-ignore method.alreadyNarrowedType
    }

    public function testDoesNotImplementJson(): void
    {
        $builder = new Builder();
        $this->assertNotInstanceOf(Json::class, $builder); // @phpstan-ignore method.alreadyNarrowedType
    }
    //  Reset clears hints

    public function testResetClearsHints(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->hint('max_threads=4');

        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('SETTINGS', $result->query);
    }

    public function testPrewhereWithSingleFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->prewhere([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `status` IN (?)', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testPrewhereWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->prewhere([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE `status` IN (?) AND `age` > ?', $result->query);
        $this->assertEquals(['active', 18], $result->bindings);
    }

    public function testPrewhereBeforeWhere(): void
    {
        $result = (new Builder())
            ->from('t')
            ->prewhere([Query::equal('status', ['active'])])
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $prewherePos = strpos($result->query, 'PREWHERE');
        $wherePos = strpos($result->query, 'WHERE');

        $this->assertNotFalse($prewherePos);
        $this->assertNotFalse($wherePos);
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testPrewhereBindingOrderBeforeWhere(): void
    {
        $result = (new Builder())
            ->from('t')
            ->prewhere([Query::equal('status', ['active'])])
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 18], $result->bindings);
    }

    public function testPrewhereWithJoin(): void
    {
        $result = (new Builder())
            ->from('t')
            ->join('u', 't.uid', 'u.id')
            ->prewhere([Query::equal('status', ['active'])])
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $joinPos = strpos($result->query, 'JOIN');
        $prewherePos = strpos($result->query, 'PREWHERE');
        $wherePos = strpos($result->query, 'WHERE');

        $this->assertNotFalse($joinPos);
        $this->assertNotFalse($prewherePos);
        $this->assertNotFalse($wherePos);
        $this->assertLessThan($prewherePos, $joinPos);
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function testFinalKeywordInFromClause(): void
    {
        $result = (new Builder())
            ->from('t')
            ->final()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `t` FINAL', $result->query);
    }

    public function testFinalAppearsBeforeWhere(): void
    {
        $result = (new Builder())
            ->from('t')
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $finalPos = strpos($result->query, 'FINAL');
        $wherePos = strpos($result->query, 'WHERE');

        $this->assertNotFalse($finalPos);
        $this->assertNotFalse($wherePos);
        $this->assertLessThan($wherePos, $finalPos);
    }

    public function testFinalWithSample(): void
    {
        $result = (new Builder())
            ->from('t')
            ->final()
            ->sample(0.5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `t` FINAL SAMPLE 0.5', $result->query);
    }

    public function testSampleFraction(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sample(0.1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `t` SAMPLE 0.1', $result->query);
    }

    public function testSampleZeroThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('t')
            ->sample(0.0);
    }

    public function testSampleOneThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('t')
            ->sample(1.0);
    }

    public function testSampleNegativeThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('t')
            ->sample(-0.5);
    }

    public function testUpdateAlterTableSyntax(): void
    {
        $result = (new Builder())
            ->from('t')
            ->set(['name' => 'Bob'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `t` UPDATE `name` = ? WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals(['Bob', 1], $result->bindings);
    }

    public function testUpdateWithoutWhereClauseThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('WHERE');

        (new Builder())
            ->from('t')
            ->set(['name' => 'Bob'])
            ->update();
    }

    public function testUpdateWithoutAssignmentsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::equal('id', [1])])
            ->update();
    }

    public function testUpdateWithRawSet(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setRaw('counter', '`counter` + 1')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`counter` = `counter` + 1', $result->query);
        $this->assertStringContainsString('ALTER TABLE `t` UPDATE', $result->query);
    }

    public function testUpdateWithRawSetBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setRaw('name', 'CONCAT(?, ?)', ['hello', ' world'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name` = CONCAT(?, ?)', $result->query);
        $this->assertEquals(['hello', ' world', 1], $result->bindings);
    }

    public function testDeleteAlterTableSyntax(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('id', [1])])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'ALTER TABLE `t` DELETE WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testDeleteWithoutWhereClauseThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('t')
            ->delete();
    }

    public function testDeleteWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('status', ['old']),
                Query::lessThan('age', 5),
            ])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE `status` IN (?) AND `age` < ?', $result->query);
        $this->assertEquals(['old', 5], $result->bindings);
    }

    public function testStartsWithUsesStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', 'foo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('startsWith(`name`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testNotStartsWithUsesNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notStartsWith('name', 'foo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT startsWith(`name`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testEndsWithUsesEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('name', 'foo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('endsWith(`name`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testNotEndsWithUsesNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEndsWith('name', 'foo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT endsWith(`name`, ?)', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testContainsSingleValueUsesPosition(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('name', ['foo'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('position(`name`, ?) > 0', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testContainsMultipleValuesUsesOrPosition(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('name', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(position(`name`, ?) > 0 OR position(`name`, ?) > 0)', $result->query);
        $this->assertEquals(['foo', 'bar'], $result->bindings);
    }

    public function testContainsAllUsesAndPosition(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('name', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(position(`name`, ?) > 0 AND position(`name`, ?) > 0)', $result->query);
        $this->assertEquals(['foo', 'bar'], $result->bindings);
    }

    public function testNotContainsSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('name', ['foo'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('position(`name`, ?) = 0', $result->query);
        $this->assertEquals(['foo'], $result->bindings);
    }

    public function testNotContainsMultipleValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('name', ['a', 'b'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(position(`name`, ?) = 0 AND position(`name`, ?) = 0)', $result->query);
        $this->assertEquals(['a', 'b'], $result->bindings);
    }

    public function testRegexUsesMatch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', '^test')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('match(`name`, ?)', $result->query);
        $this->assertEquals(['^test'], $result->bindings);
    }

    public function testSearchThrowsUnsupported(): void
    {
        $this->expectException(UnsupportedException::class);

        (new Builder())
            ->from('t')
            ->filter([Query::search('body', 'hello')])
            ->build();
    }

    public function testSettingsKeyValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->settings(['max_threads' => '4', 'enable_optimize_predicate_expression' => '1'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4, enable_optimize_predicate_expression=1', $result->query);
    }

    public function testHintAndSettingsCombined(): void
    {
        $result = (new Builder())
            ->from('t')
            ->hint('max_threads=2')
            ->settings(['enable_optimize_predicate_expression' => '1'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=2, enable_optimize_predicate_expression=1', $result->query);
    }

    public function testHintsPreserveBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->hint('max_threads=4')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active'], $result->bindings);
        $this->assertStringContainsString('SETTINGS max_threads=4', $result->query);
    }

    public function testHintsWithJoin(): void
    {
        $result = (new Builder())
            ->from('t')
            ->join('u', 't.uid', 'u.id')
            ->hint('max_threads=4')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4', $result->query);
        // SETTINGS must be at the very end
        $this->assertStringEndsWith('SETTINGS max_threads=4', $result->query);
    }

    public function testCTE(): void
    {
        $sub = (new Builder())
            ->from('events')
            ->filter([Query::equal('type', ['click'])]);

        $result = (new Builder())
            ->with('sub', $sub)
            ->from('sub')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'WITH `sub` AS (SELECT * FROM `events` WHERE `type` IN (?)) SELECT * FROM `sub`',
            $result->query
        );
        $this->assertEquals(['click'], $result->bindings);
    }

    public function testCTERecursive(): void
    {
        $sub = (new Builder())
            ->from('categories')
            ->filter([Query::equal('parent_id', [0])]);

        $result = (new Builder())
            ->withRecursive('tree', $sub)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH RECURSIVE `tree` AS', $result->query);
    }

    public function testCTEBindingOrder(): void
    {
        $sub = (new Builder())
            ->from('events')
            ->filter([Query::equal('type', ['click'])]);

        $result = (new Builder())
            ->with('sub', $sub)
            ->from('sub')
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        // CTE bindings come before main query bindings
        $this->assertEquals(['click', 5], $result->bindings);
    }

    public function testWindowFunctionPartitionAndOrder(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['created_at'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `created_at` ASC) AS `rn`', $result->query);
    }

    public function testWindowFunctionOrderDescending(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['-created_at'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `created_at` DESC) AS `rn`', $result->query);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('ROW_NUMBER()', 'rn', ['user_id'], ['created_at'])
            ->selectWindow('SUM(`amount`)', 'total', ['user_id'], null)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER', $result->query);
        $this->assertStringContainsString('SUM(`amount`) OVER', $result->query);
    }

    public function testSelectCaseExpression(): void
    {
        $case = (new CaseBuilder())
            ->when('`status` = ?', '?', ['active'], ['Active'])
            ->elseResult('?', ['Unknown'])
            ->alias('label')
            ->build();

        $result = (new Builder())
            ->from('t')
            ->selectCase($case)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN `status` = ? THEN ? ELSE ? END AS label', $result->query);
        $this->assertEquals(['active', 'Active', 'Unknown'], $result->bindings);
    }

    public function testSetCaseInUpdate(): void
    {
        $case = (new CaseBuilder())
            ->when('`role` = ?', '?', ['admin'], ['Admin'])
            ->elseResult('?', ['User'])
            ->build();

        $result = (new Builder())
            ->from('t')
            ->setRaw('label', $case->sql, $case->bindings)
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ALTER TABLE `t` UPDATE', $result->query);
        $this->assertStringContainsString('CASE WHEN `role` = ? THEN ? ELSE ? END', $result->query);
        $this->assertEquals(['admin', 'Admin', 'User', 1], $result->bindings);
    }

    public function testUnionSimple(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())
            ->from('a')
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION', $result->query);
        $this->assertStringNotContainsString('UNION ALL', $result->query);
    }

    public function testUnionAll(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())
            ->from('a')
            ->unionAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION ALL', $result->query);
    }

    public function testUnionBindingsOrder(): void
    {
        $other = (new Builder())->from('b')->filter([Query::equal('y', [2])]);
        $result = (new Builder())
            ->from('a')
            ->filter([Query::equal('x', [1])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([1, 2], $result->bindings);
    }

    public function testPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(2, 25)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals([25, 25], $result->bindings);
    }

    public function testCursorAfter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertEquals(['abc'], $result->bindings);
    }

    public function testBuildWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->build();
    }

    public function testInsertWithoutRowsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('t')
            ->insert();
    }

    public function testBatchInsertMismatchedColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('t')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'email' => 'bob@example.com'])
            ->insert();
    }

    public function testBatchInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('t')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'age' => 25])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `t` (`name`, `age`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 30, 'Bob', 25], $result->bindings);
    }

    public function testJoinFilterForcedToWhere(): void
    {
        $hook = new class () implements JoinFilter {
            public function filterJoin(string $table, JoinType $joinType): JoinCondition
            {
                return new JoinCondition(
                    new Condition('`active` = ?', [1]),
                    Placement::On,
                );
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook)
            ->leftJoin('u', 't.uid', 'u.id')
            ->build();
        $this->assertBindingCount($result);

        // ClickHouse forces all join filter conditions to WHERE placement
        $this->assertStringContainsString('WHERE `active` = ?', $result->query);
        $this->assertStringNotContainsString('ON `t`.`uid` = `u`.`id` AND', $result->query);
    }

    public function testToRawSqlClickHouseSyntax(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->toRawSql();

        $this->assertStringContainsString('FROM `t` FINAL', $sql);
        $this->assertStringContainsString("'active'", $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testResetClearsPrewhere(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->prewhere([Query::equal('status', ['active'])]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('PREWHERE', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testResetClearsSampleAndFinal(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->final()
            ->sample(0.5);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('FINAL', $result->query);
        $this->assertStringNotContainsString('SAMPLE', $result->query);
    }

    public function testEqualEmptyArrayReturnsFalse(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 0', $result->query);
    }

    public function testEqualWithNullOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` IS NULL', $result->query);
    }

    public function testEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [1, null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`x` IN (?) OR `x` IS NULL)', $result->query);
        $this->assertContains(1, $result->bindings);
    }

    public function testNotEqualSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', 42)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` != ?', $result->query);
        $this->assertContains(42, $result->bindings);
    }

    public function testAndFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::and([Query::greaterThan('age', 18), Query::lessThan('age', 65)])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`age` > ? AND `age` < ?)', $result->query);
    }

    public function testOrFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::or([Query::equal('role', ['admin']), Query::equal('role', ['editor'])])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`role` IN (?) OR `role` IN (?))', $result->query);
    }

    public function testNestedAndInsideOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::or([
                Query::and([Query::greaterThan('age', 18), Query::lessThan('age', 30)]),
                Query::and([Query::greaterThan('score', 80), Query::lessThan('score', 100)]),
            ])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('((`age` > ? AND `age` < ?) OR (`score` > ? AND `score` < ?))', $result->query);
        $this->assertEquals([18, 30, 80, 100], $result->bindings);
    }

    public function testBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testNotBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notBetween('score', 0, 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`score` NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([0, 50], $result->bindings);
    }

    public function testExistsMultipleAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name', 'email'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`name` IS NOT NULL AND `email` IS NOT NULL)', $result->query);
    }

    public function testNotExistsSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['name'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`name` IS NULL)', $result->query);
    }

    public function testRawFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('score > ?', [10])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('score > ?', $result->query);
        $this->assertContains(10, $result->bindings);
    }

    public function testRawFilterEmpty(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 1', $result->query);
    }

    public function testDottedIdentifier(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select(['events.name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`events`.`name`', $result->query);
    }

    public function testMultipleOrderBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY `name` ASC, `age` DESC', $result->query);
    }

    public function testDistinctWithSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT `name`', $result->query);
    }

    public function testSumWithAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(`amount`) AS `total`', $result->query);
    }

    public function testMultipleAggregates(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('SUM(`amount`) AS `total`', $result->query);
    }

    public function testIsNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted_at')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`deleted_at` IS NULL', $result->query);
    }

    public function testIsNotNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNotNull('name')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name` IS NOT NULL', $result->query);
    }

    public function testLessThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` < ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThanEqual('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` <= ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testGreaterThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`score` > ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`score` >= ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testRightJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->rightJoin('b', 'a.id', 'b.a_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN `b` ON `a`.`id` = `b`.`a_id`', $result->query);
    }

    public function testCrossJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `b`', $result->query);
        $this->assertStringNotContainsString(' ON ', $result->query);
    }

    public function testPrewhereAndFilterBindingOrderVerification(): void
    {
        $result = (new Builder())
            ->from('t')
            ->prewhere([Query::equal('status', ['active'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 5], $result->bindings);
    }

    public function testUpdateRawSetAndFilterBindingOrder(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setRaw('count', 'count + ?', [1])
            ->filter([Query::equal('status', ['active'])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals([1, 'active'], $result->bindings);
    }

    public function testSortRandomUsesRand(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY rand()', $result->query);
    }

    // Feature 1: Table Aliases (ClickHouse - alias AFTER FINAL/SAMPLE)

    public function testTableAliasClickHouse(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` AS `e`', $result->query);
    }

    public function testTableAliasWithFinal(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->final()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL AS `e`', $result->query);
    }

    public function testTableAliasWithSample(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->sample(0.1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` SAMPLE 0.1 AS `e`', $result->query);
    }

    public function testTableAliasWithFinalAndSample(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->final()
            ->sample(0.5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.5 AS `e`', $result->query);
    }

    // Feature 2: Subqueries (ClickHouse)

    public function testFromSubClickHouse(): void
    {
        $sub = (new Builder())->from('events')->select(['user_id'])->groupBy(['user_id']);
        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `user_id` FROM (SELECT `user_id` FROM `events` GROUP BY `user_id`) AS `sub`',
            $result->query
        );
    }

    public function testFilterWhereInClickHouse(): void
    {
        $sub = (new Builder())->from('orders')->select(['user_id']);
        $result = (new Builder())
            ->from('users')
            ->filterWhereIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`id` IN (SELECT `user_id` FROM `orders`)', $result->query);
    }

    // Feature 3: Raw ORDER BY / GROUP BY / HAVING (ClickHouse)

    public function testOrderByRawClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->orderByRaw('toDate(`created_at`) ASC')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY toDate(`created_at`) ASC', $result->query);
    }

    public function testGroupByRawClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'cnt')
            ->groupByRaw('toDate(`created_at`)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY toDate(`created_at`)', $result->query);
    }

    // Feature 4: countDistinct (ClickHouse)

    public function testCountDistinctClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->countDistinct('user_id', 'unique_users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(DISTINCT `user_id`) AS `unique_users` FROM `events`',
            $result->query
        );
    }

    // Feature 5: JoinBuilder (ClickHouse)

    public function testJoinWhereClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->joinWhere('users', function (JoinBuilder $join): void {
                $join->on('events.user_id', 'users.id');
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users` ON `events`.`user_id` = `users`.`id`', $result->query);
    }

    // Feature 6: EXISTS Subquery (ClickHouse)

    public function testFilterExistsClickHouse(): void
    {
        $sub = (new Builder())->from('orders')->select(['id'])->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);
        $result = (new Builder())
            ->from('users')
            ->filterExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT `id` FROM `orders`', $result->query);
    }

    // Feature 9: EXPLAIN (ClickHouse)

    public function testExplainClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
    }

    public function testExplainAnalyzeClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
    }

    // Feature: Cross Join Alias (ClickHouse)

    public function testCrossJoinAliasClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->crossJoin('dates', 'd')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `dates` AS `d`', $result->query);
    }

    // Subquery bindings (ClickHouse)

    public function testWhereInSubqueryClickHouse(): void
    {
        $sub = (new Builder())->from('active_users')->select(['id']);

        $result = (new Builder())
            ->from('events')
            ->filterWhereIn('user_id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`user_id` IN (SELECT `id` FROM `active_users`)', $result->query);
    }

    public function testWhereNotInSubqueryClickHouse(): void
    {
        $sub = (new Builder())->from('banned_users')->select(['id']);

        $result = (new Builder())
            ->from('events')
            ->filterWhereNotIn('user_id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`user_id` NOT IN (SELECT', $result->query);
    }

    public function testSelectSubClickHouse(): void
    {
        $sub = (new Builder())->from('events')->selectRaw('COUNT(*)');

        $result = (new Builder())
            ->from('users')
            ->selectSub($sub, 'event_count')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(SELECT COUNT(*) FROM `events`) AS `event_count`', $result->query);
    }

    public function testFromSubWithGroupByClickHouse(): void
    {
        $sub = (new Builder())->from('events')->select(['user_id'])->groupBy(['user_id']);

        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM (SELECT `user_id` FROM `events`', $result->query);
        $this->assertStringContainsString(') AS `sub`', $result->query);
    }

    // NOT EXISTS (ClickHouse)

    public function testFilterNotExistsClickHouse(): void
    {
        $sub = (new Builder())->from('banned')->select(['id']);

        $result = (new Builder())
            ->from('users')
            ->filterNotExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT EXISTS (SELECT', $result->query);
    }

    // HavingRaw (ClickHouse)

    public function testHavingRawClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->havingRaw('COUNT(*) > ?', [10])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING COUNT(*) > ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    // Table alias with FINAL and SAMPLE and alias combined

    public function testTableAliasWithFinalSampleAndAlias(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->final()
            ->sample(0.5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FINAL', $result->query);
        $this->assertStringContainsString('SAMPLE', $result->query);
        $this->assertStringContainsString('AS `e`', $result->query);
    }

    // JoinWhere LEFT JOIN (ClickHouse)

    public function testJoinWhereLeftJoinClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->joinWhere('users', function (JoinBuilder $join): void {
                $join->on('events.user_id', 'users.id')
                     ->where('users.active', '=', 1);
            }, JoinType::Left)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `users` ON', $result->query);
        $this->assertEquals([1], $result->bindings);
    }

    // JoinWhere with alias (ClickHouse)

    public function testJoinWhereWithAliasClickHouse(): void
    {
        $result = (new Builder())
            ->from('events', 'e')
            ->joinWhere('users', function (JoinBuilder $join): void {
                $join->on('e.user_id', 'u.id');
            }, JoinType::Inner, 'u')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users` AS `u`', $result->query);
    }

    // JoinWhere with multiple ON conditions (ClickHouse)

    public function testJoinWhereMultipleOnsClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->joinWhere('users', function (JoinBuilder $join): void {
                $join->on('events.user_id', 'users.id')
                     ->on('events.tenant_id', 'users.tenant_id');
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString(
            'ON `events`.`user_id` = `users`.`id` AND `events`.`tenant_id` = `users`.`tenant_id`',
            $result->query
        );
    }

    // EXPLAIN preserves bindings (ClickHouse)

    public function testExplainPreservesBindings(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([Query::equal('status', ['active'])])
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    // countDistinct without alias (ClickHouse)

    public function testCountDistinctWithoutAliasClickHouse(): void
    {
        $result = (new Builder())
            ->from('events')
            ->countDistinct('user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(DISTINCT `user_id`)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    // Multiple subqueries combined (ClickHouse)

    public function testMultipleSubqueriesCombined(): void
    {
        $sub1 = (new Builder())->from('active_users')->select(['id']);
        $sub2 = (new Builder())->from('banned_users')->select(['id']);

        $result = (new Builder())
            ->from('events')
            ->filterWhereIn('user_id', $sub1)
            ->filterWhereNotIn('user_id', $sub2)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('IN (SELECT', $result->query);
        $this->assertStringContainsString('NOT IN (SELECT', $result->query);
    }

    // PREWHERE with subquery (ClickHouse)

    public function testPrewhereWithSubquery(): void
    {
        $sub = (new Builder())->from('active_users')->select(['id']);

        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filterWhereIn('user_id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('PREWHERE', $result->query);
        $this->assertStringContainsString('IN (SELECT', $result->query);
    }

    // Settings with subquery (ClickHouse)

    public function testSettingsStillAppear(): void
    {
        $result = (new Builder())
            ->from('events')
            ->settings(['max_threads' => '4'])
            ->orderByRaw('`created_at` DESC')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SETTINGS max_threads=4', $result->query);
        $this->assertStringContainsString('ORDER BY `created_at` DESC', $result->query);
    }

    public function testExactSimpleSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(25)
            ->build();

        $this->assertSame(
            'SELECT `name`, `email` FROM `users` WHERE `status` IN (?) ORDER BY `name` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['active', 25], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSelectWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'total'])
            ->filter([
                Query::greaterThan('total', 100),
                Query::lessThanEqual('total', 5000),
                Query::equal('status', ['paid', 'shipped']),
                Query::isNotNull('shipped_at'),
            ])
            ->build();

        $this->assertSame(
            'SELECT `id`, `total` FROM `orders` WHERE `total` > ? AND `total` <= ? AND `status` IN (?, ?) AND `shipped_at` IS NOT NULL',
            $result->query
        );
        $this->assertEquals([100, 5000, 'paid', 'shipped'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactPrewhere(): void
    {
        $result = (new Builder())
            ->from('hits')
            ->select(['url', 'count'])
            ->prewhere([Query::equal('site_id', [42])])
            ->filter([Query::greaterThan('count', 10)])
            ->build();

        $this->assertSame(
            'SELECT `url`, `count` FROM `hits` PREWHERE `site_id` IN (?) WHERE `count` > ?',
            $result->query
        );
        $this->assertEquals([42, 10], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->select(['user_id', 'event_type'])
            ->build();

        $this->assertSame(
            'SELECT `user_id`, `event_type` FROM `events` FINAL',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSample(): void
    {
        $result = (new Builder())
            ->from('pageviews')
            ->sample(0.1)
            ->select(['url'])
            ->build();

        $this->assertSame(
            'SELECT `url` FROM `pageviews` SAMPLE 0.1',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactFinalSamplePrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->sortDesc('timestamp')
            ->limit(100)
            ->build();

        $this->assertSame(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `count` > ? ORDER BY `timestamp` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['click', 5, 100], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSettings(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->select(['message'])
            ->filter([Query::equal('level', ['error'])])
            ->settings(['max_threads' => '8'])
            ->build();

        $this->assertSame(
            'SELECT `message` FROM `logs` WHERE `level` IN (?) SETTINGS max_threads=8',
            $result->query
        );
        $this->assertEquals(['error'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'age' => 25])
            ->insert();

        $this->assertSame(
            'INSERT INTO `users` (`name`, `age`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 30, 'Bob', 25], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAlterTableUpdate(): void
    {
        $result = (new Builder())
            ->from('events')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('year', [2023])])
            ->update();

        $this->assertSame(
            'ALTER TABLE `events` UPDATE `status` = ? WHERE `year` IN (?)',
            $result->query
        );
        $this->assertEquals(['archived', 2023], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAlterTableDelete(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([Query::lessThan('created_at', '2023-01-01')])
            ->delete();

        $this->assertSame(
            'ALTER TABLE `events` DELETE WHERE `created_at` < ?',
            $result->query
        );
        $this->assertEquals(['2023-01-01'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactMultipleJoins(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['orders.id', 'users.name', 'products.title'])
            ->join('users', 'orders.user_id', 'users.id')
            ->leftJoin('products', 'orders.product_id', 'products.id')
            ->filter([Query::greaterThan('orders.total', 50)])
            ->build();

        $this->assertSame(
            'SELECT `orders`.`id`, `users`.`name`, `products`.`title` FROM `orders` JOIN `users` ON `orders`.`user_id` = `users`.`id` LEFT JOIN `products` ON `orders`.`product_id` = `products`.`id` WHERE `orders`.`total` > ?',
            $result->query
        );
        $this->assertEquals([50], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactCte(): void
    {
        $cteQuery = (new Builder())
            ->from('events')
            ->select(['user_id'])
            ->filter([Query::equal('event_type', ['purchase'])]);

        $result = (new Builder())
            ->with('buyers', $cteQuery)
            ->from('users')
            ->select(['name', 'email'])
            ->filterWhereIn('id', (new Builder())->from('buyers')->select(['user_id']))
            ->build();

        $this->assertSame(
            'WITH `buyers` AS (SELECT `user_id` FROM `events` WHERE `event_type` IN (?)) SELECT `name`, `email` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `buyers`)',
            $result->query
        );
        $this->assertEquals(['purchase'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactUnionAll(): void
    {
        $archive = (new Builder())
            ->from('events_2023')
            ->select(['id', 'name'])
            ->filter([Query::equal('status', ['active'])]);

        $result = (new Builder())
            ->from('events_2024')
            ->select(['id', 'name'])
            ->filter([Query::equal('status', ['active'])])
            ->unionAll($archive)
            ->build();

        $this->assertSame(
            '(SELECT `id`, `name` FROM `events_2024` WHERE `status` IN (?)) UNION ALL (SELECT `id`, `name` FROM `events_2023` WHERE `status` IN (?))',
            $result->query
        );
        $this->assertEquals(['active', 'active'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactWindowFunction(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->select(['employee_id', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['department_id'], ['-amount'])
            ->build();

        $this->assertSame(
            'SELECT `employee_id`, `amount`, ROW_NUMBER() OVER (PARTITION BY `department_id` ORDER BY `amount` DESC) AS `rn` FROM `sales`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAggregationGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'order_count')
            ->select(['customer_id'])
            ->groupBy(['customer_id'])
            ->having([Query::greaterThan('order_count', 5)])
            ->sortDesc('order_count')
            ->build();

        $this->assertSame(
            'SELECT COUNT(*) AS `order_count`, `customer_id` FROM `orders` GROUP BY `customer_id` HAVING `order_count` > ? ORDER BY `order_count` DESC',
            $result->query
        );
        $this->assertEquals([5], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSubqueryWhereIn(): void
    {
        $sub = (new Builder())
            ->from('blacklist')
            ->select(['user_id'])
            ->filter([Query::equal('active', [1])]);

        $result = (new Builder())
            ->from('events')
            ->select(['id', 'user_id', 'action'])
            ->filterWhereNotIn('user_id', $sub)
            ->build();

        $this->assertSame(
            'SELECT `id`, `user_id`, `action` FROM `events` WHERE `user_id` NOT IN (SELECT `user_id` FROM `blacklist` WHERE `active` IN (?))',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactExistsSubquery(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->selectRaw('1')
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterExists($sub)
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE EXISTS (SELECT 1 FROM `orders` WHERE `orders`.`user_id` = `users`.`id`)',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactFromSubquery(): void
    {
        $sub = (new Builder())
            ->from('events')
            ->select(['user_id'])
            ->count('*', 'cnt')
            ->groupBy(['user_id']);

        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id', 'cnt'])
            ->filter([Query::greaterThan('cnt', 10)])
            ->build();

        $this->assertSame(
            'SELECT `user_id`, `cnt` FROM (SELECT COUNT(*) AS `cnt`, `user_id` FROM `events` GROUP BY `user_id`) AS `sub` WHERE `cnt` > ?',
            $result->query
        );
        $this->assertEquals([10], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSelectSubquery(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->selectSub($sub, 'order_count')
            ->build();

        $this->assertSame(
            'SELECT `id`, `name`, (SELECT COUNT(*) AS `cnt` FROM `orders` WHERE `orders`.`user_id` = `users`.`id`) AS `order_count` FROM `users`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactNestedWhereGroups(): void
    {
        $result = (new Builder())
            ->from('products')
            ->select(['id', 'name', 'price'])
            ->filter([
                Query::and([
                    Query::or([
                        Query::equal('category', ['electronics']),
                        Query::equal('category', ['books']),
                    ]),
                    Query::greaterThan('price', 10),
                    Query::lessThan('price', 1000),
                ]),
            ])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name`, `price` FROM `products` WHERE ((`category` IN (?) OR `category` IN (?)) AND `price` > ? AND `price` < ?)',
            $result->query
        );
        $this->assertEquals(['electronics', 'books', 10, 1000], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactInsertSelect(): void
    {
        $source = (new Builder())
            ->from('events')
            ->select(['user_id', 'event_type'])
            ->filter([Query::equal('year', [2024])]);

        $result = (new Builder())
            ->into('events_archive')
            ->fromSelect(['user_id', 'event_type'], $source)
            ->insertSelect();

        $this->assertSame(
            'INSERT INTO `events_archive` (`user_id`, `event_type`) SELECT `user_id`, `event_type` FROM `events` WHERE `year` IN (?)',
            $result->query
        );
        $this->assertEquals([2024], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDistinctWithOffset(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->distinct()
            ->select(['source', 'level'])
            ->limit(20)
            ->offset(40)
            ->build();

        $this->assertSame(
            'SELECT DISTINCT `source`, `level` FROM `logs` LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals([20, 40], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactCaseInSelect(): void
    {
        $case = (new CaseBuilder())
            ->when('`status` = ?', '?', ['active'], ['Active'])
            ->when('`status` = ?', '?', ['inactive'], ['Inactive'])
            ->elseResult('?', ['Unknown'])
            ->alias('`status_label`')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->selectCase($case)
            ->build();

        $this->assertSame(
            'SELECT `id`, `name`, CASE WHEN `status` = ? THEN ? WHEN `status` = ? THEN ? ELSE ? END AS `status_label` FROM `users`',
            $result->query
        );
        $this->assertEquals(['active', 'Active', 'inactive', 'Inactive', 'Unknown'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactHintSettings(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->filter([Query::equal('type', ['click'])])
            ->settings([
                'max_threads' => '4',
                'max_memory_usage' => '10000000000',
            ])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` WHERE `type` IN (?) SETTINGS max_threads=4, max_memory_usage=10000000000',
            $result->query
        );
        $this->assertEquals(['click'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactPrewhereWithJoin(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.user_id', 'users.id')
            ->select(['events.id', 'users.name'])
            ->prewhere([Query::equal('events.event_type', ['purchase'])])
            ->filter([Query::greaterThan('users.age', 21)])
            ->sortDesc('events.created_at')
            ->limit(50)
            ->build();

        $this->assertSame(
            'SELECT `events`.`id`, `users`.`name` FROM `events` JOIN `users` ON `events`.`user_id` = `users`.`id` PREWHERE `events`.`event_type` IN (?) WHERE `users`.`age` > ? ORDER BY `events`.`created_at` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['purchase', 21, 50], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedWhenTrue(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedWhenFalse(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `users`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedExplain(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->filter([Query::equal('status', ['active'])])
            ->explain();

        $this->assertSame(
            'EXPLAIN SELECT `id`, `name` FROM `events` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedCursorAfterWithFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->filter([Query::greaterThan('age', 18)])
            ->cursorAfter('abc123')
            ->sortDesc('created_at')
            ->limit(25)
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` WHERE `age` > ? AND `_cursor` > ? ORDER BY `created_at` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals([18, 'abc123', 25], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedCursorBefore(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->cursorBefore('xyz789')
            ->sortAsc('id')
            ->limit(10)
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` WHERE `_cursor` < ? ORDER BY `id` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['xyz789', 10], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedMultipleCtes(): void
    {
        $cteA = (new Builder())
            ->from('orders')
            ->select(['customer_id'])
            ->filter([Query::greaterThan('total', 100)]);

        $cteB = (new Builder())
            ->from('customers')
            ->select(['id', 'name'])
            ->filter([Query::equal('tier', ['gold'])]);

        $result = (new Builder())
            ->with('a', $cteA)
            ->with('b', $cteB)
            ->from('a')
            ->select(['customer_id'])
            ->build();

        $this->assertSame(
            'WITH `a` AS (SELECT `customer_id` FROM `orders` WHERE `total` > ?), `b` AS (SELECT `id`, `name` FROM `customers` WHERE `tier` IN (?)) SELECT `customer_id` FROM `a`',
            $result->query
        );
        $this->assertEquals([100, 'gold'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->select(['employee_id', 'amount'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['department_id'], ['-amount'])
            ->selectWindow('SUM(`amount`)', 'running_total', ['department_id'], ['created_at'])
            ->build();

        $this->assertSame(
            'SELECT `employee_id`, `amount`, ROW_NUMBER() OVER (PARTITION BY `department_id` ORDER BY `amount` DESC) AS `rn`, SUM(`amount`) OVER (PARTITION BY `department_id` ORDER BY `created_at` ASC) AS `running_total` FROM `sales`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedUnionWithOrderAndLimit(): void
    {
        $archive = (new Builder())
            ->from('events_archive')
            ->select(['id', 'name']);

        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->sortAsc('id')
            ->limit(50)
            ->union($archive)
            ->build();

        $this->assertSame(
            '(SELECT `id`, `name` FROM `events` ORDER BY `id` ASC LIMIT ?) UNION (SELECT `id`, `name` FROM `events_archive`)',
            $result->query
        );
        $this->assertEquals([50], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedDeeplyNestedConditions(): void
    {
        $result = (new Builder())
            ->from('products')
            ->select(['id', 'name'])
            ->filter([
                Query::and([
                    Query::or([
                        Query::and([
                            Query::equal('brand', ['acme']),
                            Query::greaterThan('price', 50),
                        ]),
                        Query::and([
                            Query::equal('brand', ['globex']),
                            Query::lessThan('price', 20),
                        ]),
                    ]),
                    Query::equal('in_stock', [true]),
                ]),
            ])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `products` WHERE (((`brand` IN (?) AND `price` > ?) OR (`brand` IN (?) AND `price` < ?)) AND `in_stock` IN (?))',
            $result->query
        );
        $this->assertEquals(['acme', 50, 'globex', 20, true], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedStartsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([Query::startsWith('name', 'John')])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE startsWith(`name`, ?)',
            $result->query
        );
        $this->assertEquals(['John'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedEndsWith(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'email'])
            ->filter([Query::endsWith('email', '@example.com')])
            ->build();

        $this->assertSame(
            'SELECT `id`, `email` FROM `users` WHERE endsWith(`email`, ?)',
            $result->query
        );
        $this->assertEquals(['@example.com'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedContainsSingle(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->select(['id', 'title'])
            ->filter([Query::contains('title', ['php'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `title` FROM `articles` WHERE position(`title`, ?) > 0',
            $result->query
        );
        $this->assertEquals(['php'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->select(['id', 'title'])
            ->filter([Query::contains('title', ['php', 'laravel'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `title` FROM `articles` WHERE (position(`title`, ?) > 0 OR position(`title`, ?) > 0)',
            $result->query
        );
        $this->assertEquals(['php', 'laravel'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedContainsAll(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->select(['id', 'title'])
            ->filter([Query::containsAll('title', ['php', 'laravel'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `title` FROM `articles` WHERE (position(`title`, ?) > 0 AND position(`title`, ?) > 0)',
            $result->query
        );
        $this->assertEquals(['php', 'laravel'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedNotContainsSingle(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->select(['id', 'title'])
            ->filter([Query::notContains('title', ['spam'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `title` FROM `articles` WHERE position(`title`, ?) = 0',
            $result->query
        );
        $this->assertEquals(['spam'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedNotContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->select(['id', 'title'])
            ->filter([Query::notContains('title', ['spam', 'junk'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `title` FROM `articles` WHERE (position(`title`, ?) = 0 AND position(`title`, ?) = 0)',
            $result->query
        );
        $this->assertEquals(['spam', 'junk'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedRegex(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->select(['id', 'message'])
            ->filter([Query::regex('message', '^ERROR.*timeout$')])
            ->build();

        $this->assertSame(
            'SELECT `id`, `message` FROM `logs` WHERE match(`message`, ?)',
            $result->query
        );
        $this->assertEquals(['^ERROR.*timeout$'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedPrewhereMultipleConditions(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->prewhere([
                Query::equal('event_type', ['click']),
                Query::greaterThan('timestamp', 1000000),
            ])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` PREWHERE `event_type` IN (?) AND `timestamp` > ? WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['click', 1000000, 'active'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedFinalWithFiltersAndOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->sortDesc('created_at')
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` FINAL WHERE `status` IN (?) ORDER BY `created_at` DESC',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedSampleWithPrewhereAndWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->sample(0.1)
            ->prewhere([Query::equal('event_type', ['purchase'])])
            ->filter([Query::greaterThan('amount', 50)])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `amount` > ?',
            $result->query
        );
        $this->assertEquals(['purchase', 50], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedSettingsMultiple(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->settings([
                'max_threads' => '4',
                'max_memory_usage' => '10000000',
            ])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `events` SETTINGS max_threads=4, max_memory_usage=10000000',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedAlterTableUpdateWithSetRaw(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setRaw('views', '`views` + 1')
            ->filter([Query::equal('id', [42])])
            ->update();

        $this->assertSame(
            'ALTER TABLE `events` UPDATE `views` = `views` + 1 WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals([42], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedAlterTableDeleteWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->filter([
                Query::equal('status', ['deleted']),
                Query::lessThan('created_at', '2023-01-01'),
            ])
            ->delete();

        $this->assertSame(
            'ALTER TABLE `events` DELETE WHERE `status` IN (?) AND `created_at` < ?',
            $result->query
        );
        $this->assertEquals(['deleted', '2023-01-01'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedEmptyInClause(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([Query::equal('status', [])])
            ->build();

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE 1 = 0',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAdvancedResetClearsPrewhereAndFinal(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->select(['id', 'name'])
            ->prewhere([Query::equal('event_type', ['click'])])
            ->final()
            ->filter([Query::equal('status', ['active'])]);

        $builder->reset();

        $result = $builder
            ->from('users')
            ->select(['id', 'email'])
            ->build();

        $this->assertSame(
            'SELECT `id`, `email` FROM `users`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }
}
