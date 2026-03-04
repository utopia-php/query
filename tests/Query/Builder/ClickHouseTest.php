<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Exception;
use Utopia\Query\Query;

class ClickHouseTest extends TestCase
{
    // ── Compiler compliance ──

    public function testImplementsCompiler(): void
    {
        $builder = new Builder();
        $this->assertInstanceOf(Compiler::class, $builder);
    }

    // ── Basic queries work identically ──

    public function testBasicSelect(): void
    {
        $result = (new Builder())
            ->from('events')
            ->select(['name', 'timestamp'])
            ->build();

        $this->assertEquals('SELECT `name`, `timestamp` FROM `events`', $result['query']);
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

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `status` IN (?) AND `count` > ? ORDER BY `timestamp` DESC LIMIT ?',
            $result['query']
        );
        $this->assertEquals(['active', 10, 100], $result['bindings']);
    }

    // ── ClickHouse-specific: regex uses match() ──

    public function testRegexUsesMatchFunction(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', '^/api/v[0-9]+')])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`path`, ?)', $result['query']);
        $this->assertEquals(['^/api/v[0-9]+'], $result['bindings']);
    }

    // ── ClickHouse-specific: search throws exception ──

    public function testSearchThrowsException(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Full-text search (MATCH AGAINST) is not supported in ClickHouse');

        (new Builder())
            ->from('logs')
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    public function testNotSearchThrowsException(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Full-text search (MATCH AGAINST) is not supported in ClickHouse');

        (new Builder())
            ->from('logs')
            ->filter([Query::notSearch('content', 'hello')])
            ->build();
    }

    // ── ClickHouse-specific: random ordering uses rand() ──

    public function testRandomOrderUsesLowercaseRand(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand()', $result['query']);
    }

    // ── FINAL keyword ──

    public function testFinalKeyword(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL', $result['query']);
    }

    public function testFinalWithFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` FINAL WHERE `status` IN (?) LIMIT ?',
            $result['query']
        );
        $this->assertEquals(['active', 10], $result['bindings']);
    }

    // ── SAMPLE clause ──

    public function testSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1', $result['query']);
    }

    public function testSampleWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.5', $result['query']);
    }

    // ── PREWHERE clause ──

    public function testPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?)',
            $result['query']
        );
        $this->assertEquals(['click'], $result['bindings']);
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

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?) AND `timestamp` > ?',
            $result['query']
        );
        $this->assertEquals(['click', '2024-01-01'], $result['bindings']);
    }

    public function testPrewhereWithWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `event_type` IN (?) WHERE `count` > ?',
            $result['query']
        );
        $this->assertEquals(['click', 5], $result['bindings']);
    }

    public function testPrewhereWithJoinAndWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.user_id', 'users.id')
            ->prewhere([Query::equal('event_type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` JOIN `users` ON `events`.`user_id` = `users`.`id` PREWHERE `event_type` IN (?) WHERE `users`.`age` > ?',
            $result['query']
        );
        $this->assertEquals(['click', 18], $result['bindings']);
    }

    // ── Combined ClickHouse features ──

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

        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `count` > ? ORDER BY `timestamp` DESC LIMIT ?',
            $result['query']
        );
        $this->assertEquals(['click', 5, 100], $result['bindings']);
    }

    // ── Aggregations work ──

    public function testAggregation(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'total')
            ->sum('duration', 'total_duration')
            ->groupBy(['event_type'])
            ->having([Query::greaterThan('total', 10)])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, SUM(`duration`) AS `total_duration` FROM `events` GROUP BY `event_type` HAVING `total` > ?',
            $result['query']
        );
        $this->assertEquals([10], $result['bindings']);
    }

    // ── Joins work ──

    public function testJoin(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.user_id', 'users.id')
            ->leftJoin('sessions', 'events.session_id', 'sessions.id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` JOIN `users` ON `events`.`user_id` = `users`.`id` LEFT JOIN `sessions` ON `events`.`session_id` = `sessions`.`id`',
            $result['query']
        );
    }

    // ── Distinct ──

    public function testDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->distinct()
            ->select(['user_id'])
            ->build();

        $this->assertEquals('SELECT DISTINCT `user_id` FROM `events`', $result['query']);
    }

    // ── Union ──

    public function testUnion(): void
    {
        $other = (new Builder())->from('events_archive')->filter([Query::equal('year', [2023])]);

        $result = (new Builder())
            ->from('events')
            ->filter([Query::equal('year', [2024])])
            ->union($other)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `year` IN (?) UNION SELECT * FROM `events_archive` WHERE `year` IN (?)',
            $result['query']
        );
        $this->assertEquals([2024, 2023], $result['bindings']);
    }

    // ── toRawSql ──

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

    // ── Reset clears ClickHouse state ──

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

        $this->assertEquals('SELECT * FROM `logs`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ── Fluent chaining ──

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

    // ── Attribute resolver works ──

    public function testAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$id' => '_uid',
                default => $a,
            })
            ->filter([Query::equal('$id', ['abc'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `_uid` IN (?)',
            $result['query']
        );
    }

    // ── Condition provider works ──

    public function testConditionProvider(): void
    {
        $result = (new Builder())
            ->from('events')
            ->addConditionProvider(fn (string $table): array => [
                '_tenant = ?',
                ['t1'],
            ])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` WHERE `status` IN (?) AND _tenant = ?',
            $result['query']
        );
        $this->assertEquals(['active', 't1'], $result['bindings']);
    }

    // ── Prewhere binding order ──

    public function testPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->limit(10)
            ->build();

        // prewhere bindings come before where bindings
        $this->assertEquals(['click', 5, 10], $result['bindings']);
    }

    // ── Combined PREWHERE + WHERE + JOIN + GROUP BY ──

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

        $query = $result['query'];

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

    // ══════════════════════════════════════════════════════════════════
    // 1. PREWHERE comprehensive (40+ tests)
    // ══════════════════════════════════════════════════════════════════

    public function testPrewhereEmptyArray(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([])
            ->build();

        $this->assertEquals('SELECT * FROM `events`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testPrewhereSingleEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `status` IN (?)', $result['query']);
        $this->assertEquals(['active'], $result['bindings']);
    }

    public function testPrewhereSingleNotEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notEqual('status', 'deleted')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `status` != ?', $result['query']);
        $this->assertEquals(['deleted'], $result['bindings']);
    }

    public function testPrewhereLessThan(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::lessThan('age', 30)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` < ?', $result['query']);
        $this->assertEquals([30], $result['bindings']);
    }

    public function testPrewhereLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::lessThanEqual('age', 30)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` <= ?', $result['query']);
        $this->assertEquals([30], $result['bindings']);
    }

    public function testPrewhereGreaterThan(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThan('score', 50)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `score` > ?', $result['query']);
        $this->assertEquals([50], $result['bindings']);
    }

    public function testPrewhereGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThanEqual('score', 50)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `score` >= ?', $result['query']);
        $this->assertEquals([50], $result['bindings']);
    }

    public function testPrewhereBetween(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::between('age', 18, 65)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([18, 65], $result['bindings']);
    }

    public function testPrewhereNotBetween(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notBetween('age', 0, 17)])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `age` NOT BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([0, 17], $result['bindings']);
    }

    public function testPrewhereStartsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::startsWith('path', '/api')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `path` LIKE ?', $result['query']);
        $this->assertEquals(['/api%'], $result['bindings']);
    }

    public function testPrewhereNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notStartsWith('path', '/admin')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `path` NOT LIKE ?', $result['query']);
        $this->assertEquals(['/admin%'], $result['bindings']);
    }

    public function testPrewhereEndsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::endsWith('file', '.csv')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `file` LIKE ?', $result['query']);
        $this->assertEquals(['%.csv'], $result['bindings']);
    }

    public function testPrewhereNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notEndsWith('file', '.tmp')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `file` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%.tmp'], $result['bindings']);
    }

    public function testPrewhereContainsSingle(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::contains('name', ['foo'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `name` LIKE ?', $result['query']);
        $this->assertEquals(['%foo%'], $result['bindings']);
    }

    public function testPrewhereContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::contains('name', ['foo', 'bar'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`name` LIKE ? OR `name` LIKE ?)', $result['query']);
        $this->assertEquals(['%foo%', '%bar%'], $result['bindings']);
    }

    public function testPrewhereContainsAny(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::containsAny('tag', ['a', 'b', 'c'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `tag` IN (?, ?, ?)', $result['query']);
        $this->assertEquals(['a', 'b', 'c'], $result['bindings']);
    }

    public function testPrewhereContainsAll(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::containsAll('tag', ['x', 'y'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`tag` LIKE ? AND `tag` LIKE ?)', $result['query']);
        $this->assertEquals(['%x%', '%y%'], $result['bindings']);
    }

    public function testPrewhereNotContainsSingle(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notContains('name', ['bad'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `name` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%bad%'], $result['bindings']);
    }

    public function testPrewhereNotContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notContains('name', ['bad', 'ugly'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`name` NOT LIKE ? AND `name` NOT LIKE ?)', $result['query']);
        $this->assertEquals(['%bad%', '%ugly%'], $result['bindings']);
    }

    public function testPrewhereIsNull(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::isNull('deleted_at')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `deleted_at` IS NULL', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testPrewhereIsNotNull(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::isNotNull('email')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `email` IS NOT NULL', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testPrewhereExists(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::exists(['col_a', 'col_b'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`col_a` IS NOT NULL AND `col_b` IS NOT NULL)', $result['query']);
    }

    public function testPrewhereNotExists(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::notExists(['col_a'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`col_a` IS NULL)', $result['query']);
    }

    public function testPrewhereRegex(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::regex('path', '^/api')])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE match(`path`, ?)', $result['query']);
        $this->assertEquals(['^/api'], $result['bindings']);
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

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`a` IN (?) AND `b` IN (?))', $result['query']);
        $this->assertEquals([1, 2], $result['bindings']);
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

        $this->assertEquals('SELECT * FROM `events` PREWHERE (`a` IN (?) OR `b` IN (?))', $result['query']);
        $this->assertEquals([1, 2], $result['bindings']);
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

        $this->assertEquals('SELECT * FROM `events` PREWHERE ((`x` IN (?) OR `y` IN (?)) AND `z` > ?)', $result['query']);
        $this->assertEquals([1, 2, 0], $result['bindings']);
    }

    public function testPrewhereRawExpression(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::raw('toDate(created) > ?', ['2024-01-01'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE toDate(created) > ?', $result['query']);
        $this->assertEquals(['2024-01-01'], $result['bindings']);
    }

    public function testPrewhereMultipleCallsAdditive(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('a', [1])])
            ->prewhere([Query::equal('b', [2])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `a` IN (?) AND `b` IN (?)', $result['query']);
        $this->assertEquals([1, 2], $result['bindings']);
    }

    public function testPrewhereWithWhereFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` FINAL PREWHERE `type` IN (?) WHERE `count` > ?',
            $result['query']
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

        $this->assertEquals(
            'SELECT * FROM `events` SAMPLE 0.5 PREWHERE `type` IN (?) WHERE `count` > ?',
            $result['query']
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

        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.3 PREWHERE `type` IN (?) WHERE `count` > ?',
            $result['query']
        );
        $this->assertEquals(['click', 5], $result['bindings']);
    }

    public function testPrewhereWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'total')
            ->groupBy(['type'])
            ->build();

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
        $this->assertStringContainsString('GROUP BY `type`', $result['query']);
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

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
        $this->assertStringContainsString('HAVING `total` > ?', $result['query']);
    }

    public function testPrewhereWithOrderBy(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sortAsc('name')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) ORDER BY `name` ASC',
            $result['query']
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

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals(['click', 10, 20], $result['bindings']);
    }

    public function testPrewhereWithUnion(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
        $this->assertStringContainsString('UNION SELECT', $result['query']);
    }

    public function testPrewhereWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->distinct()
            ->select(['user_id'])
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertStringContainsString('SELECT DISTINCT', $result['query']);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
    }

    public function testPrewhereWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sum('amount', 'total_amount')
            ->build();

        $this->assertStringContainsString('SUM(`amount`) AS `total_amount`', $result['query']);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
    }

    public function testPrewhereBindingOrderWithProvider(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addConditionProvider(fn (string $table): array => ['tenant_id = ?', ['t1']])
            ->build();

        $this->assertEquals(['click', 5, 't1'], $result['bindings']);
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

        // prewhere, where filter, cursor
        $this->assertEquals('click', $result['bindings'][0]);
        $this->assertEquals(5, $result['bindings'][1]);
        $this->assertEquals('abc123', $result['bindings'][2]);
    }

    public function testPrewhereBindingOrderComplex(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addConditionProvider(fn (string $table): array => ['tenant = ?', ['t1']])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->count('*', 'total')
            ->groupBy(['type'])
            ->having([Query::greaterThan('total', 10)])
            ->limit(50)
            ->offset(100)
            ->union($other)
            ->build();

        // prewhere, filter, provider, cursor, having, limit, offset, union
        $this->assertEquals('click', $result['bindings'][0]);
        $this->assertEquals(5, $result['bindings'][1]);
        $this->assertEquals('t1', $result['bindings'][2]);
        $this->assertEquals('cur1', $result['bindings'][3]);
    }

    public function testPrewhereWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$id' => '_uid',
                default => $a,
            })
            ->prewhere([Query::equal('$id', ['abc'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` PREWHERE `_uid` IN (?)', $result['query']);
        $this->assertEquals(['abc'], $result['bindings']);
    }

    public function testPrewhereOnlyNoWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::greaterThan('ts', 100)])
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        // "PREWHERE" contains "WHERE" as a substring, so we check there is no standalone WHERE clause
        $withoutPrewhere = str_replace('PREWHERE', '', $result['query']);
        $this->assertStringNotContainsString('WHERE', $withoutPrewhere);
    }

    public function testPrewhereWithEmptyWhereFilter(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['a'])])
            ->filter([])
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $withoutPrewhere = str_replace('PREWHERE', '', $result['query']);
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

        $query = $result['query'];
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

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `a` IN (?) AND `b` > ? AND `c` < ?',
            $result['query']
        );
        $this->assertEquals([1, 2, 3], $result['bindings']);
    }

    public function testPrewhereResetClearsPrewhereQueries(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertStringNotContainsString('PREWHERE', $result['query']);
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

    // ══════════════════════════════════════════════════════════════════
    // 2. FINAL comprehensive (20+ tests)
    // ══════════════════════════════════════════════════════════════════

    public function testFinalBasicSelect(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->select(['name', 'ts'])
            ->build();

        $this->assertEquals('SELECT `name`, `ts` FROM `events` FINAL', $result['query']);
    }

    public function testFinalWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
    }

    public function testFinalWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*', 'total')
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
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

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('GROUP BY `type`', $result['query']);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
    }

    public function testFinalWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->distinct()
            ->select(['user_id'])
            ->build();

        $this->assertEquals('SELECT DISTINCT `user_id` FROM `events` FINAL', $result['query']);
    }

    public function testFinalWithSort(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sortAsc('name')
            ->sortDesc('ts')
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL ORDER BY `name` ASC, `ts` DESC', $result['query']);
    }

    public function testFinalWithLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->limit(10)
            ->offset(20)
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL LIMIT ? OFFSET ?', $result['query']);
        $this->assertEquals([10, 20], $result['bindings']);
    }

    public function testFinalWithCursor(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testFinalWithUnion(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->union($other)
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('UNION SELECT', $result['query']);
    }

    public function testFinalWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL PREWHERE `type` IN (?)', $result['query']);
    }

    public function testFinalWithSampleAlone(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.25)
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.25', $result['query']);
    }

    public function testFinalWithPrewhereSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.5)
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.5 PREWHERE `type` IN (?)', $result['query']);
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

        $query = $result['query'];
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

        $this->assertEquals('SELECT * FROM `events` FINAL', $result['query']);
        // Ensure FINAL appears only once
        $this->assertEquals(1, substr_count($result['query'], 'FINAL'));
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

        $query = $result['query'];
        $finalPos = strpos($query, 'FINAL');
        $joinPos = strpos($query, 'JOIN');

        $this->assertLessThan($joinPos, $finalPos);
    }

    public function testFinalWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->setAttributeResolver(fn (string $a): string => 'col_' . $a)
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('`col_status`', $result['query']);
    }

    public function testFinalWithConditionProvider(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addConditionProvider(fn (string $table): array => ['deleted = ?', [0]])
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('deleted = ?', $result['query']);
    }

    public function testFinalResetClearsFlag(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    public function testFinalWithWhenConditional(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);

        $result2 = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->final())
            ->build();

        $this->assertStringNotContainsString('FINAL', $result2['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 3. SAMPLE comprehensive (23 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testSample10Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.1)->build();
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1', $result['query']);
    }

    public function testSample50Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.5)->build();
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5', $result['query']);
    }

    public function testSample1Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.01)->build();
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.01', $result['query']);
    }

    public function testSample99Percent(): void
    {
        $result = (new Builder())->from('events')->sample(0.99)->build();
        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.99', $result['query']);
    }

    public function testSampleWithFilters(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.2)
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.2 WHERE `status` IN (?)', $result['query']);
    }

    public function testSampleWithJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.3)
            ->join('users', 'events.uid', 'users.id')
            ->build();

        $this->assertStringContainsString('SAMPLE 0.3', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
    }

    public function testSampleWithAggregations(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->count('*', 'cnt')
            ->build();

        $this->assertStringContainsString('SAMPLE 0.1', $result['query']);
        $this->assertStringContainsString('COUNT(*)', $result['query']);
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

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('GROUP BY', $result['query']);
        $this->assertStringContainsString('HAVING', $result['query']);
    }

    public function testSampleWithDistinct(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->distinct()
            ->select(['user_id'])
            ->build();

        $this->assertStringContainsString('SELECT DISTINCT', $result['query']);
        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
    }

    public function testSampleWithSort(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->sortDesc('ts')
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 ORDER BY `ts` DESC', $result['query']);
    }

    public function testSampleWithLimitOffset(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->limit(10)
            ->offset(20)
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 LIMIT ? OFFSET ?', $result['query']);
    }

    public function testSampleWithCursor(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->cursorAfter('xyz')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testSampleWithUnion(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->union($other)
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
    }

    public function testSampleWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.1 PREWHERE `type` IN (?)', $result['query']);
    }

    public function testSampleWithFinalKeyword(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.1', $result['query']);
    }

    public function testSampleWithFinalPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.2)
            ->prewhere([Query::equal('t', ['a'])])
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL SAMPLE 0.2 PREWHERE `t` IN (?)', $result['query']);
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

        $query = $result['query'];
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

        $query = $result['query'];
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
        $this->assertStringNotContainsString('SAMPLE', $result['query']);
    }

    public function testSampleWithWhenConditional(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);

        $result2 = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->sample(0.5))
            ->build();

        $this->assertStringNotContainsString('SAMPLE', $result2['query']);
    }

    public function testSampleCalledMultipleTimesLastWins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->sample(0.5)
            ->sample(0.9)
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.9', $result['query']);
    }

    public function testSampleWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->setAttributeResolver(fn (string $a): string => 'r_' . $a)
            ->filter([Query::equal('col', ['v'])])
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('`r_col`', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 4. ClickHouse regex: match() function (20 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testRegexBasicPattern(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', 'error|warn')])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result['query']);
        $this->assertEquals(['error|warn'], $result['bindings']);
    }

    public function testRegexWithEmptyPattern(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', '')])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result['query']);
        $this->assertEquals([''], $result['bindings']);
    }

    public function testRegexWithSpecialChars(): void
    {
        $pattern = '^/api/v[0-9]+\\.json$';
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('path', $pattern)])
            ->build();

        // Bindings preserve the pattern exactly as provided
        $this->assertEquals([$pattern], $result['bindings']);
    }

    public function testRegexWithVeryLongPattern(): void
    {
        $longPattern = str_repeat('a', 1000);
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', $longPattern)])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`msg`, ?)', $result['query']);
        $this->assertEquals([$longPattern], $result['bindings']);
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

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE match(`path`, ?) AND `status` IN (?)',
            $result['query']
        );
        $this->assertEquals(['^/api', 200], $result['bindings']);
    }

    public function testRegexInPrewhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` PREWHERE match(`path`, ?)', $result['query']);
        $this->assertEquals(['^/api'], $result['bindings']);
    }

    public function testRegexInPrewhereAndWhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->filter([Query::regex('msg', 'err')])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `logs` PREWHERE match(`path`, ?) WHERE match(`msg`, ?)',
            $result['query']
        );
        $this->assertEquals(['^/api', 'err'], $result['bindings']);
    }

    public function testRegexWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->setAttributeResolver(fn (string $a): string => 'col_' . $a)
            ->filter([Query::regex('msg', 'test')])
            ->build();

        $this->assertEquals('SELECT * FROM `logs` WHERE match(`col_msg`, ?)', $result['query']);
    }

    public function testRegexBindingPreserved(): void
    {
        $pattern = '(foo|bar)\\d+';
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::regex('msg', $pattern)])
            ->build();

        $this->assertEquals([$pattern], $result['bindings']);
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

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE match(`path`, ?) AND match(`msg`, ?)',
            $result['query']
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

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE (match(`path`, ?) AND `status` > ?)',
            $result['query']
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

        $this->assertEquals(
            'SELECT * FROM `logs` WHERE (match(`path`, ?) OR match(`path`, ?))',
            $result['query']
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

        $this->assertStringContainsString('match(`path`, ?)', $result['query']);
        $this->assertStringContainsString('`status` IN (?)', $result['query']);
    }

    public function testRegexWithFinal(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->final()
            ->filter([Query::regex('path', '^/api')])
            ->build();

        $this->assertStringContainsString('FROM `logs` FINAL', $result['query']);
        $this->assertStringContainsString('match(`path`, ?)', $result['query']);
    }

    public function testRegexWithSample(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->sample(0.5)
            ->filter([Query::regex('path', '^/api')])
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('match(`path`, ?)', $result['query']);
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

        $this->assertStringContainsString('match(`path`, ?)', $result['query']);
        $this->assertStringContainsString('`msg` LIKE ?', $result['query']);
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

        $this->assertStringContainsString('match(`path`, ?)', $result['query']);
        $this->assertStringContainsString('`msg` LIKE ?', $result['query']);
    }

    public function testRegexPrewhereWithRegexWhere(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->prewhere([Query::regex('path', '^/api')])
            ->filter([Query::regex('msg', 'error')])
            ->build();

        $this->assertStringContainsString('PREWHERE match(`path`, ?)', $result['query']);
        $this->assertStringContainsString('WHERE match(`msg`, ?)', $result['query']);
        $this->assertEquals(['^/api', 'error'], $result['bindings']);
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

        $this->assertEquals(['^/api', 'error', 'timeout'], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 5. Search exception (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testSearchThrowsExceptionMessage(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Full-text search (MATCH AGAINST) is not supported in ClickHouse');

        (new Builder())
            ->from('logs')
            ->filter([Query::search('content', 'hello world')])
            ->build();
    }

    public function testNotSearchThrowsExceptionMessage(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Full-text search (MATCH AGAINST) is not supported in ClickHouse');

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
            $this->assertStringContainsString('contains()', $e->getMessage());
        }
    }

    public function testSearchInLogicalAndThrows(): void
    {
        $this->expectException(Exception::class);

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
        $this->expectException(Exception::class);

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
        $this->expectException(Exception::class);

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
        $this->expectException(Exception::class);

        (new Builder())
            ->from('logs')
            ->prewhere([Query::search('content', 'hello')])
            ->build();
    }

    public function testNotSearchInPrewhereThrows(): void
    {
        $this->expectException(Exception::class);

        (new Builder())
            ->from('logs')
            ->prewhere([Query::notSearch('content', 'hello')])
            ->build();
    }

    public function testSearchWithFinalStillThrows(): void
    {
        $this->expectException(Exception::class);

        (new Builder())
            ->from('logs')
            ->final()
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    public function testSearchWithSampleStillThrows(): void
    {
        $this->expectException(Exception::class);

        (new Builder())
            ->from('logs')
            ->sample(0.5)
            ->filter([Query::search('content', 'hello')])
            ->build();
    }

    // ══════════════════════════════════════════════════════════════════
    // 6. ClickHouse rand() (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testRandomSortProducesLowercaseRand(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();

        $this->assertStringContainsString('rand()', $result['query']);
        $this->assertStringNotContainsString('RAND()', $result['query']);
    }

    public function testRandomSortCombinedWithAsc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortAsc('name')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY `name` ASC, rand()', $result['query']);
    }

    public function testRandomSortCombinedWithDesc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortDesc('ts')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY `ts` DESC, rand()', $result['query']);
    }

    public function testRandomSortCombinedWithAscAndDesc(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortAsc('name')
            ->sortDesc('ts')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY `name` ASC, `ts` DESC, rand()', $result['query']);
    }

    public function testRandomSortWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` FINAL ORDER BY rand()', $result['query']);
    }

    public function testRandomSortWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` SAMPLE 0.5 ORDER BY rand()', $result['query']);
    }

    public function testRandomSortWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->sortRandom()
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` PREWHERE `type` IN (?) ORDER BY rand()',
            $result['query']
        );
    }

    public function testRandomSortWithLimit(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->limit(10)
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand() LIMIT ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testRandomSortWithFiltersAndJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->filter([Query::equal('status', ['active'])])
            ->sortRandom()
            ->build();

        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
        $this->assertStringContainsString('ORDER BY rand()', $result['query']);
    }

    public function testRandomSortAlone(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `events` ORDER BY rand()', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 7. All filter types work correctly (31 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testFilterEqualSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('a', ['x'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $result['query']);
        $this->assertEquals(['x'], $result['bindings']);
    }

    public function testFilterEqualMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('a', ['x', 'y', 'z'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?, ?, ?)', $result['query']);
        $this->assertEquals(['x', 'y', 'z'], $result['bindings']);
    }

    public function testFilterNotEqualSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('a', 'x')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` != ?', $result['query']);
        $this->assertEquals(['x'], $result['bindings']);
    }

    public function testFilterNotEqualMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('a', ['x', 'y'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT IN (?, ?)', $result['query']);
        $this->assertEquals(['x', 'y'], $result['bindings']);
    }

    public function testFilterLessThanValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::lessThan('a', 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` < ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testFilterLessThanEqualValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::lessThanEqual('a', 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` <= ?', $result['query']);
    }

    public function testFilterGreaterThanValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('a', 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` > ?', $result['query']);
    }

    public function testFilterGreaterThanEqualValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThanEqual('a', 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` >= ?', $result['query']);
    }

    public function testFilterBetweenValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::between('a', 1, 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([1, 10], $result['bindings']);
    }

    public function testFilterNotBetweenValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notBetween('a', 1, 10)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT BETWEEN ? AND ?', $result['query']);
    }

    public function testFilterStartsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::startsWith('a', 'foo')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` LIKE ?', $result['query']);
        $this->assertEquals(['foo%'], $result['bindings']);
    }

    public function testFilterNotStartsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notStartsWith('a', 'foo')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT LIKE ?', $result['query']);
        $this->assertEquals(['foo%'], $result['bindings']);
    }

    public function testFilterEndsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::endsWith('a', 'bar')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` LIKE ?', $result['query']);
        $this->assertEquals(['%bar'], $result['bindings']);
    }

    public function testFilterNotEndsWithValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEndsWith('a', 'bar')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%bar'], $result['bindings']);
    }

    public function testFilterContainsSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('a', ['foo'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` LIKE ?', $result['query']);
        $this->assertEquals(['%foo%'], $result['bindings']);
    }

    public function testFilterContainsMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('a', ['foo', 'bar'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` LIKE ? OR `a` LIKE ?)', $result['query']);
        $this->assertEquals(['%foo%', '%bar%'], $result['bindings']);
    }

    public function testFilterContainsAnyValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::containsAny('a', ['x', 'y'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?, ?)', $result['query']);
    }

    public function testFilterContainsAllValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::containsAll('a', ['x', 'y'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` LIKE ? AND `a` LIKE ?)', $result['query']);
        $this->assertEquals(['%x%', '%y%'], $result['bindings']);
    }

    public function testFilterNotContainsSingleValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notContains('a', ['foo'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%foo%'], $result['bindings']);
    }

    public function testFilterNotContainsMultipleValues(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notContains('a', ['foo', 'bar'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` NOT LIKE ? AND `a` NOT LIKE ?)', $result['query']);
    }

    public function testFilterIsNullValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::isNull('a')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IS NULL', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testFilterIsNotNullValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::isNotNull('a')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IS NOT NULL', $result['query']);
    }

    public function testFilterExistsValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::exists(['a', 'b'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IS NOT NULL AND `b` IS NOT NULL)', $result['query']);
    }

    public function testFilterNotExistsValue(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notExists(['a', 'b'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IS NULL AND `b` IS NULL)', $result['query']);
    }

    public function testFilterAndLogical(): void
    {
        $result = (new Builder())->from('t')->filter([
            Query::and([Query::equal('a', [1]), Query::equal('b', [2])]),
        ])->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) AND `b` IN (?))', $result['query']);
    }

    public function testFilterOrLogical(): void
    {
        $result = (new Builder())->from('t')->filter([
            Query::or([Query::equal('a', [1]), Query::equal('b', [2])]),
        ])->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR `b` IN (?))', $result['query']);
    }

    public function testFilterRaw(): void
    {
        $result = (new Builder())->from('t')->filter([Query::raw('x > ? AND y < ?', [1, 2])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE x > ? AND y < ?', $result['query']);
        $this->assertEquals([1, 2], $result['bindings']);
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

        $this->assertStringContainsString('(`a` IN (?) OR (`b` > ? AND `c` < ?))', $result['query']);
        $this->assertStringContainsString('`d` IN (?)', $result['query']);
    }

    public function testFilterWithFloats(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('price', 9.99)])->build();
        $this->assertEquals([9.99], $result['bindings']);
    }

    public function testFilterWithNegativeNumbers(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('temp', -40)])->build();
        $this->assertEquals([-40], $result['bindings']);
    }

    public function testFilterWithEmptyStrings(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('name', [''])])->build();
        $this->assertEquals([''], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 8. Aggregation with ClickHouse features (15 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testAggregationCountWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*', 'total')
            ->build();

        $this->assertEquals('SELECT COUNT(*) AS `total` FROM `events` FINAL', $result['query']);
    }

    public function testAggregationSumWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->sum('amount', 'total_amount')
            ->build();

        $this->assertEquals('SELECT SUM(`amount`) AS `total_amount` FROM `events` SAMPLE 0.1', $result['query']);
    }

    public function testAggregationAvgWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->avg('price', 'avg_price')
            ->build();

        $this->assertStringContainsString('AVG(`price`) AS `avg_price`', $result['query']);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
    }

    public function testAggregationMinWithPrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->filter([Query::greaterThan('amount', 0)])
            ->min('price', 'min_price')
            ->build();

        $this->assertStringContainsString('MIN(`price`) AS `min_price`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
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

        $this->assertStringContainsString('MAX(`price`) AS `max_price`', $result['query']);
        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
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

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result['query']);
        $this->assertStringContainsString('SUM(`amount`) AS `total`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('GROUP BY `region`', $result['query']);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
    }

    public function testAggregationWithJoinFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->count('*', 'total')
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('COUNT(*)', $result['query']);
    }

    public function testAggregationWithDistinctSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->distinct()
            ->count('user_id', 'unique_users')
            ->build();

        $this->assertStringContainsString('SELECT DISTINCT', $result['query']);
        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
    }

    public function testAggregationWithAliasPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->count('*', 'click_count')
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `click_count`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testAggregationWithoutAliasFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->count('*')
            ->build();

        $this->assertStringContainsString('COUNT(*)', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
        $this->assertStringContainsString('FINAL', $result['query']);
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

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
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

        $this->assertStringContainsString('UNION', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testAggregationAttributeResolverPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                'amt' => 'amount_cents',
                default => $a,
            })
            ->prewhere([Query::equal('type', ['sale'])])
            ->sum('amt', 'total')
            ->build();

        $this->assertStringContainsString('SUM(`amount_cents`)', $result['query']);
    }

    public function testAggregationConditionProviderPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['sale'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->count('*', 'cnt')
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('tenant = ?', $result['query']);
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

        $query = $result['query'];
        $this->assertStringContainsString('FINAL', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('GROUP BY', $query);
        $this->assertStringContainsString('HAVING', $query);
    }

    // ══════════════════════════════════════════════════════════════════
    // 9. Join with ClickHouse features (15 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testJoinWithFinalFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->join('users', 'events.uid', 'users.id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` FINAL JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result['query']
        );
    }

    public function testJoinWithSampleFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->join('users', 'events.uid', 'users.id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `events` SAMPLE 0.5 JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result['query']
        );
    }

    public function testJoinWithPrewhereFeature(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testJoinWithPrewhereWhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
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

        $query = $result['query'];
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

        $this->assertStringContainsString('LEFT JOIN `users`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testRightJoinWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->rightJoin('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertStringContainsString('RIGHT JOIN `users`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testCrossJoinWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->crossJoin('config')
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('CROSS JOIN `config`', $result['query']);
    }

    public function testMultipleJoinsWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->leftJoin('sessions', 'events.sid', 'sessions.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->build();

        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('LEFT JOIN `sessions`', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
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

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('GROUP BY', $result['query']);
    }

    public function testJoinPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('users.age', 18)])
            ->build();

        $this->assertEquals(['click', 18], $result['bindings']);
    }

    public function testJoinAttributeResolverPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                'uid' => 'user_id',
                default => $a,
            })
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('uid', ['abc'])])
            ->build();

        $this->assertStringContainsString('PREWHERE `user_id` IN (?)', $result['query']);
    }

    public function testJoinConditionProviderPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('tenant = ?', $result['query']);
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

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
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

        $query = $result['query'];

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

    // ══════════════════════════════════════════════════════════════════
    // 10. Union with ClickHouse features (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testUnionMainHasFinal(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->final()
            ->union($other)
            ->build();

        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('UNION SELECT * FROM `archive`', $result['query']);
    }

    public function testUnionMainHasSample(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->union($other)
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
    }

    public function testUnionMainHasPrewhere(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->union($other)
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
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

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
    }

    public function testUnionAllWithPrewhere(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->unionAll($other)
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION ALL', $result['query']);
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

        // prewhere, where, union
        $this->assertEquals(['click', 2024, 2023], $result['bindings']);
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

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertEquals(2, substr_count($result['query'], 'UNION'));
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

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
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

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('COUNT(*)', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
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

        $query = $result['query'];
        $this->assertStringContainsString('SELECT `name`, `count`', $query);
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('UNION', $query);
    }

    // ══════════════════════════════════════════════════════════════════
    // 11. toRawSql with ClickHouse features (15 tests)
    // ══════════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════════
    // 12. Reset comprehensive (15 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testResetClearsPrewhereState(): void
    {
        $builder = (new Builder())->from('events')->prewhere([Query::equal('type', ['click'])]);
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();

        $this->assertStringNotContainsString('PREWHERE', $result['query']);
    }

    public function testResetClearsFinalState(): void
    {
        $builder = (new Builder())->from('events')->final();
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();

        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    public function testResetClearsSampleState(): void
    {
        $builder = (new Builder())->from('events')->sample(0.5);
        $builder->build();
        $builder->reset();
        $result = $builder->from('events')->build();

        $this->assertStringNotContainsString('SAMPLE', $result['query']);
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

        $this->assertEquals('SELECT * FROM `events`', $result['query']);
    }

    public function testResetPreservesAttributeResolver(): void
    {
        $resolver = fn (string $a): string => 'r_' . $a;
        $builder = (new Builder())
            ->from('events')
            ->setAttributeResolver($resolver)
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->filter([Query::equal('col', ['v'])])->build();
        $this->assertStringContainsString('`r_col`', $result['query']);
    }

    public function testResetPreservesConditionProviders(): void
    {
        $builder = (new Builder())
            ->from('events')
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->final();
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertStringContainsString('tenant = ?', $result['query']);
    }

    public function testResetClearsTable(): void
    {
        $builder = (new Builder())->from('events');
        $builder->build();
        $builder->reset();

        $result = $builder->from('logs')->build();
        $this->assertStringContainsString('FROM `logs`', $result['query']);
        $this->assertStringNotContainsString('events', $result['query']);
    }

    public function testResetClearsFilters(): void
    {
        $builder = (new Builder())->from('events')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertStringNotContainsString('WHERE', $result['query']);
    }

    public function testResetClearsUnions(): void
    {
        $other = (new Builder())->from('archive');
        $builder = (new Builder())->from('events')->union($other);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertStringNotContainsString('UNION', $result['query']);
    }

    public function testResetClearsBindings(): void
    {
        $builder = (new Builder())->from('events')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();

        $result = $builder->from('events')->build();
        $this->assertEquals([], $result['bindings']);
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
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testResetRebuildWithPrewhere(): void
    {
        $builder = new Builder();
        $builder->from('events')->final()->build();
        $builder->reset();

        $result = $builder->from('events')->prewhere([Query::equal('x', [1])])->build();
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    public function testResetRebuildWithFinal(): void
    {
        $builder = new Builder();
        $builder->from('events')->prewhere([Query::equal('x', [1])])->build();
        $builder->reset();

        $result = $builder->from('events')->final()->build();
        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringNotContainsString('PREWHERE', $result['query']);
    }

    public function testResetRebuildWithSample(): void
    {
        $builder = new Builder();
        $builder->from('events')->final()->build();
        $builder->reset();

        $result = $builder->from('events')->sample(0.5)->build();
        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
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
        $this->assertEquals('SELECT * FROM `d`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 13. when() with ClickHouse features (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testWhenTrueAddsPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();

        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
    }

    public function testWhenFalseDoesNotAddPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();

        $this->assertStringNotContainsString('PREWHERE', $result['query']);
    }

    public function testWhenTrueAddsFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
    }

    public function testWhenFalseDoesNotAddFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(false, fn (Builder $b) => $b->final())
            ->build();

        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    public function testWhenTrueAddsSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
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

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
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

        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result['query']);
    }

    public function testWhenChainedMultipleTimesWithClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->when(true, fn (Builder $b) => $b->sample(0.5))
            ->when(true, fn (Builder $b) => $b->prewhere([Query::equal('type', ['click'])]))
            ->build();

        $this->assertStringContainsString('FINAL SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
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

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testWhenCombinedWithRegularWhen(): void
    {
        $result = (new Builder())
            ->from('events')
            ->when(true, fn (Builder $b) => $b->final())
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 14. Condition provider with ClickHouse (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testProviderWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['deleted = ?', [0]])
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('deleted = ?', $result['query']);
    }

    public function testProviderWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addConditionProvider(fn (string $t): array => ['deleted = ?', [0]])
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('deleted = ?', $result['query']);
    }

    public function testProviderWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->addConditionProvider(fn (string $t): array => ['deleted = ?', [0]])
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('deleted = ?', $result['query']);
    }

    public function testProviderPrewhereWhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->build();

        // prewhere, filter, provider
        $this->assertEquals(['click', 5, 't1'], $result['bindings']);
    }

    public function testMultipleProvidersPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['o1']])
            ->build();

        $this->assertEquals(['click', 't1', 'o1'], $result['bindings']);
    }

    public function testProviderPrewhereCursorLimitBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->limit(10)
            ->build();

        // prewhere, provider, cursor, limit
        $this->assertEquals('click', $result['bindings'][0]);
        $this->assertEquals('t1', $result['bindings'][1]);
        $this->assertEquals('cur1', $result['bindings'][2]);
        $this->assertEquals(10, $result['bindings'][3]);
    }

    public function testProviderAllClickHouseFeatures(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 0)])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->build();

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('tenant = ?', $result['query']);
    }

    public function testProviderPrewhereAggregation(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->count('*', 'cnt')
            ->build();

        $this->assertStringContainsString('COUNT(*)', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('tenant = ?', $result['query']);
    }

    public function testProviderJoinsPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->join('users', 'events.uid', 'users.id')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->build();

        $this->assertStringContainsString('JOIN', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('tenant = ?', $result['query']);
    }

    public function testProviderReferencesTableNameFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->addConditionProvider(fn (string $table): array => [
                $table . '.deleted = ?',
                [0],
            ])
            ->build();

        $this->assertStringContainsString('events.deleted = ?', $result['query']);
        $this->assertStringContainsString('FINAL', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 15. Cursor with ClickHouse features (8 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testCursorAfterWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testCursorBeforeWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorBefore('abc')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('`_cursor` < ?', $result['query']);
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

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testCursorWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testCursorWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    public function testCursorPrewhereBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->build();

        $this->assertEquals('click', $result['bindings'][0]);
        $this->assertEquals('cur1', $result['bindings'][1]);
    }

    public function testCursorPrewhereProviderBindingOrder(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->cursorAfter('cur1')
            ->sortAsc('_cursor')
            ->build();

        $this->assertEquals('click', $result['bindings'][0]);
        $this->assertEquals('t1', $result['bindings'][1]);
        $this->assertEquals('cur1', $result['bindings'][2]);
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

        $query = $result['query'];
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('`_cursor` > ?', $query);
        $this->assertStringContainsString('LIMIT', $query);
    }

    // ══════════════════════════════════════════════════════════════════
    // 16. page() with ClickHouse features (5 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testPageWithPrewhere(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->page(2, 25)
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        $this->assertEquals(['click', 25, 25], $result['bindings']);
    }

    public function testPageWithFinal(): void
    {
        $result = (new Builder())
            ->from('events')
            ->final()
            ->page(3, 10)
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        $this->assertEquals([10, 20], $result['bindings']);
    }

    public function testPageWithSample(): void
    {
        $result = (new Builder())
            ->from('events')
            ->sample(0.5)
            ->page(1, 50)
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertEquals([50, 0], $result['bindings']);
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

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('LIMIT', $result['query']);
        $this->assertStringContainsString('OFFSET', $result['query']);
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

        $query = $result['query'];
        $this->assertStringContainsString('FINAL', $query);
        $this->assertStringContainsString('SAMPLE', $query);
        $this->assertStringContainsString('PREWHERE', $query);
        $this->assertStringContainsString('WHERE', $query);
        $this->assertStringContainsString('ORDER BY', $query);
        $this->assertStringContainsString('LIMIT', $query);
        $this->assertStringContainsString('OFFSET', $query);
    }

    // ══════════════════════════════════════════════════════════════════
    // 17. Fluent chaining comprehensive (5 tests)
    // ══════════════════════════════════════════════════════════════════

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

        $this->assertNotEmpty($result['query']);
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

        $this->assertEquals($result1['query'], $result2['query']);
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

        $this->assertEquals($result1['query'], $result2['query']);
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

        $this->assertEquals('SELECT * FROM `logs` SAMPLE 0.5', $result['query']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 18. SQL clause ordering verification (10 tests)
    // ══════════════════════════════════════════════════════════════════

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

        $query = $result['query'];

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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];

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

    // ══════════════════════════════════════════════════════════════════
    // 19. Batch mode with ClickHouse (5 tests)
    // ══════════════════════════════════════════════════════════════════

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

        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
        $this->assertStringContainsString('ORDER BY', $result['query']);
        $this->assertStringContainsString('LIMIT', $result['query']);
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

        $this->assertStringContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
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

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
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

        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
        $this->assertStringContainsString('ORDER BY', $result['query']);
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

        $this->assertEquals($resultA['query'], $resultB['query']);
        $this->assertEquals($resultA['bindings'], $resultB['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 20. Edge cases (10 tests)
    // ══════════════════════════════════════════════════════════════════

    public function testEmptyTableNameWithFinal(): void
    {
        $result = (new Builder())
            ->from('')
            ->final()
            ->build();

        $this->assertStringContainsString('FINAL', $result['query']);
    }

    public function testEmptyTableNameWithSample(): void
    {
        $result = (new Builder())
            ->from('')
            ->sample(0.5)
            ->build();

        $this->assertStringContainsString('SAMPLE 0.5', $result['query']);
    }

    public function testPrewhereWithEmptyFilterValues(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', [])])
            ->build();

        $this->assertStringContainsString('PREWHERE', $result['query']);
    }

    public function testVeryLongTableNameWithFinalSample(): void
    {
        $longName = str_repeat('a', 200);
        $result = (new Builder())
            ->from($longName)
            ->final()
            ->sample(0.1)
            ->build();

        $this->assertStringContainsString('`' . $longName . '`', $result['query']);
        $this->assertStringContainsString('FINAL SAMPLE 0.1', $result['query']);
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

        $this->assertEquals($result1['query'], $result2['query']);
        $this->assertEquals($result2['query'], $result3['query']);
        $this->assertEquals($result1['bindings'], $result2['bindings']);
        $this->assertEquals($result2['bindings'], $result3['bindings']);
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
        $this->assertStringContainsString('FINAL', $result2['query']);
        $this->assertStringContainsString('SAMPLE', $result2['query']);
        $this->assertStringContainsString('PREWHERE', $result2['query']);

        // Bindings are consistent
        $this->assertEquals($result1['bindings'], $result2['bindings']);
    }

    public function testSampleWithAllBindingTypes(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);
        $result = (new Builder())
            ->from('events')
            ->sample(0.1)
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
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

        // Verify all binding types present
        $this->assertNotEmpty($result['bindings']);
        $this->assertGreaterThan(5, count($result['bindings']));
    }

    public function testPrewhereAppearsCorrectlyWithoutJoins(): void
    {
        $result = (new Builder())
            ->from('events')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->build();

        $query = $result['query'];
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

        $query = $result['query'];
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

        $query = $result['query'];
        $this->assertStringContainsString('FROM `events` FINAL SAMPLE 0.1', $query);
        $this->assertStringContainsString('JOIN `users`', $query);
        $this->assertStringContainsString('LEFT JOIN `sessions`', $query);

        // FINAL SAMPLE appears before JOINs
        $finalSamplePos = strpos($query, 'FINAL SAMPLE 0.1');
        $joinPos = strpos($query, 'JOIN');
        $this->assertLessThan($joinPos, $finalSamplePos);
    }

    // ══════════════════════════════════════════════════════════════════
    // 1. Spatial/Vector/ElemMatch Exception Tests
    // ══════════════════════════════════════════════════════════════════

    public function testFilterCrossesThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::crosses('attr', [1])])->build();
    }

    public function testFilterNotCrossesThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notCrosses('attr', [1])])->build();
    }

    public function testFilterDistanceEqualThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceEqual('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceNotEqualThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceNotEqual('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceGreaterThanThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceGreaterThan('attr', [0, 0], 1)])->build();
    }

    public function testFilterDistanceLessThanThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceLessThan('attr', [0, 0], 1)])->build();
    }

    public function testFilterIntersectsThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::intersects('attr', [1])])->build();
    }

    public function testFilterNotIntersectsThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notIntersects('attr', [1])])->build();
    }

    public function testFilterOverlapsThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::overlaps('attr', [1])])->build();
    }

    public function testFilterNotOverlapsThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notOverlaps('attr', [1])])->build();
    }

    public function testFilterTouchesThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::touches('attr', [1])])->build();
    }

    public function testFilterNotTouchesThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notTouches('attr', [1])])->build();
    }

    public function testFilterVectorDotThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorDot('attr', [1.0, 2.0])])->build();
    }

    public function testFilterVectorCosineThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorCosine('attr', [1.0, 2.0])])->build();
    }

    public function testFilterVectorEuclideanThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorEuclidean('attr', [1.0, 2.0])])->build();
    }

    public function testFilterElemMatchThrowsException(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::elemMatch('attr', [Query::equal('x', [1])])])->build();
    }

    // ══════════════════════════════════════════════════════════════════
    // 2. SAMPLE Boundary Values
    // ══════════════════════════════════════════════════════════════════

    public function testSampleZero(): void
    {
        $result = (new Builder())->from('t')->sample(0.0)->build();
        $this->assertStringContainsString('SAMPLE 0', $result['query']);
    }

    public function testSampleOne(): void
    {
        $result = (new Builder())->from('t')->sample(1.0)->build();
        $this->assertStringContainsString('SAMPLE 1', $result['query']);
    }

    public function testSampleNegative(): void
    {
        // Builder doesn't validate - it passes through
        $result = (new Builder())->from('t')->sample(-0.5)->build();
        $this->assertStringContainsString('SAMPLE -0.5', $result['query']);
    }

    public function testSampleGreaterThanOne(): void
    {
        $result = (new Builder())->from('t')->sample(2.0)->build();
        $this->assertStringContainsString('SAMPLE 2', $result['query']);
    }

    public function testSampleVerySmall(): void
    {
        $result = (new Builder())->from('t')->sample(0.001)->build();
        $this->assertStringContainsString('SAMPLE 0.001', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 3. Standalone Compiler Method Tests
    // ══════════════════════════════════════════════════════════════════

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
        $this->expectException(\Utopia\Query\Exception::class);
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
        $this->expectException(\Utopia\Query\Exception::class);
        $builder->compileJoin(Query::equal('x', [1]));
    }

    // ══════════════════════════════════════════════════════════════════
    // 4. Union with ClickHouse Features on Both Sides
    // ══════════════════════════════════════════════════════════════════

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
        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('PREWHERE', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
        $this->assertStringContainsString('FROM `archive` FINAL SAMPLE 0.5', $result['query']);
    }

    public function testUnionAllBothWithFinal(): void
    {
        $sub = (new Builder())->from('b')->final();
        $result = (new Builder())->from('a')->final()
            ->unionAll($sub)
            ->build();
        $this->assertStringContainsString('FROM `a` FINAL', $result['query']);
        $this->assertStringContainsString('UNION ALL SELECT * FROM `b` FINAL', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 5. PREWHERE Binding Order Exhaustive Tests
    // ══════════════════════════════════════════════════════════════════

    public function testPrewhereBindingOrderWithFilterAndHaving(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->prewhere([Query::equal('type', ['click'])])
            ->filter([Query::greaterThan('count', 5)])
            ->groupBy(['type'])
            ->having([Query::greaterThan('total', 10)])
            ->build();
        // Binding order: prewhere, filter, having
        $this->assertEquals(['click', 5, 10], $result['bindings']);
    }

    public function testPrewhereBindingOrderWithProviderAndCursor(): void
    {
        $result = (new Builder())->from('t')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t) => ["_tenant = ?", ['t1']])
            ->cursorAfter('abc')
            ->sortAsc('_cursor')
            ->build();
        // Binding order: prewhere, filter(none), provider, cursor
        $this->assertEquals(['click', 't1', 'abc'], $result['bindings']);
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
        // prewhere bindings first, then filter, then limit
        $this->assertEquals(['a', 3, 30, 10], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 6. Search Exception in PREWHERE Interaction
    // ══════════════════════════════════════════════════════════════════

    public function testSearchInFilterThrowsExceptionWithMessage(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        $this->expectExceptionMessage('Full-text search');
        (new Builder())->from('t')->filter([Query::search('content', 'hello')])->build();
    }

    public function testSearchInPrewhereThrowsExceptionWithMessage(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->prewhere([Query::search('content', 'hello')])->build();
    }

    // ══════════════════════════════════════════════════════════════════
    // 7. Join Combinations with FINAL/SAMPLE
    // ══════════════════════════════════════════════════════════════════

    public function testLeftJoinWithFinalAndSample(): void
    {
        $result = (new Builder())->from('events')
            ->final()
            ->sample(0.1)
            ->leftJoin('users', 'events.uid', 'users.id')
            ->build();
        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 LEFT JOIN `users` ON `events`.`uid` = `users`.`id`',
            $result['query']
        );
    }

    public function testRightJoinWithFinalFeature(): void
    {
        $result = (new Builder())->from('events')
            ->final()
            ->rightJoin('users', 'events.uid', 'users.id')
            ->build();
        $this->assertStringContainsString('FROM `events` FINAL', $result['query']);
        $this->assertStringContainsString('RIGHT JOIN', $result['query']);
    }

    public function testCrossJoinWithPrewhereFeature(): void
    {
        $result = (new Builder())->from('events')
            ->crossJoin('colors')
            ->prewhere([Query::equal('type', ['a'])])
            ->build();
        $this->assertStringContainsString('CROSS JOIN `colors`', $result['query']);
        $this->assertStringContainsString('PREWHERE `type` IN (?)', $result['query']);
        $this->assertEquals(['a'], $result['bindings']);
    }

    public function testJoinWithNonDefaultOperator(): void
    {
        $result = (new Builder())->from('t')
            ->join('other', 'a', 'b', '!=')
            ->build();
        $this->assertStringContainsString('JOIN `other` ON `a` != `b`', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 8. Condition Provider Position Verification
    // ══════════════════════════════════════════════════════════════════

    public function testConditionProviderInWhereNotPrewhere(): void
    {
        $result = (new Builder())->from('t')
            ->prewhere([Query::equal('type', ['click'])])
            ->addConditionProvider(fn (string $t) => ["_tenant = ?", ['t1']])
            ->build();
        $query = $result['query'];
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
            ->addConditionProvider(fn (string $t) => ["_deleted = ?", [0]])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE _deleted = ?', $result['query']);
        $this->assertEquals([0], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 9. Page Boundary Values
    // ══════════════════════════════════════════════════════════════════

    public function testPageZero(): void
    {
        $result = (new Builder())->from('t')->page(0, 10)->build();
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        // page 0 -> offset clamped to 0
        $this->assertEquals([10, 0], $result['bindings']);
    }

    public function testPageNegative(): void
    {
        $result = (new Builder())->from('t')->page(-1, 10)->build();
        $this->assertEquals([10, 0], $result['bindings']);
    }

    public function testPageLargeNumber(): void
    {
        $result = (new Builder())->from('t')->page(1000000, 25)->build();
        $this->assertEquals([25, 24999975], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 10. Build Without From
    // ══════════════════════════════════════════════════════════════════

    public function testBuildWithoutFrom(): void
    {
        $result = (new Builder())->filter([Query::equal('x', [1])])->build();
        $this->assertStringContainsString('FROM ``', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 11. toRawSql Edge Cases for ClickHouse
    // ══════════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════════
    // 12. Having with Multiple Sub-Queries
    // ══════════════════════════════════════════════════════════════════

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
        $this->assertStringContainsString('HAVING `total` > ? AND `total` < ?', $result['query']);
        $this->assertContains(5, $result['bindings']);
        $this->assertContains(100, $result['bindings']);
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
        $this->assertStringContainsString('HAVING (`total` > ? OR `total` < ?)', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 13. Reset Property-by-Property Verification
    // ══════════════════════════════════════════════════════════════════

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

        $this->assertEquals('SELECT * FROM `other`', $result['query']);
        $this->assertEquals([], $result['bindings']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
        $this->assertStringNotContainsString('SAMPLE', $result['query']);
        $this->assertStringNotContainsString('PREWHERE', $result['query']);
    }

    public function testResetFollowedByUnion(): void
    {
        $builder = (new Builder())->from('a')
            ->final()
            ->union((new Builder())->from('old'));
        $builder->reset()->from('b');
        $result = $builder->build();
        $this->assertEquals('SELECT * FROM `b`', $result['query']);
        $this->assertStringNotContainsString('UNION', $result['query']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
    }

    public function testConditionProviderPersistsAfterReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->final()
            ->addConditionProvider(fn (string $t) => ["_tenant = ?", ['t1']]);
        $builder->build();
        $builder->reset()->from('other');
        $result = $builder->build();
        $this->assertStringContainsString('FROM `other`', $result['query']);
        $this->assertStringNotContainsString('FINAL', $result['query']);
        $this->assertStringContainsString('_tenant = ?', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 14. Exact Full SQL Assertions
    // ══════════════════════════════════════════════════════════════════

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
        $this->assertEquals(
            'SELECT * FROM `events` FINAL SAMPLE 0.1 PREWHERE `event_type` IN (?) WHERE `amount` > ? ORDER BY `amount` DESC LIMIT ?',
            $result['query']
        );
        $this->assertEquals(['purchase', 100, 50], $result['bindings']);
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
        $this->assertEquals(
            'SELECT DISTINCT COUNT(*) AS `total`, `event_type` FROM `events` FINAL SAMPLE 0.1 JOIN `users` ON `events`.`uid` = `users`.`id` PREWHERE `event_type` IN (?) WHERE `amount` > ? GROUP BY `event_type` HAVING `total` > ? ORDER BY `total` DESC LIMIT ? OFFSET ? UNION SELECT * FROM `archive` FINAL WHERE `status` IN (?)',
            $result['query']
        );
        $this->assertEquals(['purchase', 100, 5, 50, 10, 'closed'], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 15. Query::compile() Integration Tests
    // ══════════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════════
    // 16. Binding Type Assertions with assertSame
    // ══════════════════════════════════════════════════════════════════

    public function testBindingTypesPreservedInt(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('age', 18)])->build();
        $this->assertSame([18], $result['bindings']);
    }

    public function testBindingTypesPreservedFloat(): void
    {
        $result = (new Builder())->from('t')->filter([Query::greaterThan('score', 9.5)])->build();
        $this->assertSame([9.5], $result['bindings']);
    }

    public function testBindingTypesPreservedBool(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('active', [true])])->build();
        $this->assertSame([true], $result['bindings']);
    }

    public function testBindingTypesPreservedNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('val', [null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `val` IS NULL', $result['query']);
        $this->assertSame([], $result['bindings']);
    }

    public function testEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', ['a', null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` IN (?) OR `col` IS NULL)', $result['query']);
        $this->assertSame(['a'], $result['bindings']);
    }

    public function testNotEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', [null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NOT NULL', $result['query']);
        $this->assertSame([], $result['bindings']);
    }

    public function testNotEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', 'b', null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` NOT IN (?, ?) AND `col` IS NOT NULL)', $result['query']);
        $this->assertSame(['a', 'b'], $result['bindings']);
    }

    public function testBindingTypesPreservedString(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('name', ['hello'])])->build();
        $this->assertSame(['hello'], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 17. Raw Inside Logical Groups
    // ══════════════════════════════════════════════════════════════════

    public function testRawInsideLogicalAnd(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::and([
                Query::greaterThan('x', 1),
                Query::raw('custom_func(y) > ?', [5]),
            ])])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`x` > ? AND custom_func(y) > ?)', $result['query']);
        $this->assertEquals([1, 5], $result['bindings']);
    }

    public function testRawInsideLogicalOr(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::or([
                Query::equal('a', [1]),
                Query::raw('b IS NOT NULL', []),
            ])])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR b IS NOT NULL)', $result['query']);
        $this->assertEquals([1], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 18. Negative/Zero Limit and Offset
    // ══════════════════════════════════════════════════════════════════

    public function testNegativeLimit(): void
    {
        $result = (new Builder())->from('t')->limit(-1)->build();
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([-1], $result['bindings']);
    }

    public function testNegativeOffset(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(-5)->build();
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testLimitZero(): void
    {
        $result = (new Builder())->from('t')->limit(0)->build();
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([0], $result['bindings']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 19. Multiple Limits/Offsets/Cursors First Wins
    // ══════════════════════════════════════════════════════════════════

    public function testMultipleLimitsFirstWins(): void
    {
        $result = (new Builder())->from('t')->limit(10)->limit(20)->build();
        $this->assertEquals([10], $result['bindings']);
    }

    public function testMultipleOffsetsFirstWins(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(5)->offset(50)->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testCursorAfterAndBeforeFirstWins(): void
    {
        $result = (new Builder())->from('t')->cursorAfter('a')->cursorBefore('b')->sortAsc('_cursor')->build();
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
    }

    // ══════════════════════════════════════════════════════════════════
    // 20. Distinct + Union
    // ══════════════════════════════════════════════════════════════════

    public function testDistinctWithUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())->from('a')->distinct()->union($other)->build();
        $this->assertEquals('SELECT DISTINCT * FROM `a` UNION SELECT * FROM `b`', $result['query']);
    }
}
