<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class QueryHelperTest extends TestCase
{
    public function testIsMethodValid(): void
    {
        $this->assertTrue(Query::isMethod('equal'));
        $this->assertTrue(Query::isMethod('notEqual'));
        $this->assertTrue(Query::isMethod('lessThan'));
        $this->assertTrue(Query::isMethod('greaterThan'));
        $this->assertTrue(Query::isMethod('contains'));
        $this->assertTrue(Query::isMethod('search'));
        $this->assertTrue(Query::isMethod('orderAsc'));
        $this->assertTrue(Query::isMethod('orderDesc'));
        $this->assertTrue(Query::isMethod('orderRandom'));
        $this->assertTrue(Query::isMethod('limit'));
        $this->assertTrue(Query::isMethod('offset'));
        $this->assertTrue(Query::isMethod('cursorAfter'));
        $this->assertTrue(Query::isMethod('cursorBefore'));
        $this->assertTrue(Query::isMethod('isNull'));
        $this->assertTrue(Query::isMethod('isNotNull'));
        $this->assertTrue(Query::isMethod('between'));
        $this->assertTrue(Query::isMethod('select'));
        $this->assertTrue(Query::isMethod('or'));
        $this->assertTrue(Query::isMethod('and'));
        $this->assertTrue(Query::isMethod('crosses'));
        $this->assertTrue(Query::isMethod('vectorDot'));
        $this->assertTrue(Query::isMethod('exists'));
        $this->assertTrue(Query::isMethod('notExists'));
        $this->assertTrue(Query::isMethod('containsAll'));
        $this->assertTrue(Query::isMethod('elemMatch'));
        $this->assertTrue(Query::isMethod('regex'));
        $this->assertTrue(Query::isMethod('count'));
        $this->assertTrue(Query::isMethod('sum'));
        $this->assertTrue(Query::isMethod('avg'));
        $this->assertTrue(Query::isMethod('min'));
        $this->assertTrue(Query::isMethod('max'));
        $this->assertTrue(Query::isMethod('groupBy'));
        $this->assertTrue(Query::isMethod('having'));
        $this->assertTrue(Query::isMethod('distinct'));
        $this->assertTrue(Query::isMethod('join'));
        $this->assertTrue(Query::isMethod('leftJoin'));
        $this->assertTrue(Query::isMethod('rightJoin'));
        $this->assertTrue(Query::isMethod('crossJoin'));
        $this->assertTrue(Query::isMethod('union'));
        $this->assertTrue(Query::isMethod('unionAll'));
        $this->assertTrue(Query::isMethod('raw'));
    }

    public function testIsMethodInvalid(): void
    {
        $this->assertFalse(Query::isMethod('invalid'));
        $this->assertFalse(Query::isMethod(''));
        $this->assertFalse(Query::isMethod('EQUAL'));
        $this->assertFalse(Query::isMethod('foobar'));
    }

    public function testIsSpatialQueryTrue(): void
    {
        $this->assertTrue(Query::crosses('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::notCrosses('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::distanceEqual('geo', [0, 0], 10)->isSpatialQuery());
        $this->assertTrue(Query::distanceNotEqual('geo', [0, 0], 10)->isSpatialQuery());
        $this->assertTrue(Query::distanceGreaterThan('geo', [0, 0], 10)->isSpatialQuery());
        $this->assertTrue(Query::distanceLessThan('geo', [0, 0], 10)->isSpatialQuery());
        $this->assertTrue(Query::intersects('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::notIntersects('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::overlaps('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::notOverlaps('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::touches('geo', [[0, 0]])->isSpatialQuery());
        $this->assertTrue(Query::notTouches('geo', [[0, 0]])->isSpatialQuery());
    }

    public function testIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::equal('name', ['x'])->isSpatialQuery());
        $this->assertFalse(Query::limit(10)->isSpatialQuery());
        $this->assertFalse(Query::orderAsc('name')->isSpatialQuery());
    }

    public function testIsNestedTrue(): void
    {
        $this->assertTrue(Query::or([Query::equal('x', [1])])->isNested());
        $this->assertTrue(Query::and([Query::equal('x', [1])])->isNested());
        $this->assertTrue(Query::elemMatch('items', [Query::equal('x', [1])])->isNested());
    }

    public function testIsNestedFalse(): void
    {
        $this->assertFalse(Query::equal('x', [1])->isNested());
        $this->assertFalse(Query::limit(10)->isNested());
        $this->assertFalse(Query::orderAsc()->isNested());
    }

    public function testCloneDeepCopiesNestedQueries(): void
    {
        $inner = Query::equal('name', ['John']);
        $outer = Query::or([$inner]);
        $cloned = clone $outer;

        $clonedValues = $cloned->getValues();
        $this->assertInstanceOf(Query::class, $clonedValues[0]);
        $this->assertNotSame($inner, $clonedValues[0]);
        $this->assertEquals('equal', $clonedValues[0]->getMethod());
    }

    public function testClonePreservesNonQueryValues(): void
    {
        $query = Query::equal('name', ['John', 42, true]);
        $cloned = clone $query;
        $this->assertEquals(['John', 42, true], $cloned->getValues());
    }

    public function testGetByType(): void
    {
        $queries = [
            Query::equal('name', ['John']),
            Query::limit(10),
            Query::greaterThan('age', 18),
            Query::offset(5),
        ];

        $filters = Query::getByType($queries, [Query::TYPE_EQUAL, Query::TYPE_GREATER]);
        $this->assertCount(2, $filters);
        $this->assertEquals('equal', $filters[0]->getMethod());
        $this->assertEquals('greaterThan', $filters[1]->getMethod());
    }

    public function testGetByTypeClone(): void
    {
        $original = Query::equal('name', ['John']);
        $queries = [$original];

        $result = Query::getByType($queries, [Query::TYPE_EQUAL], true);
        $this->assertNotSame($original, $result[0]);
    }

    public function testGetByTypeNoClone(): void
    {
        $original = Query::equal('name', ['John']);
        $queries = [$original];

        $result = Query::getByType($queries, [Query::TYPE_EQUAL], false);
        $this->assertSame($original, $result[0]);
    }

    public function testGetByTypeEmpty(): void
    {
        $queries = [Query::equal('x', [1])];
        $result = Query::getByType($queries, [Query::TYPE_LIMIT]);
        $this->assertCount(0, $result);
    }

    public function testGetCursorQueries(): void
    {
        $queries = [
            Query::equal('name', ['John']),
            Query::cursorAfter('abc'),
            Query::limit(10),
            Query::cursorBefore('xyz'),
        ];

        $cursors = Query::getCursorQueries($queries);
        $this->assertCount(2, $cursors);
        $this->assertEquals(Query::TYPE_CURSOR_AFTER, $cursors[0]->getMethod());
        $this->assertEquals(Query::TYPE_CURSOR_BEFORE, $cursors[1]->getMethod());
    }

    public function testGetCursorQueriesNone(): void
    {
        $queries = [Query::equal('name', ['John']), Query::limit(10)];
        $cursors = Query::getCursorQueries($queries);
        $this->assertCount(0, $cursors);
    }

    public function testGroupByType(): void
    {
        $queries = [
            Query::equal('name', ['John']),
            Query::greaterThan('age', 18),
            Query::select(['name', 'email']),
            Query::limit(25),
            Query::offset(10),
            Query::orderAsc('name'),
            Query::orderDesc('age'),
            Query::cursorAfter('doc123'),
        ];

        $grouped = Query::groupByType($queries);

        $this->assertCount(2, $grouped['filters']);
        $this->assertEquals('equal', $grouped['filters'][0]->getMethod());
        $this->assertEquals('greaterThan', $grouped['filters'][1]->getMethod());

        $this->assertCount(1, $grouped['selections']);
        $this->assertEquals('select', $grouped['selections'][0]->getMethod());

        $this->assertEquals(25, $grouped['limit']);
        $this->assertEquals(10, $grouped['offset']);

        $this->assertEquals(['name', 'age'], $grouped['orderAttributes']);
        $this->assertEquals([Query::ORDER_ASC, Query::ORDER_DESC], $grouped['orderTypes']);

        $this->assertEquals('doc123', $grouped['cursor']);
        $this->assertEquals(Query::CURSOR_AFTER, $grouped['cursorDirection']);
    }

    public function testGroupByTypeFirstLimitWins(): void
    {
        $queries = [
            Query::limit(10),
            Query::limit(50),
        ];

        $grouped = Query::groupByType($queries);
        $this->assertEquals(10, $grouped['limit']);
    }

    public function testGroupByTypeFirstOffsetWins(): void
    {
        $queries = [
            Query::offset(5),
            Query::offset(100),
        ];

        $grouped = Query::groupByType($queries);
        $this->assertEquals(5, $grouped['offset']);
    }

    public function testGroupByTypeFirstCursorWins(): void
    {
        $queries = [
            Query::cursorAfter('first'),
            Query::cursorBefore('second'),
        ];

        $grouped = Query::groupByType($queries);
        $this->assertEquals('first', $grouped['cursor']);
        $this->assertEquals(Query::CURSOR_AFTER, $grouped['cursorDirection']);
    }

    public function testGroupByTypeCursorBefore(): void
    {
        $queries = [
            Query::cursorBefore('doc456'),
        ];

        $grouped = Query::groupByType($queries);
        $this->assertEquals('doc456', $grouped['cursor']);
        $this->assertEquals(Query::CURSOR_BEFORE, $grouped['cursorDirection']);
    }

    public function testGroupByTypeEmpty(): void
    {
        $grouped = Query::groupByType([]);
        $this->assertEquals([], $grouped['filters']);
        $this->assertEquals([], $grouped['selections']);
        $this->assertNull($grouped['limit']);
        $this->assertNull($grouped['offset']);
        $this->assertEquals([], $grouped['orderAttributes']);
        $this->assertEquals([], $grouped['orderTypes']);
        $this->assertNull($grouped['cursor']);
        $this->assertNull($grouped['cursorDirection']);
    }

    public function testGroupByTypeOrderRandom(): void
    {
        $queries = [Query::orderRandom()];
        $grouped = Query::groupByType($queries);
        $this->assertEquals([Query::ORDER_RANDOM], $grouped['orderTypes']);
        $this->assertEquals([], $grouped['orderAttributes']);
    }

    public function testGroupByTypeSkipsNonQueryInstances(): void
    {
        $grouped = Query::groupByType(['not a query', null, 42]);
        $this->assertEquals([], $grouped['filters']);
    }

    // ── groupByType with new types ──

    public function testGroupByTypeAggregations(): void
    {
        $queries = [
            Query::count('*', 'total'),
            Query::sum('price'),
            Query::avg('score'),
            Query::min('age'),
            Query::max('salary'),
        ];

        $grouped = Query::groupByType($queries);
        $this->assertCount(5, $grouped['aggregations']);
        $this->assertEquals(Query::TYPE_COUNT, $grouped['aggregations'][0]->getMethod());
        $this->assertEquals(Query::TYPE_MAX, $grouped['aggregations'][4]->getMethod());
    }

    public function testGroupByTypeGroupBy(): void
    {
        $queries = [Query::groupBy(['status', 'country'])];
        $grouped = Query::groupByType($queries);
        $this->assertEquals(['status', 'country'], $grouped['groupBy']);
    }

    public function testGroupByTypeHaving(): void
    {
        $queries = [Query::having([Query::greaterThan('total', 5)])];
        $grouped = Query::groupByType($queries);
        $this->assertCount(1, $grouped['having']);
        $this->assertEquals(Query::TYPE_HAVING, $grouped['having'][0]->getMethod());
    }

    public function testGroupByTypeDistinct(): void
    {
        $queries = [Query::distinct()];
        $grouped = Query::groupByType($queries);
        $this->assertTrue($grouped['distinct']);
    }

    public function testGroupByTypeDistinctDefaultFalse(): void
    {
        $grouped = Query::groupByType([]);
        $this->assertFalse($grouped['distinct']);
    }

    public function testGroupByTypeJoins(): void
    {
        $queries = [
            Query::join('orders', 'users.id', 'orders.user_id'),
            Query::leftJoin('profiles', 'users.id', 'profiles.user_id'),
            Query::crossJoin('colors'),
        ];
        $grouped = Query::groupByType($queries);
        $this->assertCount(3, $grouped['joins']);
        $this->assertEquals(Query::TYPE_JOIN, $grouped['joins'][0]->getMethod());
        $this->assertEquals(Query::TYPE_CROSS_JOIN, $grouped['joins'][2]->getMethod());
    }

    public function testGroupByTypeUnions(): void
    {
        $queries = [
            Query::union([Query::equal('x', [1])]),
            Query::unionAll([Query::equal('y', [2])]),
        ];
        $grouped = Query::groupByType($queries);
        $this->assertCount(2, $grouped['unions']);
    }

    // ── merge() ──

    public function testMergeConcatenates(): void
    {
        $a = [Query::equal('name', ['John'])];
        $b = [Query::greaterThan('age', 18)];

        $result = Query::merge($a, $b);
        $this->assertCount(2, $result);
        $this->assertEquals('equal', $result[0]->getMethod());
        $this->assertEquals('greaterThan', $result[1]->getMethod());
    }

    public function testMergeLimitOverrides(): void
    {
        $a = [Query::limit(10)];
        $b = [Query::limit(50)];

        $result = Query::merge($a, $b);
        $this->assertCount(1, $result);
        $this->assertEquals(50, $result[0]->getValue());
    }

    public function testMergeOffsetOverrides(): void
    {
        $a = [Query::offset(5), Query::equal('x', [1])];
        $b = [Query::offset(100)];

        $result = Query::merge($a, $b);
        $this->assertCount(2, $result);
        // equal stays, offset replaced
        $this->assertEquals('equal', $result[0]->getMethod());
        $this->assertEquals(100, $result[1]->getValue());
    }

    public function testMergeCursorOverrides(): void
    {
        $a = [Query::cursorAfter('abc')];
        $b = [Query::cursorAfter('xyz')];

        $result = Query::merge($a, $b);
        $this->assertCount(1, $result);
        $this->assertEquals('xyz', $result[0]->getValue());
    }

    // ── diff() ──

    public function testDiffReturnsUnique(): void
    {
        $shared = Query::equal('name', ['John']);
        $a = [$shared, Query::greaterThan('age', 18)];
        $b = [$shared];

        $result = Query::diff($a, $b);
        $this->assertCount(1, $result);
        $this->assertEquals('greaterThan', $result[0]->getMethod());
    }

    public function testDiffEmpty(): void
    {
        $q = Query::equal('x', [1]);
        $result = Query::diff([$q], [$q]);
        $this->assertCount(0, $result);
    }

    public function testDiffNoOverlap(): void
    {
        $a = [Query::equal('x', [1])];
        $b = [Query::equal('y', [2])];
        $result = Query::diff($a, $b);
        $this->assertCount(1, $result);
    }

    // ── validate() ──

    public function testValidatePassesAllowed(): void
    {
        $queries = [
            Query::equal('name', ['John']),
            Query::greaterThan('age', 18),
        ];
        $errors = Query::validate($queries, ['name', 'age']);
        $this->assertCount(0, $errors);
    }

    public function testValidateFailsInvalid(): void
    {
        $queries = [
            Query::equal('name', ['John']),
            Query::greaterThan('secret', 42),
        ];
        $errors = Query::validate($queries, ['name', 'age']);
        $this->assertCount(1, $errors);
        $this->assertStringContainsString('secret', $errors[0]);
    }

    public function testValidateSkipsNoAttribute(): void
    {
        $queries = [
            Query::limit(10),
            Query::offset(5),
            Query::distinct(),
            Query::orderRandom(),
        ];
        $errors = Query::validate($queries, []);
        $this->assertCount(0, $errors);
    }

    public function testValidateRecursesNested(): void
    {
        $queries = [
            Query::or([
                Query::equal('name', ['John']),
                Query::equal('invalid', ['x']),
            ]),
        ];
        $errors = Query::validate($queries, ['name']);
        $this->assertCount(1, $errors);
        $this->assertStringContainsString('invalid', $errors[0]);
    }

    public function testValidateGroupByColumns(): void
    {
        $queries = [Query::groupBy(['status', 'bad_col'])];
        $errors = Query::validate($queries, ['status']);
        $this->assertCount(1, $errors);
        $this->assertStringContainsString('bad_col', $errors[0]);
    }

    public function testValidateSkipsStar(): void
    {
        $queries = [Query::count()]; // attribute = '*'
        $errors = Query::validate($queries, []);
        $this->assertCount(0, $errors);
    }

    // ── page() static helper ──

    public function testPageStaticHelper(): void
    {
        $result = Query::page(3, 10);
        $this->assertCount(2, $result);
        $this->assertEquals(Query::TYPE_LIMIT, $result[0]->getMethod());
        $this->assertEquals(10, $result[0]->getValue());
        $this->assertEquals(Query::TYPE_OFFSET, $result[1]->getMethod());
        $this->assertEquals(20, $result[1]->getValue());
    }

    public function testPageStaticHelperFirstPage(): void
    {
        $result = Query::page(1);
        $this->assertEquals(25, $result[0]->getValue());
        $this->assertEquals(0, $result[1]->getValue());
    }
}
