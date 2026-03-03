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
}
