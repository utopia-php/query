<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class AggregationQueryTest extends TestCase
{
    public function testCountDefaultAttribute(): void
    {
        $query = Query::count();
        $this->assertEquals(Query::TYPE_COUNT, $query->getMethod());
        $this->assertEquals('*', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testCountWithAttribute(): void
    {
        $query = Query::count('id');
        $this->assertEquals(Query::TYPE_COUNT, $query->getMethod());
        $this->assertEquals('id', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testCountWithAlias(): void
    {
        $query = Query::count('*', 'total');
        $this->assertEquals('*', $query->getAttribute());
        $this->assertEquals(['total'], $query->getValues());
        $this->assertEquals('total', $query->getValue());
    }

    public function testSum(): void
    {
        $query = Query::sum('price');
        $this->assertEquals(Query::TYPE_SUM, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testSumWithAlias(): void
    {
        $query = Query::sum('price', 'total_price');
        $this->assertEquals(['total_price'], $query->getValues());
    }

    public function testAvg(): void
    {
        $query = Query::avg('score');
        $this->assertEquals(Query::TYPE_AVG, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
    }

    public function testMin(): void
    {
        $query = Query::min('price');
        $this->assertEquals(Query::TYPE_MIN, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
    }

    public function testMax(): void
    {
        $query = Query::max('price');
        $this->assertEquals(Query::TYPE_MAX, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
    }

    public function testGroupBy(): void
    {
        $query = Query::groupBy(['status', 'country']);
        $this->assertEquals(Query::TYPE_GROUP_BY, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(['status', 'country'], $query->getValues());
    }

    public function testHaving(): void
    {
        $inner = [
            Query::greaterThan('count', 5),
        ];
        $query = Query::having($inner);
        $this->assertEquals(Query::TYPE_HAVING, $query->getMethod());
        $this->assertCount(1, $query->getValues());
        $this->assertInstanceOf(Query::class, $query->getValues()[0]);
    }

    public function testAggregateTypesConstant(): void
    {
        $this->assertContains(Query::TYPE_COUNT, Query::AGGREGATE_TYPES);
        $this->assertContains(Query::TYPE_SUM, Query::AGGREGATE_TYPES);
        $this->assertContains(Query::TYPE_AVG, Query::AGGREGATE_TYPES);
        $this->assertContains(Query::TYPE_MIN, Query::AGGREGATE_TYPES);
        $this->assertContains(Query::TYPE_MAX, Query::AGGREGATE_TYPES);
        $this->assertCount(5, Query::AGGREGATE_TYPES);
    }
}
