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

    // ── Edge cases ──

    public function testCountWithEmptyStringAttribute(): void
    {
        $query = Query::count('');
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testSumWithEmptyAlias(): void
    {
        $query = Query::sum('price', '');
        $this->assertEquals([], $query->getValues());
    }

    public function testAvgWithAlias(): void
    {
        $query = Query::avg('score', 'avg_score');
        $this->assertEquals(['avg_score'], $query->getValues());
        $this->assertEquals('avg_score', $query->getValue());
    }

    public function testMinWithAlias(): void
    {
        $query = Query::min('price', 'min_price');
        $this->assertEquals(['min_price'], $query->getValues());
    }

    public function testMaxWithAlias(): void
    {
        $query = Query::max('price', 'max_price');
        $this->assertEquals(['max_price'], $query->getValues());
    }

    public function testGroupByEmpty(): void
    {
        $query = Query::groupBy([]);
        $this->assertEquals(Query::TYPE_GROUP_BY, $query->getMethod());
        $this->assertEquals([], $query->getValues());
    }

    public function testGroupBySingleColumn(): void
    {
        $query = Query::groupBy(['status']);
        $this->assertEquals(['status'], $query->getValues());
    }

    public function testGroupByManyColumns(): void
    {
        $cols = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];
        $query = Query::groupBy($cols);
        $this->assertCount(7, $query->getValues());
    }

    public function testGroupByDuplicateColumns(): void
    {
        $query = Query::groupBy(['status', 'status']);
        $this->assertEquals(['status', 'status'], $query->getValues());
    }

    public function testHavingEmpty(): void
    {
        $query = Query::having([]);
        $this->assertEquals(Query::TYPE_HAVING, $query->getMethod());
        $this->assertEquals([], $query->getValues());
    }

    public function testHavingMultipleConditions(): void
    {
        $inner = [
            Query::greaterThan('count', 5),
            Query::lessThan('total', 1000),
        ];
        $query = Query::having($inner);
        $this->assertCount(2, $query->getValues());
        $this->assertInstanceOf(Query::class, $query->getValues()[0]);
        $this->assertInstanceOf(Query::class, $query->getValues()[1]);
    }

    public function testHavingWithLogicalOr(): void
    {
        $inner = [
            Query::or([
                Query::greaterThan('count', 5),
                Query::lessThan('count', 1),
            ]),
        ];
        $query = Query::having($inner);
        $this->assertCount(1, $query->getValues());
    }

    public function testHavingIsNested(): void
    {
        $query = Query::having([Query::greaterThan('x', 1)]);
        $this->assertTrue($query->isNested());
    }

    public function testDistinctIsNotNested(): void
    {
        $query = Query::distinct();
        $this->assertFalse($query->isNested());
    }

    public function testCountCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::count('id');
        $sql = $query->compile($builder);
        $this->assertEquals('COUNT(`id`)', $sql);
    }

    public function testSumCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::sum('price', 'total');
        $sql = $query->compile($builder);
        $this->assertEquals('SUM(`price`) AS `total`', $sql);
    }

    public function testAvgCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::avg('score');
        $sql = $query->compile($builder);
        $this->assertEquals('AVG(`score`)', $sql);
    }

    public function testMinCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::min('price');
        $sql = $query->compile($builder);
        $this->assertEquals('MIN(`price`)', $sql);
    }

    public function testMaxCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::max('price');
        $sql = $query->compile($builder);
        $this->assertEquals('MAX(`price`)', $sql);
    }

    public function testGroupByCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::groupBy(['status', 'country']);
        $sql = $query->compile($builder);
        $this->assertEquals('`status`, `country`', $sql);
    }

    public function testHavingCompileDispatchUsesCompileFilter(): void
    {
        $builder = new \Utopia\Query\Builder\SQL();
        $query = Query::having([Query::greaterThan('total', 5)]);
        $sql = $query->compile($builder);
        $this->assertEquals('(`total` > ?)', $sql);
        $this->assertEquals([5], $builder->getBindings());
    }
}
