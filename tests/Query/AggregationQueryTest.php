<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Method;
use Utopia\Query\Query;

class AggregationQueryTest extends TestCase
{
    public function testCountDefaultAttribute(): void
    {
        $query = Query::count();
        $this->assertSame(Method::Count, $query->getMethod());
        $this->assertEquals('*', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testCountWithAttribute(): void
    {
        $query = Query::count('id');
        $this->assertSame(Method::Count, $query->getMethod());
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
        $this->assertSame(Method::Sum, $query->getMethod());
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
        $this->assertSame(Method::Avg, $query->getMethod());
        $this->assertEquals('score', $query->getAttribute());
    }

    public function testMin(): void
    {
        $query = Query::min('price');
        $this->assertSame(Method::Min, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
    }

    public function testMax(): void
    {
        $query = Query::max('price');
        $this->assertSame(Method::Max, $query->getMethod());
        $this->assertEquals('price', $query->getAttribute());
    }

    public function testGroupBy(): void
    {
        $query = Query::groupBy(['status', 'country']);
        $this->assertSame(Method::GroupBy, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(['status', 'country'], $query->getValues());
    }

    public function testHaving(): void
    {
        $inner = [
            Query::greaterThan('count', 5),
        ];
        $query = Query::having($inner);
        $this->assertSame(Method::Having, $query->getMethod());
        $this->assertCount(1, $query->getValues());
        $this->assertInstanceOf(Query::class, $query->getValues()[0]);
    }

    public function testAggregateMethodsAreAggregate(): void
    {
        $this->assertTrue(Method::Count->isAggregate());
        $this->assertTrue(Method::Sum->isAggregate());
        $this->assertTrue(Method::Avg->isAggregate());
        $this->assertTrue(Method::Min->isAggregate());
        $this->assertTrue(Method::Max->isAggregate());
        $this->assertTrue(Method::CountDistinct->isAggregate());
        $aggMethods = array_filter(Method::cases(), fn (Method $m) => $m->isAggregate());
        $this->assertCount(6, $aggMethods);
    }

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
        $this->assertSame(Method::GroupBy, $query->getMethod());
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
        $this->assertSame(Method::Having, $query->getMethod());
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
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::count('id');
        $sql = $query->compile($builder);
        $this->assertEquals('COUNT(`id`)', $sql);
    }

    public function testSumCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::sum('price', 'total');
        $sql = $query->compile($builder);
        $this->assertEquals('SUM(`price`) AS `total`', $sql);
    }

    public function testAvgCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::avg('score');
        $sql = $query->compile($builder);
        $this->assertEquals('AVG(`score`)', $sql);
    }

    public function testMinCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::min('price');
        $sql = $query->compile($builder);
        $this->assertEquals('MIN(`price`)', $sql);
    }

    public function testMaxCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::max('price');
        $sql = $query->compile($builder);
        $this->assertEquals('MAX(`price`)', $sql);
    }

    public function testGroupByCompileDispatch(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::groupBy(['status', 'country']);
        $sql = $query->compile($builder);
        $this->assertEquals('`status`, `country`', $sql);
    }

    public function testHavingCompileDispatchUsesCompileFilter(): void
    {
        $builder = new \Utopia\Query\Builder\MySQL();
        $query = Query::having([Query::greaterThan('total', 5)]);
        $sql = $query->compile($builder);
        $this->assertEquals('(`total` > ?)', $sql);
        $this->assertEquals([5], $builder->getBindings());
    }
}
