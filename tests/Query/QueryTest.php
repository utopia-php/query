<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class QueryTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $query = new Query('equal');
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testConstructorWithAllParams(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals(['John'], $query->getValues());
    }

    public function testConstructorOrderAscDefaultAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_ASC);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderDescDefaultAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_DESC);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderAscWithAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_ASC, 'name');
        $this->assertEquals('name', $query->getAttribute());
    }

    public function testGetValue(): void
    {
        $query = new Query('equal', 'name', ['John', 'Jane']);
        $this->assertEquals('John', $query->getValue());
    }

    public function testGetValueDefault(): void
    {
        $query = new Query('equal', 'name');
        $this->assertEquals('fallback', $query->getValue('fallback'));
    }

    public function testGetValueDefaultNull(): void
    {
        $query = new Query('equal', 'name');
        $this->assertNull($query->getValue());
    }

    public function testSetMethod(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setMethod('notEqual');
        $this->assertEquals('notEqual', $query->getMethod());
        $this->assertSame($query, $result);
    }

    public function testSetAttribute(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setAttribute('age');
        $this->assertEquals('age', $query->getAttribute());
        $this->assertSame($query, $result);
    }

    public function testSetValues(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setValues(['Jane', 'Doe']);
        $this->assertEquals(['Jane', 'Doe'], $query->getValues());
        $this->assertSame($query, $result);
    }

    public function testSetValue(): void
    {
        $query = new Query('equal', 'name', ['John', 'Jane']);
        $result = $query->setValue('Only');
        $this->assertEquals(['Only'], $query->getValues());
        $this->assertSame($query, $result);
    }

    public function testSetAttributeType(): void
    {
        $query = new Query('equal', 'name');
        $query->setAttributeType('string');
        $this->assertEquals('string', $query->getAttributeType());
    }

    public function testOnArray(): void
    {
        $query = new Query('equal', 'tags', [['a', 'b']]);
        $this->assertFalse($query->onArray());
        $query->setOnArray(true);
        $this->assertTrue($query->onArray());
    }

    public function testConstants(): void
    {
        $this->assertEquals('ASC', Query::ORDER_ASC);
        $this->assertEquals('DESC', Query::ORDER_DESC);
        $this->assertEquals('RANDOM', Query::ORDER_RANDOM);
        $this->assertEquals('after', Query::CURSOR_AFTER);
        $this->assertEquals('before', Query::CURSOR_BEFORE);
    }

    public function testVectorTypesConstant(): void
    {
        $this->assertContains(Query::TYPE_VECTOR_DOT, Query::VECTOR_TYPES);
        $this->assertContains(Query::TYPE_VECTOR_COSINE, Query::VECTOR_TYPES);
        $this->assertContains(Query::TYPE_VECTOR_EUCLIDEAN, Query::VECTOR_TYPES);
        $this->assertCount(3, Query::VECTOR_TYPES);
    }

    public function testTypesConstantContainsAll(): void
    {
        $this->assertContains(Query::TYPE_EQUAL, Query::TYPES);
        $this->assertContains(Query::TYPE_REGEX, Query::TYPES);
        $this->assertContains(Query::TYPE_AND, Query::TYPES);
        $this->assertContains(Query::TYPE_OR, Query::TYPES);
        $this->assertContains(Query::TYPE_ELEM_MATCH, Query::TYPES);
        $this->assertContains(Query::TYPE_VECTOR_DOT, Query::TYPES);
    }

    public function testEmptyValues(): void
    {
        $query = Query::equal('name', []);
        $this->assertEquals([], $query->getValues());
    }

    public function testTypesConstantContainsNewTypes(): void
    {
        $this->assertContains(Query::TYPE_COUNT, Query::TYPES);
        $this->assertContains(Query::TYPE_SUM, Query::TYPES);
        $this->assertContains(Query::TYPE_AVG, Query::TYPES);
        $this->assertContains(Query::TYPE_MIN, Query::TYPES);
        $this->assertContains(Query::TYPE_MAX, Query::TYPES);
        $this->assertContains(Query::TYPE_GROUP_BY, Query::TYPES);
        $this->assertContains(Query::TYPE_HAVING, Query::TYPES);
        $this->assertContains(Query::TYPE_DISTINCT, Query::TYPES);
        $this->assertContains(Query::TYPE_JOIN, Query::TYPES);
        $this->assertContains(Query::TYPE_LEFT_JOIN, Query::TYPES);
        $this->assertContains(Query::TYPE_RIGHT_JOIN, Query::TYPES);
        $this->assertContains(Query::TYPE_CROSS_JOIN, Query::TYPES);
        $this->assertContains(Query::TYPE_UNION, Query::TYPES);
        $this->assertContains(Query::TYPE_UNION_ALL, Query::TYPES);
        $this->assertContains(Query::TYPE_RAW, Query::TYPES);
    }

    public function testIsMethodNewTypes(): void
    {
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

    public function testDistinctFactory(): void
    {
        $query = Query::distinct();
        $this->assertEquals(Query::TYPE_DISTINCT, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactory(): void
    {
        $query = Query::raw('score > ?', [10]);
        $this->assertEquals(Query::TYPE_RAW, $query->getMethod());
        $this->assertEquals('score > ?', $query->getAttribute());
        $this->assertEquals([10], $query->getValues());
    }

    public function testUnionFactory(): void
    {
        $inner = [Query::equal('x', [1])];
        $query = Query::union($inner);
        $this->assertEquals(Query::TYPE_UNION, $query->getMethod());
        $this->assertCount(1, $query->getValues());
    }

    public function testUnionAllFactory(): void
    {
        $inner = [Query::equal('x', [1])];
        $query = Query::unionAll($inner);
        $this->assertEquals(Query::TYPE_UNION_ALL, $query->getMethod());
    }

    // ══════════════════════════════════════════
    //  ADDITIONAL EDGE CASES
    // ══════════════════════════════════════════

    public function testTypesNoDuplicates(): void
    {
        $this->assertEquals(count(Query::TYPES), count(array_unique(Query::TYPES)));
    }

    public function testAggregateTypesNoDuplicates(): void
    {
        $this->assertEquals(count(Query::AGGREGATE_TYPES), count(array_unique(Query::AGGREGATE_TYPES)));
    }

    public function testJoinTypesNoDuplicates(): void
    {
        $this->assertEquals(count(Query::JOIN_TYPES), count(array_unique(Query::JOIN_TYPES)));
    }

    public function testAggregateTypesSubsetOfTypes(): void
    {
        foreach (Query::AGGREGATE_TYPES as $type) {
            $this->assertContains($type, Query::TYPES);
        }
    }

    public function testJoinTypesSubsetOfTypes(): void
    {
        foreach (Query::JOIN_TYPES as $type) {
            $this->assertContains($type, Query::TYPES);
        }
    }

    public function testIsMethodCaseSensitive(): void
    {
        $this->assertFalse(Query::isMethod('COUNT'));
        $this->assertFalse(Query::isMethod('Sum'));
        $this->assertFalse(Query::isMethod('JOIN'));
        $this->assertFalse(Query::isMethod('DISTINCT'));
        $this->assertFalse(Query::isMethod('GroupBy'));
        $this->assertFalse(Query::isMethod('RAW'));
    }

    public function testRawFactoryEmptySql(): void
    {
        $query = Query::raw('');
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactoryEmptyBindings(): void
    {
        $query = Query::raw('1 = 1', []);
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactoryMixedBindings(): void
    {
        $query = Query::raw('a = ? AND b = ? AND c = ?', ['str', 42, 3.14]);
        $this->assertEquals(['str', 42, 3.14], $query->getValues());
    }

    public function testUnionIsNested(): void
    {
        $query = Query::union([Query::equal('x', [1])]);
        $this->assertTrue($query->isNested());
    }

    public function testUnionAllIsNested(): void
    {
        $query = Query::unionAll([Query::equal('x', [1])]);
        $this->assertTrue($query->isNested());
    }

    public function testDistinctNotNested(): void
    {
        $this->assertFalse(Query::distinct()->isNested());
    }

    public function testCountNotNested(): void
    {
        $this->assertFalse(Query::count()->isNested());
    }

    public function testGroupByNotNested(): void
    {
        $this->assertFalse(Query::groupBy(['a'])->isNested());
    }

    public function testJoinNotNested(): void
    {
        $this->assertFalse(Query::join('t', 'a', 'b')->isNested());
    }

    public function testRawNotNested(): void
    {
        $this->assertFalse(Query::raw('1=1')->isNested());
    }

    public function testHavingNested(): void
    {
        $this->assertTrue(Query::having([Query::equal('x', [1])])->isNested());
    }

    public function testCloneDeepCopiesHavingQueries(): void
    {
        $inner = Query::greaterThan('total', 5);
        $outer = Query::having([$inner]);
        $cloned = clone $outer;

        $clonedValues = $cloned->getValues();
        $this->assertNotSame($inner, $clonedValues[0]);
        $this->assertInstanceOf(Query::class, $clonedValues[0]);

        /** @var Query $clonedInner */
        $clonedInner = $clonedValues[0];
        $this->assertEquals('greaterThan', $clonedInner->getMethod());
    }

    public function testCloneDeepCopiesUnionQueries(): void
    {
        $inner = Query::equal('x', [1]);
        $outer = Query::union([$inner]);
        $cloned = clone $outer;

        $clonedValues = $cloned->getValues();
        $this->assertNotSame($inner, $clonedValues[0]);
    }

    public function testCountConstantValue(): void
    {
        $this->assertEquals('count', Query::TYPE_COUNT);
    }

    public function testSumConstantValue(): void
    {
        $this->assertEquals('sum', Query::TYPE_SUM);
    }

    public function testAvgConstantValue(): void
    {
        $this->assertEquals('avg', Query::TYPE_AVG);
    }

    public function testMinConstantValue(): void
    {
        $this->assertEquals('min', Query::TYPE_MIN);
    }

    public function testMaxConstantValue(): void
    {
        $this->assertEquals('max', Query::TYPE_MAX);
    }

    public function testGroupByConstantValue(): void
    {
        $this->assertEquals('groupBy', Query::TYPE_GROUP_BY);
    }

    public function testHavingConstantValue(): void
    {
        $this->assertEquals('having', Query::TYPE_HAVING);
    }

    public function testDistinctConstantValue(): void
    {
        $this->assertEquals('distinct', Query::TYPE_DISTINCT);
    }

    public function testJoinConstantValue(): void
    {
        $this->assertEquals('join', Query::TYPE_JOIN);
    }

    public function testLeftJoinConstantValue(): void
    {
        $this->assertEquals('leftJoin', Query::TYPE_LEFT_JOIN);
    }

    public function testRightJoinConstantValue(): void
    {
        $this->assertEquals('rightJoin', Query::TYPE_RIGHT_JOIN);
    }

    public function testCrossJoinConstantValue(): void
    {
        $this->assertEquals('crossJoin', Query::TYPE_CROSS_JOIN);
    }

    public function testUnionConstantValue(): void
    {
        $this->assertEquals('union', Query::TYPE_UNION);
    }

    public function testUnionAllConstantValue(): void
    {
        $this->assertEquals('unionAll', Query::TYPE_UNION_ALL);
    }

    public function testRawConstantValue(): void
    {
        $this->assertEquals('raw', Query::TYPE_RAW);
    }

    public function testCountIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::count()->isSpatialQuery());
    }

    public function testJoinIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::join('t', 'a', 'b')->isSpatialQuery());
    }

    public function testDistinctIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::distinct()->isSpatialQuery());
    }
}
