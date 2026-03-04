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
}
