<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Method;
use Utopia\Query\Query;

class SelectionQueryTest extends TestCase
{
    public function testSelect(): void
    {
        $query = Query::select(['name', 'email']);
        $this->assertSame(Method::Select, $query->getMethod());
        $this->assertEquals(['name', 'email'], $query->getValues());
    }

    public function testOrderAsc(): void
    {
        $query = Query::orderAsc('name');
        $this->assertSame(Method::OrderAsc, $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
    }

    public function testOrderAscNoAttribute(): void
    {
        $query = Query::orderAsc();
        $this->assertEquals('', $query->getAttribute());
    }

    public function testOrderDesc(): void
    {
        $query = Query::orderDesc('name');
        $this->assertSame(Method::OrderDesc, $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
    }

    public function testOrderDescNoAttribute(): void
    {
        $query = Query::orderDesc();
        $this->assertEquals('', $query->getAttribute());
    }

    public function testOrderRandom(): void
    {
        $query = Query::orderRandom();
        $this->assertSame(Method::OrderRandom, $query->getMethod());
    }

    public function testLimit(): void
    {
        $query = Query::limit(25);
        $this->assertSame(Method::Limit, $query->getMethod());
        $this->assertEquals([25], $query->getValues());
    }

    public function testOffset(): void
    {
        $query = Query::offset(10);
        $this->assertSame(Method::Offset, $query->getMethod());
        $this->assertEquals([10], $query->getValues());
    }

    public function testCursorAfter(): void
    {
        $query = Query::cursorAfter('doc123');
        $this->assertSame(Method::CursorAfter, $query->getMethod());
        $this->assertEquals(['doc123'], $query->getValues());
    }

    public function testCursorBefore(): void
    {
        $query = Query::cursorBefore('doc123');
        $this->assertSame(Method::CursorBefore, $query->getMethod());
        $this->assertEquals(['doc123'], $query->getValues());
    }
}
