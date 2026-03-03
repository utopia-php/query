<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class SelectionQueryTest extends TestCase
{
    public function testSelect(): void
    {
        $query = Query::select(['name', 'email']);
        $this->assertEquals(Query::TYPE_SELECT, $query->getMethod());
        $this->assertEquals(['name', 'email'], $query->getValues());
    }

    public function testOrderAsc(): void
    {
        $query = Query::orderAsc('name');
        $this->assertEquals(Query::TYPE_ORDER_ASC, $query->getMethod());
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
        $this->assertEquals(Query::TYPE_ORDER_DESC, $query->getMethod());
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
        $this->assertEquals(Query::TYPE_ORDER_RANDOM, $query->getMethod());
    }

    public function testLimit(): void
    {
        $query = Query::limit(25);
        $this->assertEquals(Query::TYPE_LIMIT, $query->getMethod());
        $this->assertEquals([25], $query->getValues());
    }

    public function testOffset(): void
    {
        $query = Query::offset(10);
        $this->assertEquals(Query::TYPE_OFFSET, $query->getMethod());
        $this->assertEquals([10], $query->getValues());
    }

    public function testCursorAfter(): void
    {
        $query = Query::cursorAfter('doc123');
        $this->assertEquals(Query::TYPE_CURSOR_AFTER, $query->getMethod());
        $this->assertEquals(['doc123'], $query->getValues());
    }

    public function testCursorBefore(): void
    {
        $query = Query::cursorBefore('doc123');
        $this->assertEquals(Query::TYPE_CURSOR_BEFORE, $query->getMethod());
        $this->assertEquals(['doc123'], $query->getValues());
    }
}
