<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class LogicalQueryTest extends TestCase
{
    public function testOr(): void
    {
        $q1 = Query::equal('name', ['John']);
        $q2 = Query::equal('name', ['Jane']);
        $query = Query::or([$q1, $q2]);
        $this->assertEquals(Query::TYPE_OR, $query->getMethod());
        $this->assertCount(2, $query->getValues());
    }

    public function testAnd(): void
    {
        $q1 = Query::greaterThan('age', 18);
        $q2 = Query::lessThan('age', 65);
        $query = Query::and([$q1, $q2]);
        $this->assertEquals(Query::TYPE_AND, $query->getMethod());
        $this->assertCount(2, $query->getValues());
    }

    public function testContainsAll(): void
    {
        $query = Query::containsAll('tags', ['php', 'js']);
        $this->assertEquals(Query::TYPE_CONTAINS_ALL, $query->getMethod());
        $this->assertEquals(['php', 'js'], $query->getValues());
    }

    public function testElemMatch(): void
    {
        $inner = [Query::equal('field', ['val'])];
        $query = Query::elemMatch('items', $inner);
        $this->assertEquals(Query::TYPE_ELEM_MATCH, $query->getMethod());
        $this->assertEquals('items', $query->getAttribute());
    }
}
