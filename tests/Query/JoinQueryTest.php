<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class JoinQueryTest extends TestCase
{
    public function testJoin(): void
    {
        $query = Query::join('orders', 'users.id', 'orders.user_id');
        $this->assertEquals(Query::TYPE_JOIN, $query->getMethod());
        $this->assertEquals('orders', $query->getAttribute());
        $this->assertEquals(['users.id', '=', 'orders.user_id'], $query->getValues());
    }

    public function testJoinWithOperator(): void
    {
        $query = Query::join('orders', 'users.id', 'orders.user_id', '!=');
        $this->assertEquals(['users.id', '!=', 'orders.user_id'], $query->getValues());
    }

    public function testLeftJoin(): void
    {
        $query = Query::leftJoin('profiles', 'users.id', 'profiles.user_id');
        $this->assertEquals(Query::TYPE_LEFT_JOIN, $query->getMethod());
        $this->assertEquals('profiles', $query->getAttribute());
        $this->assertEquals(['users.id', '=', 'profiles.user_id'], $query->getValues());
    }

    public function testRightJoin(): void
    {
        $query = Query::rightJoin('orders', 'users.id', 'orders.user_id');
        $this->assertEquals(Query::TYPE_RIGHT_JOIN, $query->getMethod());
        $this->assertEquals('orders', $query->getAttribute());
    }

    public function testCrossJoin(): void
    {
        $query = Query::crossJoin('colors');
        $this->assertEquals(Query::TYPE_CROSS_JOIN, $query->getMethod());
        $this->assertEquals('colors', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testJoinTypesConstant(): void
    {
        $this->assertContains(Query::TYPE_JOIN, Query::JOIN_TYPES);
        $this->assertContains(Query::TYPE_LEFT_JOIN, Query::JOIN_TYPES);
        $this->assertContains(Query::TYPE_RIGHT_JOIN, Query::JOIN_TYPES);
        $this->assertContains(Query::TYPE_CROSS_JOIN, Query::JOIN_TYPES);
        $this->assertCount(4, Query::JOIN_TYPES);
    }
}
