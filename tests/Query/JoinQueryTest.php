<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\MySQL;
use Utopia\Query\Method;
use Utopia\Query\Query;

class JoinQueryTest extends TestCase
{
    public function testJoin(): void
    {
        $query = Query::join('orders', 'users.id', 'orders.user_id');
        $this->assertSame(Method::Join, $query->getMethod());
        $this->assertSame('orders', $query->getAttribute());
        $this->assertSame(['users.id', '=', 'orders.user_id'], $query->getValues());
    }

    public function testJoinWithOperator(): void
    {
        $query = Query::join('orders', 'users.id', 'orders.user_id', '!=');
        $this->assertSame(['users.id', '!=', 'orders.user_id'], $query->getValues());
    }

    public function testLeftJoin(): void
    {
        $query = Query::leftJoin('profiles', 'users.id', 'profiles.user_id');
        $this->assertSame(Method::LeftJoin, $query->getMethod());
        $this->assertSame('profiles', $query->getAttribute());
        $this->assertSame(['users.id', '=', 'profiles.user_id'], $query->getValues());
    }

    public function testRightJoin(): void
    {
        $query = Query::rightJoin('orders', 'users.id', 'orders.user_id');
        $this->assertSame(Method::RightJoin, $query->getMethod());
        $this->assertSame('orders', $query->getAttribute());
    }

    public function testCrossJoin(): void
    {
        $query = Query::crossJoin('colors');
        $this->assertSame(Method::CrossJoin, $query->getMethod());
        $this->assertSame('colors', $query->getAttribute());
        $this->assertSame([], $query->getValues());
    }

    public function testJoinMethodsAreJoin(): void
    {
        $this->assertTrue(Method::Join->isJoin());
        $this->assertTrue(Method::LeftJoin->isJoin());
        $this->assertTrue(Method::RightJoin->isJoin());
        $this->assertTrue(Method::CrossJoin->isJoin());
        $this->assertTrue(Method::FullOuterJoin->isJoin());
        $this->assertTrue(Method::NaturalJoin->isJoin());
        $joinMethods = array_filter(Method::cases(), fn (Method $m) => $m->isJoin());
        $this->assertCount(6, $joinMethods);
    }

    public function testJoinWithEmptyTableName(): void
    {
        $query = Query::join('', 'left', 'right');
        $this->assertSame('', $query->getAttribute());
        $this->assertSame(['left', '=', 'right'], $query->getValues());
    }

    public function testJoinWithEmptyLeftColumn(): void
    {
        $query = Query::join('t', '', 'right');
        $this->assertSame(['', '=', 'right'], $query->getValues());
    }

    public function testJoinWithEmptyRightColumn(): void
    {
        $query = Query::join('t', 'left', '');
        $this->assertSame(['left', '=', ''], $query->getValues());
    }

    public function testJoinWithSpecialOperators(): void
    {
        $ops = ['!=', '<>', '<', '>', '<=', '>='];
        foreach ($ops as $op) {
            $query = Query::join('t', 'a', 'b', $op);
            $this->assertSame(['a', $op, 'b'], $query->getValues());
        }
    }

    public function testLeftJoinValues(): void
    {
        $query = Query::leftJoin('t', 'a.id', 'b.aid', '!=');
        $this->assertSame(['a.id', '!=', 'b.aid'], $query->getValues());
    }

    public function testRightJoinValues(): void
    {
        $query = Query::rightJoin('t', 'a.id', 'b.aid');
        $this->assertSame(['a.id', '=', 'b.aid'], $query->getValues());
    }

    public function testCrossJoinEmptyTableName(): void
    {
        $query = Query::crossJoin('');
        $this->assertSame('', $query->getAttribute());
        $this->assertSame([], $query->getValues());
    }

    public function testJoinCompileDispatch(): void
    {
        $builder = new MySQL();
        $query = Query::join('orders', 'users.id', 'orders.uid');
        $sql = $query->compile($builder);
        $this->assertSame('JOIN `orders` ON `users`.`id` = `orders`.`uid`', $sql);
    }

    public function testLeftJoinCompileDispatch(): void
    {
        $builder = new MySQL();
        $query = Query::leftJoin('p', 'u.id', 'p.uid');
        $sql = $query->compile($builder);
        $this->assertSame('LEFT JOIN `p` ON `u`.`id` = `p`.`uid`', $sql);
    }

    public function testRightJoinCompileDispatch(): void
    {
        $builder = new MySQL();
        $query = Query::rightJoin('o', 'u.id', 'o.uid');
        $sql = $query->compile($builder);
        $this->assertSame('RIGHT JOIN `o` ON `u`.`id` = `o`.`uid`', $sql);
    }

    public function testCrossJoinCompileDispatch(): void
    {
        $builder = new MySQL();
        $query = Query::crossJoin('colors');
        $sql = $query->compile($builder);
        $this->assertSame('CROSS JOIN `colors`', $sql);
    }

    public function testJoinIsNotNested(): void
    {
        $query = Query::join('t', 'a', 'b');
        $this->assertFalse($query->isNested());
    }
}
