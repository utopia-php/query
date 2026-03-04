<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Condition;

class ConditionTest extends TestCase
{
    public function testGetExpression(): void
    {
        $condition = new Condition('status = ?', ['active']);
        $this->assertEquals('status = ?', $condition->getExpression());
    }

    public function testGetBindings(): void
    {
        $condition = new Condition('status = ?', ['active']);
        $this->assertEquals(['active'], $condition->getBindings());
    }

    public function testEmptyBindings(): void
    {
        $condition = new Condition('1 = 1');
        $this->assertEquals('1 = 1', $condition->getExpression());
        $this->assertEquals([], $condition->getBindings());
    }

    public function testMultipleBindings(): void
    {
        $condition = new Condition('age BETWEEN ? AND ?', [18, 65]);
        $this->assertEquals('age BETWEEN ? AND ?', $condition->getExpression());
        $this->assertEquals([18, 65], $condition->getBindings());
    }
}
