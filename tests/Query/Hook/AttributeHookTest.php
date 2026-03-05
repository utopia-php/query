<?php

namespace Tests\Query\Hook;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Hook\AttributeMapHook;

class AttributeHookTest extends TestCase
{
    public function testMappedAttribute(): void
    {
        $hook = new AttributeMapHook([
            '$id' => '_uid',
            '$createdAt' => '_createdAt',
        ]);

        $this->assertEquals('_uid', $hook->resolve('$id'));
        $this->assertEquals('_createdAt', $hook->resolve('$createdAt'));
    }

    public function testUnmappedPassthrough(): void
    {
        $hook = new AttributeMapHook(['$id' => '_uid']);

        $this->assertEquals('name', $hook->resolve('name'));
        $this->assertEquals('status', $hook->resolve('status'));
    }

    public function testEmptyMap(): void
    {
        $hook = new AttributeMapHook([]);

        $this->assertEquals('anything', $hook->resolve('anything'));
    }
}
