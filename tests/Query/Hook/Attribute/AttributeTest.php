<?php

namespace Tests\Query\Hook\Attribute;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Hook\Attribute\Map;

class AttributeTest extends TestCase
{
    public function testMappedAttribute(): void
    {
        $hook = new Map([
            '$id' => '_uid',
            '$createdAt' => '_createdAt',
        ]);

        $this->assertEquals('_uid', $hook->resolve('$id'));
        $this->assertEquals('_createdAt', $hook->resolve('$createdAt'));
    }

    public function testUnmappedPassthrough(): void
    {
        $hook = new Map(['$id' => '_uid']);

        $this->assertEquals('name', $hook->resolve('name'));
        $this->assertEquals('status', $hook->resolve('status'));
    }

    public function testEmptyMap(): void
    {
        $hook = new Map([]);

        $this->assertEquals('anything', $hook->resolve('anything'));
    }
}
