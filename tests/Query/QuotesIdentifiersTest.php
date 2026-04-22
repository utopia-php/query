<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Tests\Query\Fixture\QuotesIdentifiersHarness;

final class QuotesIdentifiersTest extends TestCase
{
    private QuotesIdentifiersHarness $wrapper;

    protected function setUp(): void
    {
        $this->wrapper = new QuotesIdentifiersHarness();
    }

    public function testDotlessIdentifierIsWrapped(): void
    {
        $this->assertSame('`users`', $this->wrapper->quote('users'));
    }

    public function testBareStarIsPreserved(): void
    {
        $this->assertSame('*', $this->wrapper->quote('*'));
    }

    public function testTableStarWrapsTableAndKeepsStar(): void
    {
        $this->assertSame('`users`.*', $this->wrapper->quote('users.*'));
    }

    public function testDottedIdentifierIsWrappedSegmentwise(): void
    {
        $this->assertSame('`schema`.`users`', $this->wrapper->quote('schema.users'));
    }

    public function testWrapCharIsDoubledInsideIdentifier(): void
    {
        $this->assertSame('```weird```', $this->wrapper->quote('`weird`'));
    }

    public function testWrapCharIsDoubledInsideDottedSegment(): void
    {
        $this->assertSame('`schema`.```weird```', $this->wrapper->quote('schema.`weird`'));
    }

    public function testStarInNonFinalSegmentIsQuotedAsLiteral(): void
    {
        $this->assertSame('`foo`.`*`.`bar`', $this->wrapper->quote('foo.*.bar'));
    }

    public function testStarOnlyAllowedBareInFinalSegment(): void
    {
        $this->assertSame('`a`.`b`.*', $this->wrapper->quote('a.b.*'));
    }
}
