<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class FilterQueryTest extends TestCase
{
    public function testEqual(): void
    {
        $query = Query::equal('name', ['John', 'Jane']);
        $this->assertEquals(Query::TYPE_EQUAL, $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals(['John', 'Jane'], $query->getValues());
    }

    public function testNotEqual(): void
    {
        $query = Query::notEqual('name', 'John');
        $this->assertEquals(Query::TYPE_NOT_EQUAL, $query->getMethod());
        $this->assertEquals(['John'], $query->getValues());
    }

    public function testNotEqualWithList(): void
    {
        $query = Query::notEqual('name', ['John', 'Jane']);
        $this->assertEquals(['John', 'Jane'], $query->getValues());
    }

    public function testNotEqualWithMap(): void
    {
        $query = Query::notEqual('data', ['key' => 'value']);
        $this->assertEquals([['key' => 'value']], $query->getValues());
    }

    public function testLessThan(): void
    {
        $query = Query::lessThan('age', 30);
        $this->assertEquals(Query::TYPE_LESSER, $query->getMethod());
        $this->assertEquals('age', $query->getAttribute());
        $this->assertEquals([30], $query->getValues());
    }

    public function testLessThanEqual(): void
    {
        $query = Query::lessThanEqual('age', 30);
        $this->assertEquals(Query::TYPE_LESSER_EQUAL, $query->getMethod());
        $this->assertEquals([30], $query->getValues());
    }

    public function testGreaterThan(): void
    {
        $query = Query::greaterThan('age', 18);
        $this->assertEquals(Query::TYPE_GREATER, $query->getMethod());
        $this->assertEquals([18], $query->getValues());
    }

    public function testGreaterThanEqual(): void
    {
        $query = Query::greaterThanEqual('age', 18);
        $this->assertEquals(Query::TYPE_GREATER_EQUAL, $query->getMethod());
        $this->assertEquals([18], $query->getValues());
    }

    public function testContains(): void
    {
        $query = Query::contains('tags', ['php', 'js']);
        $this->assertEquals(Query::TYPE_CONTAINS, $query->getMethod());
        $this->assertEquals(['php', 'js'], $query->getValues());
    }

    public function testContainsAny(): void
    {
        $query = Query::containsAny('tags', ['php', 'js']);
        $this->assertEquals(Query::TYPE_CONTAINS_ANY, $query->getMethod());
        $this->assertEquals(['php', 'js'], $query->getValues());
    }

    public function testNotContains(): void
    {
        $query = Query::notContains('tags', ['php']);
        $this->assertEquals(Query::TYPE_NOT_CONTAINS, $query->getMethod());
        $this->assertEquals(['php'], $query->getValues());
    }

    public function testContainsDeprecated(): void
    {
        $query = Query::contains('tags', ['a', 'b']);
        $this->assertEquals(Query::TYPE_CONTAINS, $query->getMethod());
        $this->assertEquals(['a', 'b'], $query->getValues());
    }

    public function testBetween(): void
    {
        $query = Query::between('age', 18, 65);
        $this->assertEquals(Query::TYPE_BETWEEN, $query->getMethod());
        $this->assertEquals([18, 65], $query->getValues());
    }

    public function testNotBetween(): void
    {
        $query = Query::notBetween('age', 18, 65);
        $this->assertEquals(Query::TYPE_NOT_BETWEEN, $query->getMethod());
        $this->assertEquals([18, 65], $query->getValues());
    }

    public function testSearch(): void
    {
        $query = Query::search('content', 'hello world');
        $this->assertEquals(Query::TYPE_SEARCH, $query->getMethod());
        $this->assertEquals(['hello world'], $query->getValues());
    }

    public function testNotSearch(): void
    {
        $query = Query::notSearch('content', 'hello');
        $this->assertEquals(Query::TYPE_NOT_SEARCH, $query->getMethod());
        $this->assertEquals(['hello'], $query->getValues());
    }

    public function testIsNull(): void
    {
        $query = Query::isNull('email');
        $this->assertEquals(Query::TYPE_IS_NULL, $query->getMethod());
        $this->assertEquals('email', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testIsNotNull(): void
    {
        $query = Query::isNotNull('email');
        $this->assertEquals(Query::TYPE_IS_NOT_NULL, $query->getMethod());
    }

    public function testStartsWith(): void
    {
        $query = Query::startsWith('name', 'Jo');
        $this->assertEquals(Query::TYPE_STARTS_WITH, $query->getMethod());
        $this->assertEquals(['Jo'], $query->getValues());
    }

    public function testNotStartsWith(): void
    {
        $query = Query::notStartsWith('name', 'Jo');
        $this->assertEquals(Query::TYPE_NOT_STARTS_WITH, $query->getMethod());
    }

    public function testEndsWith(): void
    {
        $query = Query::endsWith('email', '.com');
        $this->assertEquals(Query::TYPE_ENDS_WITH, $query->getMethod());
        $this->assertEquals(['.com'], $query->getValues());
    }

    public function testNotEndsWith(): void
    {
        $query = Query::notEndsWith('email', '.com');
        $this->assertEquals(Query::TYPE_NOT_ENDS_WITH, $query->getMethod());
    }

    public function testRegex(): void
    {
        $query = Query::regex('name', '^Jo.*');
        $this->assertEquals(Query::TYPE_REGEX, $query->getMethod());
        $this->assertEquals(['^Jo.*'], $query->getValues());
    }

    public function testExists(): void
    {
        $query = Query::exists(['name', 'email']);
        $this->assertEquals(Query::TYPE_EXISTS, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals(['name', 'email'], $query->getValues());
    }

    public function testNotExistsArray(): void
    {
        $query = Query::notExists(['name']);
        $this->assertEquals(Query::TYPE_NOT_EXISTS, $query->getMethod());
        $this->assertEquals(['name'], $query->getValues());
    }

    public function testNotExistsScalar(): void
    {
        $query = Query::notExists('name');
        $this->assertEquals(['name'], $query->getValues());
    }

    public function testCreatedBefore(): void
    {
        $query = Query::createdBefore('2024-01-01');
        $this->assertEquals(Query::TYPE_LESSER, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2024-01-01'], $query->getValues());
    }

    public function testCreatedAfter(): void
    {
        $query = Query::createdAfter('2024-01-01');
        $this->assertEquals(Query::TYPE_GREATER, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
    }

    public function testUpdatedBefore(): void
    {
        $query = Query::updatedBefore('2024-06-01');
        $this->assertEquals(Query::TYPE_LESSER, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
    }

    public function testUpdatedAfter(): void
    {
        $query = Query::updatedAfter('2024-06-01');
        $this->assertEquals(Query::TYPE_GREATER, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
    }

    public function testCreatedBetween(): void
    {
        $query = Query::createdBetween('2024-01-01', '2024-12-31');
        $this->assertEquals(Query::TYPE_BETWEEN, $query->getMethod());
        $this->assertEquals('$createdAt', $query->getAttribute());
        $this->assertEquals(['2024-01-01', '2024-12-31'], $query->getValues());
    }

    public function testUpdatedBetween(): void
    {
        $query = Query::updatedBetween('2024-01-01', '2024-12-31');
        $this->assertEquals(Query::TYPE_BETWEEN, $query->getMethod());
        $this->assertEquals('$updatedAt', $query->getAttribute());
    }
}
