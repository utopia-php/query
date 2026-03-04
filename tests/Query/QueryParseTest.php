<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Exception;
use Utopia\Query\Query;

class QueryParseTest extends TestCase
{
    public function testParseValidJson(): void
    {
        $json = '{"method":"equal","attribute":"name","values":["John"]}';
        $query = Query::parse($json);
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals(['John'], $query->getValues());
    }

    public function testParseInvalidJson(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query');
        Query::parse('not json');
    }

    public function testParseInvalidMethod(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query method: foobar');
        Query::parse('{"method":"foobar","attribute":"x","values":[]}');
    }

    public function testParseInvalidMethodType(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query method. Must be a string');
        Query::parse('{"method":123,"attribute":"x","values":[]}');
    }

    public function testParseInvalidAttribute(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query attribute. Must be a string');
        Query::parse('{"method":"equal","attribute":123,"values":["x"]}');
    }

    public function testParseInvalidValues(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query values. Must be an array');
        Query::parse('{"method":"equal","attribute":"x","values":"bad"}');
    }

    public function testParseWithDefaultValues(): void
    {
        $json = '{"method":"isNull"}';
        $query = Query::parse($json);
        $this->assertEquals('isNull', $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testParseQueryFromArray(): void
    {
        $query = Query::parseQuery([
            'method' => 'equal',
            'attribute' => 'name',
            'values' => ['John'],
        ]);
        $this->assertEquals('equal', $query->getMethod());
    }

    public function testParseNestedLogicalQuery(): void
    {
        $json = (string) json_encode([
            'method' => 'or',
            'attribute' => '',
            'values' => [
                ['method' => 'equal', 'attribute' => 'name', 'values' => ['John']],
                ['method' => 'equal', 'attribute' => 'name', 'values' => ['Jane']],
            ],
        ]);

        $query = Query::parse($json);
        $this->assertEquals(Query::TYPE_OR, $query->getMethod());
        $this->assertCount(2, $query->getValues());
        $this->assertInstanceOf(Query::class, $query->getValues()[0]);
        $this->assertEquals('John', $query->getValues()[0]->getValue());
    }

    public function testParseQueries(): void
    {
        $queries = Query::parseQueries([
            '{"method":"equal","attribute":"name","values":["John"]}',
            '{"method":"limit","values":[25]}',
        ]);
        $this->assertCount(2, $queries);
        $this->assertEquals('equal', $queries[0]->getMethod());
        $this->assertEquals('limit', $queries[1]->getMethod());
    }

    public function testToArray(): void
    {
        $query = Query::equal('name', ['John']);
        $array = $query->toArray();
        $this->assertEquals([
            'method' => 'equal',
            'attribute' => 'name',
            'values' => ['John'],
        ], $array);
    }

    public function testToArrayEmptyAttribute(): void
    {
        $query = Query::limit(25);
        $array = $query->toArray();
        $this->assertArrayNotHasKey('attribute', $array);
        $this->assertEquals(['method' => 'limit', 'values' => [25]], $array);
    }

    public function testToArrayNested(): void
    {
        $query = Query::or([
            Query::equal('name', ['John']),
            Query::greaterThan('age', 18),
        ]);
        $array = $query->toArray();
        $this->assertEquals('or', $array['method']);

        $values = $array['values'] ?? [];
        $this->assertIsArray($values);
        $this->assertCount(2, $values);
        $this->assertIsArray($values[0]);
        $this->assertIsArray($values[1]);
        $this->assertEquals('equal', $values[0]['method']);
        $this->assertEquals('greaterThan', $values[1]['method']);
    }

    public function testToString(): void
    {
        $query = Query::equal('name', ['John']);
        $string = $query->toString();

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($string, true);
        $this->assertEquals('equal', $decoded['method']);
        $this->assertEquals('name', $decoded['attribute']);
        $this->assertEquals(['John'], $decoded['values']);
    }

    public function testToStringNested(): void
    {
        $query = Query::and([
            Query::equal('x', [1]),
        ]);
        $string = $query->toString();

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($string, true);
        $this->assertEquals('and', $decoded['method']);

        $values = $decoded['values'] ?? [];
        $this->assertIsArray($values);
        $this->assertCount(1, $values);
    }

    public function testRoundTripParseSerialization(): void
    {
        $original = Query::equal('name', ['John']);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals($original->getMethod(), $parsed->getMethod());
        $this->assertEquals($original->getAttribute(), $parsed->getAttribute());
        $this->assertEquals($original->getValues(), $parsed->getValues());
    }

    public function testRoundTripNestedParseSerialization(): void
    {
        $original = Query::or([
            Query::equal('name', ['John']),
            Query::greaterThan('age', 18),
        ]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals($original->getMethod(), $parsed->getMethod());
        $this->assertCount(2, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }

    // ── Round-trip tests for new types ──

    public function testRoundTripCount(): void
    {
        $original = Query::count('id', 'total');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('count', $parsed->getMethod());
        $this->assertEquals('id', $parsed->getAttribute());
        $this->assertEquals(['total'], $parsed->getValues());
    }

    public function testRoundTripSum(): void
    {
        $original = Query::sum('price');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('sum', $parsed->getMethod());
        $this->assertEquals('price', $parsed->getAttribute());
    }

    public function testRoundTripGroupBy(): void
    {
        $original = Query::groupBy(['status', 'country']);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('groupBy', $parsed->getMethod());
        $this->assertEquals(['status', 'country'], $parsed->getValues());
    }

    public function testRoundTripHaving(): void
    {
        $original = Query::having([Query::greaterThan('total', 5)]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('having', $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }

    public function testRoundTripDistinct(): void
    {
        $original = Query::distinct();
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('distinct', $parsed->getMethod());
    }

    public function testRoundTripJoin(): void
    {
        $original = Query::join('orders', 'users.id', 'orders.user_id');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('join', $parsed->getMethod());
        $this->assertEquals('orders', $parsed->getAttribute());
        $this->assertEquals(['users.id', '=', 'orders.user_id'], $parsed->getValues());
    }

    public function testRoundTripCrossJoin(): void
    {
        $original = Query::crossJoin('colors');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('crossJoin', $parsed->getMethod());
        $this->assertEquals('colors', $parsed->getAttribute());
    }

    public function testRoundTripRaw(): void
    {
        $original = Query::raw('score > ?', [10]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('raw', $parsed->getMethod());
        $this->assertEquals('score > ?', $parsed->getAttribute());
        $this->assertEquals([10], $parsed->getValues());
    }

    public function testRoundTripUnion(): void
    {
        $original = Query::union([Query::equal('x', [1])]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals('union', $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }
}
