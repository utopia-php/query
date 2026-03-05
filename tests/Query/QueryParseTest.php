<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Exception;
use Utopia\Query\Method;
use Utopia\Query\Query;

class QueryParseTest extends TestCase
{
    public function testParseValidJson(): void
    {
        $json = '{"method":"equal","attribute":"name","values":["John"]}';
        $query = Query::parse($json);
        $this->assertSame(Method::Equal, $query->getMethod());
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
        $this->assertSame(Method::IsNull, $query->getMethod());
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
        $this->assertSame(Method::Equal, $query->getMethod());
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
        $this->assertSame(Method::Or, $query->getMethod());
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
        $this->assertSame(Method::Equal, $queries[0]->getMethod());
        $this->assertSame(Method::Limit, $queries[1]->getMethod());
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
        $this->assertSame(Method::Count, $parsed->getMethod());
        $this->assertEquals('id', $parsed->getAttribute());
        $this->assertEquals(['total'], $parsed->getValues());
    }

    public function testRoundTripSum(): void
    {
        $original = Query::sum('price');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Sum, $parsed->getMethod());
        $this->assertEquals('price', $parsed->getAttribute());
    }

    public function testRoundTripGroupBy(): void
    {
        $original = Query::groupBy(['status', 'country']);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::GroupBy, $parsed->getMethod());
        $this->assertEquals(['status', 'country'], $parsed->getValues());
    }

    public function testRoundTripHaving(): void
    {
        $original = Query::having([Query::greaterThan('total', 5)]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Having, $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }

    public function testRoundTripDistinct(): void
    {
        $original = Query::distinct();
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Distinct, $parsed->getMethod());
    }

    public function testRoundTripJoin(): void
    {
        $original = Query::join('orders', 'users.id', 'orders.user_id');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Join, $parsed->getMethod());
        $this->assertEquals('orders', $parsed->getAttribute());
        $this->assertEquals(['users.id', '=', 'orders.user_id'], $parsed->getValues());
    }

    public function testRoundTripCrossJoin(): void
    {
        $original = Query::crossJoin('colors');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::CrossJoin, $parsed->getMethod());
        $this->assertEquals('colors', $parsed->getAttribute());
    }

    public function testRoundTripRaw(): void
    {
        $original = Query::raw('score > ?', [10]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Raw, $parsed->getMethod());
        $this->assertEquals('score > ?', $parsed->getAttribute());
        $this->assertEquals([10], $parsed->getValues());
    }

    public function testRoundTripUnion(): void
    {
        $original = Query::union([Query::equal('x', [1])]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Union, $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }

    // ══════════════════════════════════════════
    //  ADDITIONAL EDGE CASES
    // ══════════════════════════════════════════

    // ── Round-trip additional ──

    public function testRoundTripAvg(): void
    {
        $original = Query::avg('score', 'avg_score');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Avg, $parsed->getMethod());
        $this->assertEquals('score', $parsed->getAttribute());
        $this->assertEquals(['avg_score'], $parsed->getValues());
    }

    public function testRoundTripMin(): void
    {
        $original = Query::min('price');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Min, $parsed->getMethod());
        $this->assertEquals('price', $parsed->getAttribute());
        $this->assertEquals([], $parsed->getValues());
    }

    public function testRoundTripMax(): void
    {
        $original = Query::max('age', 'oldest');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Max, $parsed->getMethod());
        $this->assertEquals(['oldest'], $parsed->getValues());
    }

    public function testRoundTripCountWithoutAlias(): void
    {
        $original = Query::count('id');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Count, $parsed->getMethod());
        $this->assertEquals('id', $parsed->getAttribute());
        $this->assertEquals([], $parsed->getValues());
    }

    public function testRoundTripGroupByEmpty(): void
    {
        $original = Query::groupBy([]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::GroupBy, $parsed->getMethod());
        $this->assertEquals([], $parsed->getValues());
    }

    public function testRoundTripHavingMultiple(): void
    {
        $original = Query::having([
            Query::greaterThan('total', 5),
            Query::lessThan('total', 100),
        ]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertCount(2, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
        $this->assertInstanceOf(Query::class, $parsed->getValues()[1]);
    }

    public function testRoundTripLeftJoin(): void
    {
        $original = Query::leftJoin('profiles', 'u.id', 'p.uid');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::LeftJoin, $parsed->getMethod());
        $this->assertEquals('profiles', $parsed->getAttribute());
        $this->assertEquals(['u.id', '=', 'p.uid'], $parsed->getValues());
    }

    public function testRoundTripRightJoin(): void
    {
        $original = Query::rightJoin('orders', 'u.id', 'o.uid');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::RightJoin, $parsed->getMethod());
    }

    public function testRoundTripJoinWithSpecialOperator(): void
    {
        $original = Query::join('t', 'a.val', 'b.val', '!=');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals(['a.val', '!=', 'b.val'], $parsed->getValues());
    }

    public function testRoundTripUnionAll(): void
    {
        $original = Query::unionAll([Query::equal('y', [2])]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::UnionAll, $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());
        $this->assertInstanceOf(Query::class, $parsed->getValues()[0]);
    }

    public function testRoundTripRawNoBindings(): void
    {
        $original = Query::raw('1 = 1');
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Raw, $parsed->getMethod());
        $this->assertEquals('1 = 1', $parsed->getAttribute());
        $this->assertEquals([], $parsed->getValues());
    }

    public function testRoundTripRawWithMultipleBindings(): void
    {
        $original = Query::raw('a > ? AND b < ?', [10, 20]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertEquals([10, 20], $parsed->getValues());
    }

    public function testRoundTripComplexNested(): void
    {
        $original = Query::or([
            Query::and([
                Query::equal('a', [1]),
                Query::or([
                    Query::equal('b', [2]),
                    Query::equal('c', [3]),
                ]),
            ]),
        ]);
        $json = $original->toString();
        $parsed = Query::parse($json);
        $this->assertSame(Method::Or, $parsed->getMethod());
        $this->assertCount(1, $parsed->getValues());

        /** @var Query $inner */
        $inner = $parsed->getValues()[0];
        $this->assertSame(Method::And, $inner->getMethod());
        $this->assertCount(2, $inner->getValues());
    }

    // ── Parse edge cases ──

    public function testParseEmptyStringThrows(): void
    {
        $this->expectException(Exception::class);
        Query::parse('');
    }

    public function testParseWhitespaceThrows(): void
    {
        $this->expectException(Exception::class);
        Query::parse('   ');
    }

    public function testParseMissingMethodUsesEmptyString(): void
    {
        // method defaults to '' which is not a valid method
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid query method: ');
        Query::parse('{"attribute":"x","values":[]}');
    }

    public function testParseMissingAttributeDefaultsToEmpty(): void
    {
        $query = Query::parse('{"method":"isNull","values":[]}');
        $this->assertEquals('', $query->getAttribute());
    }

    public function testParseMissingValuesDefaultsToEmpty(): void
    {
        $query = Query::parse('{"method":"isNull"}');
        $this->assertEquals([], $query->getValues());
    }

    public function testParseExtraFieldsIgnored(): void
    {
        $query = Query::parse('{"method":"equal","attribute":"x","values":[1],"extra":"ignored"}');
        $this->assertSame(Method::Equal, $query->getMethod());
        $this->assertEquals('x', $query->getAttribute());
    }

    public function testParseNonObjectJsonThrows(): void
    {
        $this->expectException(Exception::class);
        Query::parse('"just a string"');
    }

    public function testParseJsonArrayThrows(): void
    {
        $this->expectException(Exception::class);
        Query::parse('[1,2,3]');
    }

    // ── toArray edge cases ──

    public function testToArrayCountWithAlias(): void
    {
        $query = Query::count('id', 'total');
        $array = $query->toArray();
        $this->assertEquals('count', $array['method']);
        $this->assertEquals('id', $array['attribute']);
        $this->assertEquals(['total'], $array['values']);
    }

    public function testToArrayCountWithoutAlias(): void
    {
        $query = Query::count();
        $array = $query->toArray();
        $this->assertEquals('count', $array['method']);
        $this->assertEquals('*', $array['attribute']);
        $this->assertEquals([], $array['values']);
    }

    public function testToArrayDistinct(): void
    {
        $query = Query::distinct();
        $array = $query->toArray();
        $this->assertEquals('distinct', $array['method']);
        $this->assertArrayNotHasKey('attribute', $array);
        $this->assertEquals([], $array['values']);
    }

    public function testToArrayJoinPreservesOperator(): void
    {
        $query = Query::join('t', 'a', 'b', '!=');
        $array = $query->toArray();
        $this->assertEquals(['a', '!=', 'b'], $array['values']);
    }

    public function testToArrayCrossJoin(): void
    {
        $query = Query::crossJoin('t');
        $array = $query->toArray();
        $this->assertEquals('crossJoin', $array['method']);
        $this->assertEquals('t', $array['attribute']);
        $this->assertEquals([], $array['values']);
    }

    public function testToArrayHaving(): void
    {
        $query = Query::having([Query::greaterThan('x', 1), Query::lessThan('y', 10)]);
        $array = $query->toArray();
        $this->assertEquals('having', $array['method']);

        /** @var array<int, array<string, mixed>> $values */
        $values = $array['values'] ?? [];
        $this->assertCount(2, $values);
        $this->assertEquals('greaterThan', $values[0]['method']);
    }

    public function testToArrayUnionAll(): void
    {
        $query = Query::unionAll([Query::equal('x', [1])]);
        $array = $query->toArray();
        $this->assertEquals('unionAll', $array['method']);

        /** @var array<int, array<string, mixed>> $values */
        $values = $array['values'] ?? [];
        $this->assertCount(1, $values);
    }

    public function testToArrayRaw(): void
    {
        $query = Query::raw('a > ?', [10]);
        $array = $query->toArray();
        $this->assertEquals('raw', $array['method']);
        $this->assertEquals('a > ?', $array['attribute']);
        $this->assertEquals([10], $array['values']);
    }

    // ── parseQueries edge cases ──

    public function testParseQueriesEmpty(): void
    {
        $result = Query::parseQueries([]);
        $this->assertCount(0, $result);
    }

    public function testParseQueriesWithNewTypes(): void
    {
        $queries = Query::parseQueries([
            '{"method":"count","attribute":"*","values":["total"]}',
            '{"method":"groupBy","values":["status","country"]}',
            '{"method":"distinct","values":[]}',
            '{"method":"join","attribute":"orders","values":["u.id","=","o.uid"]}',
        ]);
        $this->assertCount(4, $queries);
        $this->assertSame(Method::Count, $queries[0]->getMethod());
        $this->assertSame(Method::GroupBy, $queries[1]->getMethod());
        $this->assertSame(Method::Distinct, $queries[2]->getMethod());
        $this->assertSame(Method::Join, $queries[3]->getMethod());
    }

    // ── toString edge cases ──

    public function testToStringGroupByProducesValidJson(): void
    {
        $query = Query::groupBy(['a', 'b']);
        $json = $query->toString();
        $decoded = json_decode($json, true);
        $this->assertIsArray($decoded);
        $this->assertEquals('groupBy', $decoded['method']);
        $this->assertEquals(['a', 'b'], $decoded['values']);
    }

    public function testToStringRawProducesValidJson(): void
    {
        $query = Query::raw('x > ? AND y < ?', [1, 2]);
        $json = $query->toString();
        $decoded = json_decode($json, true);
        $this->assertIsArray($decoded);
        $this->assertEquals('raw', $decoded['method']);
        $this->assertEquals('x > ? AND y < ?', $decoded['attribute']);
        $this->assertEquals([1, 2], $decoded['values']);
    }
}
