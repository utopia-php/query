<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\MySQL as MySQLBuilder;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query;

class QueryTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $query = new Query('equal');
        $this->assertSame(Method::Equal, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testConstructorWithAllParams(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $this->assertSame(Method::Equal, $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals(['John'], $query->getValues());
    }

    public function testConstructorOrderAscDefaultAttribute(): void
    {
        $query = new Query(Method::OrderAsc);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderDescDefaultAttribute(): void
    {
        $query = new Query(Method::OrderDesc);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderAscWithAttribute(): void
    {
        $query = new Query(Method::OrderAsc, 'name');
        $this->assertEquals('name', $query->getAttribute());
    }

    public function testGetValue(): void
    {
        $query = new Query('equal', 'name', ['John', 'Jane']);
        $this->assertEquals('John', $query->getValue());
    }

    public function testGetValueDefault(): void
    {
        $query = new Query('equal', 'name');
        $this->assertEquals('fallback', $query->getValue('fallback'));
    }

    public function testGetValueDefaultNull(): void
    {
        $query = new Query('equal', 'name');
        $this->assertNull($query->getValue());
    }

    public function testSetMethod(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setMethod('notEqual');
        $this->assertSame(Method::NotEqual, $query->getMethod());
        $this->assertSame($query, $result);
    }

    public function testSetAttribute(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setAttribute('age');
        $this->assertEquals('age', $query->getAttribute());
        $this->assertSame($query, $result);
    }

    public function testSetValues(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $result = $query->setValues(['Jane', 'Doe']);
        $this->assertEquals(['Jane', 'Doe'], $query->getValues());
        $this->assertSame($query, $result);
    }

    public function testSetValue(): void
    {
        $query = new Query('equal', 'name', ['John', 'Jane']);
        $result = $query->setValue('Only');
        $this->assertEquals(['Only'], $query->getValues());
        $this->assertSame($query, $result);
    }

    public function testSetAttributeType(): void
    {
        $query = new Query('equal', 'name');
        $query->setAttributeType('string');
        $this->assertEquals('string', $query->getAttributeType());
    }

    public function testOnArray(): void
    {
        $query = new Query('equal', 'tags', [['a', 'b']]);
        $this->assertFalse($query->onArray());
        $query->setOnArray(true);
        $this->assertTrue($query->onArray());
    }

    public function testMethodEnumValues(): void
    {
        $this->assertEquals('ASC', OrderDirection::Asc->value);
        $this->assertEquals('DESC', OrderDirection::Desc->value);
        $this->assertEquals('RANDOM', OrderDirection::Random->value);
        $this->assertEquals('after', CursorDirection::After->value);
        $this->assertEquals('before', CursorDirection::Before->value);
    }

    public function testVectorMethodsAreVector(): void
    {
        $this->assertTrue(Method::VectorDot->isVector());
        $this->assertTrue(Method::VectorCosine->isVector());
        $this->assertTrue(Method::VectorEuclidean->isVector());
        $vectorMethods = array_filter(Method::cases(), fn (Method $m) => $m->isVector());
        $this->assertCount(3, $vectorMethods);
    }

    public function testAllMethodCasesAreValid(): void
    {
        $this->assertTrue(Query::isMethod(Method::Equal->value));
        $this->assertTrue(Query::isMethod(Method::Regex->value));
        $this->assertTrue(Query::isMethod(Method::And->value));
        $this->assertTrue(Query::isMethod(Method::Or->value));
        $this->assertTrue(Query::isMethod(Method::ElemMatch->value));
        $this->assertTrue(Query::isMethod(Method::VectorDot->value));
    }

    public function testEmptyValues(): void
    {
        $query = Query::equal('name', []);
        $this->assertEquals([], $query->getValues());
    }

    public function testMethodContainsNewTypes(): void
    {
        $this->assertSame(Method::Count, Method::from('count'));
        $this->assertSame(Method::Sum, Method::from('sum'));
        $this->assertSame(Method::Avg, Method::from('avg'));
        $this->assertSame(Method::Min, Method::from('min'));
        $this->assertSame(Method::Max, Method::from('max'));
        $this->assertSame(Method::GroupBy, Method::from('groupBy'));
        $this->assertSame(Method::Having, Method::from('having'));
        $this->assertSame(Method::Distinct, Method::from('distinct'));
        $this->assertSame(Method::Join, Method::from('join'));
        $this->assertSame(Method::LeftJoin, Method::from('leftJoin'));
        $this->assertSame(Method::RightJoin, Method::from('rightJoin'));
        $this->assertSame(Method::CrossJoin, Method::from('crossJoin'));
        $this->assertSame(Method::Union, Method::from('union'));
        $this->assertSame(Method::UnionAll, Method::from('unionAll'));
        $this->assertSame(Method::Raw, Method::from('raw'));
    }

    public function testIsMethodNewTypes(): void
    {
        $this->assertTrue(Query::isMethod('count'));
        $this->assertTrue(Query::isMethod('sum'));
        $this->assertTrue(Query::isMethod('avg'));
        $this->assertTrue(Query::isMethod('min'));
        $this->assertTrue(Query::isMethod('max'));
        $this->assertTrue(Query::isMethod('groupBy'));
        $this->assertTrue(Query::isMethod('having'));
        $this->assertTrue(Query::isMethod('distinct'));
        $this->assertTrue(Query::isMethod('join'));
        $this->assertTrue(Query::isMethod('leftJoin'));
        $this->assertTrue(Query::isMethod('rightJoin'));
        $this->assertTrue(Query::isMethod('crossJoin'));
        $this->assertTrue(Query::isMethod('union'));
        $this->assertTrue(Query::isMethod('unionAll'));
        $this->assertTrue(Query::isMethod('raw'));
    }

    public function testDistinctFactory(): void
    {
        $query = Query::distinct();
        $this->assertSame(Method::Distinct, $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactory(): void
    {
        $query = Query::raw('score > ?', [10]);
        $this->assertSame(Method::Raw, $query->getMethod());
        $this->assertEquals('score > ?', $query->getAttribute());
        $this->assertEquals([10], $query->getValues());
    }

    public function testUnionFactory(): void
    {
        $inner = [Query::equal('x', [1])];
        $query = Query::union($inner);
        $this->assertSame(Method::Union, $query->getMethod());
        $this->assertCount(1, $query->getValues());
    }

    public function testUnionAllFactory(): void
    {
        $inner = [Query::equal('x', [1])];
        $query = Query::unionAll($inner);
        $this->assertSame(Method::UnionAll, $query->getMethod());
    }
    //  ADDITIONAL EDGE CASES

    public function testMethodNoDuplicateValues(): void
    {
        $values = array_map(fn (Method $m) => $m->value, Method::cases());
        $this->assertEquals(count($values), count(array_unique($values)));
    }

    public function testAggregateMethodsNoDuplicates(): void
    {
        $aggMethods = array_filter(Method::cases(), fn (Method $m) => $m->isAggregate());
        $values = array_map(fn (Method $m) => $m->value, $aggMethods);
        $this->assertEquals(count($values), count(array_unique($values)));
    }

    public function testJoinMethodsNoDuplicates(): void
    {
        $joinMethods = array_filter(Method::cases(), fn (Method $m) => $m->isJoin());
        $values = array_map(fn (Method $m) => $m->value, $joinMethods);
        $this->assertEquals(count($values), count(array_unique($values)));
    }

    public function testAggregateMethodsAreValidMethods(): void
    {
        $aggMethods = array_filter(Method::cases(), fn (Method $m) => $m->isAggregate());
        foreach ($aggMethods as $method) {
            $this->assertSame($method, Method::from($method->value));
        }
    }

    public function testJoinMethodsAreValidMethods(): void
    {
        $joinMethods = array_filter(Method::cases(), fn (Method $m) => $m->isJoin());
        foreach ($joinMethods as $method) {
            $this->assertSame($method, Method::from($method->value));
        }
    }

    public function testIsMethodCaseSensitive(): void
    {
        $this->assertFalse(Query::isMethod('COUNT'));
        $this->assertFalse(Query::isMethod('Sum'));
        $this->assertFalse(Query::isMethod('JOIN'));
        $this->assertFalse(Query::isMethod('DISTINCT'));
        $this->assertFalse(Query::isMethod('GroupBy'));
        $this->assertFalse(Query::isMethod('RAW'));
    }

    public function testRawFactoryEmptySql(): void
    {
        $query = Query::raw('');
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactoryEmptyBindings(): void
    {
        $query = Query::raw('1 = 1', []);
        $this->assertEquals([], $query->getValues());
    }

    public function testRawFactoryMixedBindings(): void
    {
        $query = Query::raw('a = ? AND b = ? AND c = ?', ['str', 42, 3.14]);
        $this->assertEquals(['str', 42, 3.14], $query->getValues());
    }

    public function testUnionIsNested(): void
    {
        $query = Query::union([Query::equal('x', [1])]);
        $this->assertTrue($query->isNested());
    }

    public function testUnionAllIsNested(): void
    {
        $query = Query::unionAll([Query::equal('x', [1])]);
        $this->assertTrue($query->isNested());
    }

    public function testDistinctNotNested(): void
    {
        $this->assertFalse(Query::distinct()->isNested());
    }

    public function testCountNotNested(): void
    {
        $this->assertFalse(Query::count()->isNested());
    }

    public function testGroupByNotNested(): void
    {
        $this->assertFalse(Query::groupBy(['a'])->isNested());
    }

    public function testJoinNotNested(): void
    {
        $this->assertFalse(Query::join('t', 'a', 'b')->isNested());
    }

    public function testRawNotNested(): void
    {
        $this->assertFalse(Query::raw('1=1')->isNested());
    }

    public function testHavingNested(): void
    {
        $this->assertTrue(Query::having([Query::equal('x', [1])])->isNested());
    }

    public function testCloneDeepCopiesHavingQueries(): void
    {
        $inner = Query::greaterThan('total', 5);
        $outer = Query::having([$inner]);
        $cloned = clone $outer;

        $clonedValues = $cloned->getValues();
        $this->assertNotSame($inner, $clonedValues[0]);
        $this->assertInstanceOf(Query::class, $clonedValues[0]);

        /** @var Query $clonedInner */
        $clonedInner = $clonedValues[0];
        $this->assertSame(Method::GreaterThan, $clonedInner->getMethod());
    }

    public function testCloneDeepCopiesUnionQueries(): void
    {
        $inner = Query::equal('x', [1]);
        $outer = Query::union([$inner]);
        $cloned = clone $outer;

        $clonedValues = $cloned->getValues();
        $this->assertNotSame($inner, $clonedValues[0]);
    }

    public function testCountEnumValue(): void
    {
        $this->assertEquals('count', Method::Count->value);
    }

    public function testSumEnumValue(): void
    {
        $this->assertEquals('sum', Method::Sum->value);
    }

    public function testAvgEnumValue(): void
    {
        $this->assertEquals('avg', Method::Avg->value);
    }

    public function testMinEnumValue(): void
    {
        $this->assertEquals('min', Method::Min->value);
    }

    public function testMaxEnumValue(): void
    {
        $this->assertEquals('max', Method::Max->value);
    }

    public function testGroupByEnumValue(): void
    {
        $this->assertEquals('groupBy', Method::GroupBy->value);
    }

    public function testHavingEnumValue(): void
    {
        $this->assertEquals('having', Method::Having->value);
    }

    public function testDistinctEnumValue(): void
    {
        $this->assertEquals('distinct', Method::Distinct->value);
    }

    public function testJoinEnumValue(): void
    {
        $this->assertEquals('join', Method::Join->value);
    }

    public function testLeftJoinEnumValue(): void
    {
        $this->assertEquals('leftJoin', Method::LeftJoin->value);
    }

    public function testRightJoinEnumValue(): void
    {
        $this->assertEquals('rightJoin', Method::RightJoin->value);
    }

    public function testCrossJoinEnumValue(): void
    {
        $this->assertEquals('crossJoin', Method::CrossJoin->value);
    }

    public function testUnionEnumValue(): void
    {
        $this->assertEquals('union', Method::Union->value);
    }

    public function testUnionAllEnumValue(): void
    {
        $this->assertEquals('unionAll', Method::UnionAll->value);
    }

    public function testRawEnumValue(): void
    {
        $this->assertEquals('raw', Method::Raw->value);
    }

    public function testCountIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::count()->isSpatialQuery());
    }

    public function testJoinIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::join('t', 'a', 'b')->isSpatialQuery());
    }

    public function testDistinctIsSpatialQueryFalse(): void
    {
        $this->assertFalse(Query::distinct()->isSpatialQuery());
    }

    public function testToStringReturnsJson(): void
    {
        $json = Query::equal('name', ['John'])->toString();
        $decoded = \json_decode($json, true);
        $this->assertIsArray($decoded);
        $this->assertEquals('equal', $decoded['method']);
        $this->assertEquals('name', $decoded['attribute']);
        $this->assertEquals(['John'], $decoded['values']);
    }

    public function testToStringWithNestedQuery(): void
    {
        $json = Query::and([Query::equal('x', [1])])->toString();
        $decoded = \json_decode($json, true);
        $this->assertIsArray($decoded);
        /** @var array<string, mixed> $decoded */
        $this->assertEquals('and', $decoded['method']);
        $this->assertIsArray($decoded['values']);
        $this->assertCount(1, $decoded['values']);
        /** @var array<string, mixed> $inner */
        $inner = $decoded['values'][0];
        $this->assertEquals('equal', $inner['method']);
    }

    public function testToStringThrowsOnInvalidJson(): void
    {
        // Verify that toString returns valid JSON for complex queries
        $query = Query::and([
            Query::or([
                Query::equal('a', [1]),
                Query::greaterThan('b', 2),
            ]),
            Query::lessThan('c', 3),
        ]);
        $json = $query->toString();
        $this->assertJson($json);
    }

    public function testSetMethodWithEnum(): void
    {
        $query = new Query('equal');
        $query->setMethod(Method::GreaterThan);
        $this->assertSame(Method::GreaterThan, $query->getMethod());
    }

    public function testToArraySimpleFilter(): void
    {
        $array = Query::equal('age', [25])->toArray();
        $this->assertEquals('equal', $array['method']);
        $this->assertEquals('age', $array['attribute']);
        $this->assertEquals([25], $array['values']);
    }

    public function testToArrayWithEmptyAttribute(): void
    {
        $array = Query::distinct()->toArray();
        $this->assertArrayNotHasKey('attribute', $array);
    }

    public function testToArrayNestedQuery(): void
    {
        $array = Query::and([Query::equal('x', [1])])->toArray();
        $this->assertIsArray($array['values']);
        $this->assertCount(1, $array['values']);
        /** @var array<string, mixed> $nested */
        $nested = $array['values'][0];
        $this->assertArrayHasKey('method', $nested);
        $this->assertArrayHasKey('attribute', $nested);
        $this->assertArrayHasKey('values', $nested);
        $this->assertEquals('equal', $nested['method']);
    }

    public function testCompileOrderAsc(): void
    {
        $builder = new MySQLBuilder();
        $result = Query::orderAsc('name')->compile($builder);
        $this->assertStringContainsString('ASC', $result);
    }

    public function testCompileOrderDesc(): void
    {
        $builder = new MySQLBuilder();
        $result = Query::orderDesc('name')->compile($builder);
        $this->assertStringContainsString('DESC', $result);
    }

    public function testCompileLimit(): void
    {
        $builder = new MySQLBuilder();
        $result = Query::limit(10)->compile($builder);
        $this->assertStringContainsString('LIMIT ?', $result);
    }

    public function testCompileOffset(): void
    {
        $builder = new MySQLBuilder();
        $result = Query::offset(5)->compile($builder);
        $this->assertStringContainsString('OFFSET ?', $result);
    }

    public function testCompileAggregate(): void
    {
        $builder = new MySQLBuilder();
        $result = Query::count('*', 'total')->compile($builder);
        $this->assertStringContainsString('COUNT(*)', $result);
        $this->assertStringContainsString('total', $result);
    }

    public function testIsMethodReturnsFalseForGarbage(): void
    {
        $this->assertFalse(Query::isMethod('notAMethod'));
    }

    public function testIsMethodReturnsFalseForEmpty(): void
    {
        $this->assertFalse(Query::isMethod(''));
    }

    public function testJsonContainsFactory(): void
    {
        $query = Query::jsonContains('tags', 'php');
        $this->assertSame(Method::JsonContains, $query->getMethod());
        $this->assertEquals('tags', $query->getAttribute());
        $this->assertEquals(['php'], $query->getValues());
    }

    public function testJsonNotContainsFactory(): void
    {
        $query = Query::jsonNotContains('meta', 42);
        $this->assertSame(Method::JsonNotContains, $query->getMethod());
    }

    public function testJsonOverlapsFactory(): void
    {
        $query = Query::jsonOverlaps('tags', ['a', 'b']);
        $this->assertSame(Method::JsonOverlaps, $query->getMethod());
        $this->assertEquals([['a', 'b']], $query->getValues());
    }

    public function testJsonPathFactory(): void
    {
        $query = Query::jsonPath('data', 'name', '=', 'test');
        $this->assertSame(Method::JsonPath, $query->getMethod());
        $this->assertEquals(['name', '=', 'test'], $query->getValues());
    }

    public function testCoversFactory(): void
    {
        $query = Query::covers('zone', [1.0, 2.0]);
        $this->assertSame(Method::Covers, $query->getMethod());
    }

    public function testNotCoversFactory(): void
    {
        $query = Query::notCovers('zone', [1.0, 2.0]);
        $this->assertSame(Method::NotCovers, $query->getMethod());
    }

    public function testSpatialEqualsFactory(): void
    {
        $query = Query::spatialEquals('geom', [3.0, 4.0]);
        $this->assertSame(Method::SpatialEquals, $query->getMethod());
    }

    public function testNotSpatialEqualsFactory(): void
    {
        $query = Query::notSpatialEquals('geom', [3.0, 4.0]);
        $this->assertSame(Method::NotSpatialEquals, $query->getMethod());
    }

    public function testIsJsonMethod(): void
    {
        $this->assertTrue(Method::JsonContains->isJson());
        $this->assertTrue(Method::JsonNotContains->isJson());
        $this->assertTrue(Method::JsonOverlaps->isJson());
        $this->assertTrue(Method::JsonPath->isJson());
    }

    public function testIsJsonMethodFalseForNonJson(): void
    {
        $this->assertFalse(Method::Equal->isJson());
    }

    public function testIsSpatialMethodCovers(): void
    {
        $this->assertTrue(Method::Covers->isSpatial());
        $this->assertTrue(Method::NotCovers->isSpatial());
        $this->assertTrue(Method::SpatialEquals->isSpatial());
        $this->assertTrue(Method::NotSpatialEquals->isSpatial());
    }

    public function testIsSpatialMethodFalseForNonSpatial(): void
    {
        $this->assertFalse(Method::Equal->isSpatial());
    }

    public function testIsFilterMethod(): void
    {
        $this->assertTrue(Method::Equal->isFilter());
        $this->assertTrue(Method::NotEqual->isFilter());
    }

    public function testIsFilterMethodFalseForNonFilter(): void
    {
        $this->assertFalse(Method::OrderAsc->isFilter());
    }
}
