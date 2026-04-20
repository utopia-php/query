<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class QueryTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $query = new Query('equal');
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('', $query->getAttribute());
        $this->assertEquals([], $query->getValues());
    }

    public function testConstructorWithAllParams(): void
    {
        $query = new Query('equal', 'name', ['John']);
        $this->assertEquals('equal', $query->getMethod());
        $this->assertEquals('name', $query->getAttribute());
        $this->assertEquals(['John'], $query->getValues());
    }

    public function testConstructorOrderAscDefaultAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_ASC);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderDescDefaultAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_DESC);
        $this->assertEquals('', $query->getAttribute());
    }

    public function testConstructorOrderAscWithAttribute(): void
    {
        $query = new Query(Query::TYPE_ORDER_ASC, 'name');
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
        $this->assertEquals('notEqual', $query->getMethod());
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

    public function testConstants(): void
    {
        $this->assertEquals('ASC', Query::ORDER_ASC);
        $this->assertEquals('DESC', Query::ORDER_DESC);
        $this->assertEquals('RANDOM', Query::ORDER_RANDOM);
        $this->assertEquals('after', Query::CURSOR_AFTER);
        $this->assertEquals('before', Query::CURSOR_BEFORE);
    }

    public function testVectorTypesConstant(): void
    {
        $this->assertContains(Query::TYPE_VECTOR_DOT, Query::VECTOR_TYPES);
        $this->assertContains(Query::TYPE_VECTOR_COSINE, Query::VECTOR_TYPES);
        $this->assertContains(Query::TYPE_VECTOR_EUCLIDEAN, Query::VECTOR_TYPES);
        $this->assertCount(3, Query::VECTOR_TYPES);
    }

    public function testTypesConstantContainsAll(): void
    {
        $this->assertContains(Query::TYPE_EQUAL, Query::TYPES);
        $this->assertContains(Query::TYPE_REGEX, Query::TYPES);
        $this->assertContains(Query::TYPE_AND, Query::TYPES);
        $this->assertContains(Query::TYPE_OR, Query::TYPES);
        $this->assertContains(Query::TYPE_ELEM_MATCH, Query::TYPES);
        $this->assertContains(Query::TYPE_VECTOR_DOT, Query::TYPES);
    }

    public function testEmptyValues(): void
    {
        $query = Query::equal('name', []);
        $this->assertEquals([], $query->getValues());
    }

    public function testFingerprint(): void
    {
        $equalAlice = '{"method":"equal","attribute":"name","values":["Alice"]}';
        $equalBob = '{"method":"equal","attribute":"name","values":["Bob"]}';
        $equalEmail = '{"method":"equal","attribute":"email","values":["a@b.c"]}';
        $notEqualAlice = '{"method":"notEqual","attribute":"name","values":["Alice"]}';
        $gtAge18 = '{"method":"greaterThan","attribute":"age","values":[18]}';
        $gtAge42 = '{"method":"greaterThan","attribute":"age","values":[42]}';

        // Same shape, different values produce the same fingerprint
        $a = Query::fingerprint([$equalAlice, $gtAge18]);
        $b = Query::fingerprint([$equalBob, $gtAge42]);
        $this->assertSame($a, $b);

        // Different attribute produces different fingerprint
        $c = Query::fingerprint([$equalEmail, $gtAge18]);
        $this->assertNotSame($a, $c);

        // Different method produces different fingerprint
        $d = Query::fingerprint([$notEqualAlice, $gtAge18]);
        $this->assertNotSame($a, $d);

        // Order-independent
        $e = Query::fingerprint([$gtAge18, $equalAlice]);
        $this->assertSame($a, $e);

        // Accepts parsed Query objects
        $parsed = [Query::equal('name', ['Alice']), Query::greaterThan('age', 18)];
        $f = Query::fingerprint($parsed);
        $this->assertSame($a, $f);

        // Empty array returns deterministic hash
        $this->assertSame(\md5(''), Query::fingerprint([]));
    }

    public function testFingerprintNestedLogicalQueries(): void
    {
        // AND queries with different inner shapes produce different fingerprints
        $andEqName = new Query(Query::TYPE_AND, '', [Query::equal('name', ['Alice'])]);
        $andEqEmail = new Query(Query::TYPE_AND, '', [Query::equal('email', ['a@b.c'])]);
        $this->assertNotSame(Query::fingerprint([$andEqName]), Query::fingerprint([$andEqEmail]));

        // AND queries with same inner shape produce the same fingerprint (values differ)
        $andEqNameBob = new Query(Query::TYPE_AND, '', [Query::equal('name', ['Bob'])]);
        $this->assertSame(Query::fingerprint([$andEqName]), Query::fingerprint([$andEqNameBob]));

        // Order of children inside a logical query does not matter
        $andA = new Query(Query::TYPE_AND, '', [Query::equal('name', ['Alice']), Query::greaterThan('age', 18)]);
        $andB = new Query(Query::TYPE_AND, '', [Query::greaterThan('age', 42), Query::equal('name', ['Bob'])]);
        $this->assertSame(Query::fingerprint([$andA]), Query::fingerprint([$andB]));

        // AND of two filters differs from OR of the same two filters
        $orA = new Query(Query::TYPE_OR, '', [Query::equal('name', ['Alice']), Query::greaterThan('age', 18)]);
        $this->assertNotSame(Query::fingerprint([$andA]), Query::fingerprint([$orA]));

        // AND with one child differs from AND with two children
        $andOne = new Query(Query::TYPE_AND, '', [Query::equal('name', ['Alice'])]);
        $andTwo = new Query(Query::TYPE_AND, '', [Query::equal('name', ['Alice']), Query::greaterThan('age', 18)]);
        $this->assertNotSame(Query::fingerprint([$andOne]), Query::fingerprint([$andTwo]));
    }

    public function testFingerprintRejectsInvalidElements(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        Query::fingerprint([42]);
    }
}
