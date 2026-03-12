<?php

namespace Tests\Query\Parser;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Parser\PostgreSQL;
use Utopia\Query\Type;

/**
 * Tests for the shared SQL classification logic in Parser\SQL.
 * Uses PostgreSQL parser as a concrete implementation since
 * classifySQL and extractKeyword are protocol-agnostic.
 */
class SQLTest extends TestCase
{
    protected PostgreSQL $parser;

    protected function setUp(): void
    {
        $this->parser = new PostgreSQL();
    }

    // -- classifySQL Edge Cases --

    public function testClassifyLeadingWhitespace(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("   \t\n  SELECT * FROM users"));
    }

    public function testClassifyLeadingLineComment(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("-- this is a comment\nSELECT * FROM users"));
    }

    public function testClassifyLeadingBlockComment(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("/* block comment */ SELECT * FROM users"));
    }

    public function testClassifyMultipleComments(): void
    {
        $sql = "-- line comment\n/* block comment */\n  -- another line\n  SELECT 1";
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function testClassifyNestedBlockComment(): void
    {
        $sql = "/* outer /* inner */ SELECT 1";
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function testClassifyEmptyQuery(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL(''));
    }

    public function testClassifyWhitespaceOnly(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL("   \t\n  "));
    }

    public function testClassifyCommentOnly(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL('-- just a comment'));
    }

    public function testClassifySelectWithParenthesis(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('SELECT(1)'));
    }

    public function testClassifySelectWithSemicolon(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('SELECT;'));
    }

    // -- COPY Direction --

    public function testClassifyCopyTo(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('COPY users TO STDOUT'));
    }

    public function testClassifyCopyFrom(): void
    {
        $this->assertSame(Type::Write, $this->parser->classifySQL("COPY users FROM '/tmp/data.csv'"));
    }

    public function testClassifyCopyAmbiguous(): void
    {
        $this->assertSame(Type::Write, $this->parser->classifySQL('COPY users'));
    }

    // -- CTE (WITH) --

    public function testClassifyCteWithSelect(): void
    {
        $sql = 'WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function testClassifyCteWithInsert(): void
    {
        $sql = 'WITH new_data AS (SELECT 1 AS id) INSERT INTO users SELECT * FROM new_data';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function testClassifyCteWithUpdate(): void
    {
        $sql = 'WITH src AS (SELECT id FROM staging) UPDATE users SET active = true FROM src WHERE users.id = src.id';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function testClassifyCteWithDelete(): void
    {
        $sql = 'WITH old AS (SELECT id FROM users WHERE created_at < now()) DELETE FROM users WHERE id IN (SELECT id FROM old)';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function testClassifyCteRecursiveSelect(): void
    {
        $sql = 'WITH RECURSIVE tree AS (SELECT id, parent_id FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function testClassifyCteNoFinalKeyword(): void
    {
        $sql = 'WITH x AS (SELECT 1)';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    // -- extractKeyword --

    public function testExtractKeywordSimple(): void
    {
        $this->assertSame('SELECT', $this->parser->extractKeyword('SELECT * FROM users'));
    }

    public function testExtractKeywordLowercase(): void
    {
        $this->assertSame('INSERT', $this->parser->extractKeyword('insert into users'));
    }

    public function testExtractKeywordWithWhitespace(): void
    {
        $this->assertSame('DELETE', $this->parser->extractKeyword("  \t\n  DELETE FROM users"));
    }

    public function testExtractKeywordWithComments(): void
    {
        $this->assertSame('UPDATE', $this->parser->extractKeyword("-- comment\nUPDATE users SET x = 1"));
    }

    public function testExtractKeywordEmpty(): void
    {
        $this->assertSame('', $this->parser->extractKeyword(''));
    }

    public function testExtractKeywordParenthesized(): void
    {
        $this->assertSame('SELECT', $this->parser->extractKeyword('SELECT(1)'));
    }

    // -- Performance --

    public function testClassifySqlPerformance(): void
    {
        $queries = [
            'SELECT * FROM users WHERE id = 1',
            "INSERT INTO logs (msg) VALUES ('test')",
            'BEGIN',
            '   /* comment */ SELECT 1',
            'WITH cte AS (SELECT 1) SELECT * FROM cte',
        ];

        $iterations = 100_000;

        $start = \hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->parser->classifySQL($queries[$i % \count($queries)]);
        }
        $elapsed = (\hrtime(true) - $start) / 1_000_000_000;
        $perQuery = ($elapsed / $iterations) * 1_000_000;

        $this->assertLessThan(
            2.0,
            $perQuery,
            \sprintf('classifySQL took %.3f us/query (target: < 2.0 us)', $perQuery)
        );
    }
}
