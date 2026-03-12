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

    public function test_classify_leading_whitespace(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("   \t\n  SELECT * FROM users"));
    }

    public function test_classify_leading_line_comment(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("-- this is a comment\nSELECT * FROM users"));
    }

    public function test_classify_leading_block_comment(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL("/* block comment */ SELECT * FROM users"));
    }

    public function test_classify_multiple_comments(): void
    {
        $sql = "-- line comment\n/* block comment */\n  -- another line\n  SELECT 1";
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function test_classify_nested_block_comment(): void
    {
        $sql = "/* outer /* inner */ SELECT 1";
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function test_classify_empty_query(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL(''));
    }

    public function test_classify_whitespace_only(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL("   \t\n  "));
    }

    public function test_classify_comment_only(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL('-- just a comment'));
    }

    public function test_classify_select_with_parenthesis(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('SELECT(1)'));
    }

    public function test_classify_select_with_semicolon(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('SELECT;'));
    }

    // -- COPY Direction --

    public function test_classify_copy_to(): void
    {
        $this->assertSame(Type::Read, $this->parser->classifySQL('COPY users TO STDOUT'));
    }

    public function test_classify_copy_from(): void
    {
        $this->assertSame(Type::Write, $this->parser->classifySQL("COPY users FROM '/tmp/data.csv'"));
    }

    public function test_classify_copy_ambiguous(): void
    {
        $this->assertSame(Type::Write, $this->parser->classifySQL('COPY users'));
    }

    // -- CTE (WITH) --

    public function test_classify_cte_with_select(): void
    {
        $sql = 'WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function test_classify_cte_with_insert(): void
    {
        $sql = 'WITH new_data AS (SELECT 1 AS id) INSERT INTO users SELECT * FROM new_data';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function test_classify_cte_with_update(): void
    {
        $sql = 'WITH src AS (SELECT id FROM staging) UPDATE users SET active = true FROM src WHERE users.id = src.id';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function test_classify_cte_with_delete(): void
    {
        $sql = 'WITH old AS (SELECT id FROM users WHERE created_at < now()) DELETE FROM users WHERE id IN (SELECT id FROM old)';
        $this->assertSame(Type::Write, $this->parser->classifySQL($sql));
    }

    public function test_classify_cte_recursive_select(): void
    {
        $sql = 'WITH RECURSIVE tree AS (SELECT id, parent_id FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    public function test_classify_cte_no_final_keyword(): void
    {
        $sql = 'WITH x AS (SELECT 1)';
        $this->assertSame(Type::Read, $this->parser->classifySQL($sql));
    }

    // -- extractKeyword --

    public function test_extract_keyword_simple(): void
    {
        $this->assertSame('SELECT', $this->parser->extractKeyword('SELECT * FROM users'));
    }

    public function test_extract_keyword_lowercase(): void
    {
        $this->assertSame('INSERT', $this->parser->extractKeyword('insert into users'));
    }

    public function test_extract_keyword_with_whitespace(): void
    {
        $this->assertSame('DELETE', $this->parser->extractKeyword("  \t\n  DELETE FROM users"));
    }

    public function test_extract_keyword_with_comments(): void
    {
        $this->assertSame('UPDATE', $this->parser->extractKeyword("-- comment\nUPDATE users SET x = 1"));
    }

    public function test_extract_keyword_empty(): void
    {
        $this->assertSame('', $this->parser->extractKeyword(''));
    }

    public function test_extract_keyword_parenthesized(): void
    {
        $this->assertSame('SELECT', $this->parser->extractKeyword('SELECT(1)'));
    }

    // -- Performance --

    public function test_classify_sql_performance(): void
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
