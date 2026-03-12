<?php

namespace Tests\Query\Parser;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Parser\PostgreSQL;
use Utopia\Query\Type;

class PostgreSQLTest extends TestCase
{
    protected PostgreSQL $parser;

    protected function setUp(): void
    {
        $this->parser = new PostgreSQL();
    }

    /**
     * Build a PostgreSQL Simple Query ('Q') message
     */
    private function buildQuery(string $sql): string
    {
        $body = $sql . "\x00";
        $length = \strlen($body) + 4;

        return 'Q' . \pack('N', $length) . $body;
    }

    /**
     * Build a PostgreSQL Parse ('P') message
     */
    private function buildParse(string $stmtName, string $sql): string
    {
        $body = $stmtName . "\x00" . $sql . "\x00" . \pack('n', 0);
        $length = \strlen($body) + 4;

        return 'P' . \pack('N', $length) . $body;
    }

    /**
     * Build a PostgreSQL Bind ('B') message
     */
    private function buildBind(): string
    {
        $body = "\x00\x00" . \pack('n', 0) . \pack('n', 0) . \pack('n', 0);
        $length = \strlen($body) + 4;

        return 'B' . \pack('N', $length) . $body;
    }

    /**
     * Build a PostgreSQL Execute ('E') message
     */
    private function buildExecute(): string
    {
        $body = "\x00" . \pack('N', 0);
        $length = \strlen($body) + 4;

        return 'E' . \pack('N', $length) . $body;
    }

    // -- Read Queries --

    public function test_select_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('SELECT * FROM users WHERE id = 1')));
    }

    public function test_select_lowercase(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('select id, name from users')));
    }

    public function test_select_mixed_case(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('SeLeCt * FROM users')));
    }

    public function test_show_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('SHOW TABLES')));
    }

    public function test_describe_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('DESCRIBE users')));
    }

    public function test_explain_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('EXPLAIN SELECT * FROM users')));
    }

    public function test_table_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('TABLE users')));
    }

    public function test_values_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery("VALUES (1, 'a'), (2, 'b')")));
    }

    // -- Write Queries --

    public function test_insert_query(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery("INSERT INTO users (name) VALUES ('test')")));
    }

    public function test_update_query(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery("UPDATE users SET name = 'test' WHERE id = 1")));
    }

    public function test_delete_query(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('DELETE FROM users WHERE id = 1')));
    }

    public function test_create_table(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('CREATE TABLE test (id INT PRIMARY KEY)')));
    }

    public function test_drop_table(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('DROP TABLE IF EXISTS test')));
    }

    public function test_alter_table(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('ALTER TABLE users ADD COLUMN email TEXT')));
    }

    public function test_truncate(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('TRUNCATE TABLE users')));
    }

    public function test_grant(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('GRANT SELECT ON users TO readonly')));
    }

    public function test_revoke(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('REVOKE ALL ON users FROM public')));
    }

    public function test_lock_table(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('LOCK TABLE users IN ACCESS EXCLUSIVE MODE')));
    }

    public function test_call(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('CALL my_procedure()')));
    }

    public function test_do(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery("DO \$\$ BEGIN RAISE NOTICE 'hello'; END \$\$")));
    }

    // -- Transaction Commands --

    public function test_begin_transaction(): void
    {
        $this->assertSame(Type::TransactionBegin, $this->parser->parse($this->buildQuery('BEGIN')));
    }

    public function test_start_transaction(): void
    {
        $this->assertSame(Type::TransactionBegin, $this->parser->parse($this->buildQuery('START TRANSACTION')));
    }

    public function test_commit(): void
    {
        $this->assertSame(Type::TransactionEnd, $this->parser->parse($this->buildQuery('COMMIT')));
    }

    public function test_rollback(): void
    {
        $this->assertSame(Type::TransactionEnd, $this->parser->parse($this->buildQuery('ROLLBACK')));
    }

    public function test_savepoint(): void
    {
        $this->assertSame(Type::Transaction, $this->parser->parse($this->buildQuery('SAVEPOINT sp1')));
    }

    public function test_release_savepoint(): void
    {
        $this->assertSame(Type::Transaction, $this->parser->parse($this->buildQuery('RELEASE SAVEPOINT sp1')));
    }

    public function test_set_command(): void
    {
        $this->assertSame(Type::Transaction, $this->parser->parse($this->buildQuery("SET search_path TO 'public'")));
    }

    // -- Extended Query Protocol --

    public function test_parse_message_routes_to_write(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildParse('stmt1', 'SELECT * FROM users')));
    }

    public function test_bind_message_routes_to_write(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildBind()));
    }

    public function test_execute_message_routes_to_write(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildExecute()));
    }

    // -- Edge Cases --

    public function test_too_short_packet(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->parse('Q'));
    }

    public function test_unknown_message_type(): void
    {
        $data = 'X' . \pack('N', 5) . "\x00";
        $this->assertSame(Type::Unknown, $this->parser->parse($data));
    }

    // -- Performance --

    public function test_parse_performance(): void
    {
        $data = $this->buildQuery('SELECT * FROM users WHERE id = 1');
        $iterations = 100_000;

        $start = \hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->parser->parse($data);
        }
        $elapsed = (\hrtime(true) - $start) / 1_000_000_000;
        $perQuery = ($elapsed / $iterations) * 1_000_000;

        $this->assertLessThan(
            1.0,
            $perQuery,
            \sprintf('PostgreSQL parse took %.3f us/query (target: < 1.0 us)', $perQuery)
        );
    }
}
