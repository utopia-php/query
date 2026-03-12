<?php

namespace Tests\Query\Parser;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Parser\MySQL;
use Utopia\Query\Type;

class MySQLTest extends TestCase
{
    protected MySQL $parser;

    protected function setUp(): void
    {
        $this->parser = new MySQL();
    }

    /**
     * Build a MySQL COM_QUERY packet
     */
    private function buildQuery(string $sql): string
    {
        $payloadLen = 1 + \strlen($sql);
        $header = \pack('V', $payloadLen);
        $header[3] = "\x00";

        return $header . "\x03" . $sql;
    }

    /**
     * Build a MySQL COM_STMT_PREPARE packet
     */
    private function buildStmtPrepare(string $sql): string
    {
        $payloadLen = 1 + \strlen($sql);
        $header = \pack('V', $payloadLen);
        $header[3] = "\x00";

        return $header . "\x16" . $sql;
    }

    /**
     * Build a MySQL COM_STMT_EXECUTE packet
     */
    private function buildStmtExecute(int $stmtId): string
    {
        $body = \pack('V', $stmtId) . "\x00" . \pack('V', 1);
        $payloadLen = 1 + \strlen($body);
        $header = \pack('V', $payloadLen);
        $header[3] = "\x00";

        return $header . "\x17" . $body;
    }

    // -- Read Queries --

    public function test_select_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('SELECT * FROM users WHERE id = 1')));
    }

    public function test_select_lowercase(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('select id from users')));
    }

    public function test_show_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('SHOW DATABASES')));
    }

    public function test_describe_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('DESCRIBE users')));
    }

    public function test_desc_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('DESC users')));
    }

    public function test_explain_query(): void
    {
        $this->assertSame(Type::Read, $this->parser->parse($this->buildQuery('EXPLAIN SELECT * FROM users')));
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
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('DROP TABLE test')));
    }

    public function test_alter_table(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('ALTER TABLE users ADD COLUMN email VARCHAR(255)')));
    }

    public function test_truncate(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildQuery('TRUNCATE TABLE users')));
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

    public function test_set_command(): void
    {
        $this->assertSame(Type::Transaction, $this->parser->parse($this->buildQuery('SET autocommit = 0')));
    }

    // -- Prepared Statement Protocol --

    public function test_stmt_prepare_routes_to_write(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildStmtPrepare('SELECT * FROM users WHERE id = ?')));
    }

    public function test_stmt_execute_routes_to_write(): void
    {
        $this->assertSame(Type::Write, $this->parser->parse($this->buildStmtExecute(1)));
    }

    // -- Edge Cases --

    public function test_too_short_packet(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->parse("\x00\x00"));
    }

    public function test_unknown_command(): void
    {
        $header = \pack('V', 1);
        $header[3] = "\x00";
        $data = $header . "\x01"; // COM_QUIT
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
            \sprintf('MySQL parse took %.3f us/query (target: < 1.0 us)', $perQuery)
        );
    }
}
