<?php

namespace Tests\Query\Parser;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Parser\MongoDB;
use Utopia\Query\Type;

class MongoDBTest extends TestCase
{
    protected MongoDB $parser;

    protected function setUp(): void
    {
        $this->parser = new MongoDB();
    }

    /**
     * Build a MongoDB OP_MSG packet with a BSON command document
     *
     * @param  array<string, mixed>  $document  Command document
     */
    private function buildOpMsg(array $document): string
    {
        $bson = $this->encodeBsonDocument($document);

        $sectionKind = "\x00"; // kind 0 = body
        $flags = \pack('V', 0);

        // Header: length (4) + requestId (4) + responseTo (4) + opcode (4)
        $body = $flags . $sectionKind . $bson;
        $header = \pack('V', 16 + \strlen($body))  // message length
            . \pack('V', 1)                          // request ID
            . \pack('V', 0)                          // response to
            . \pack('V', 2013);                      // opcode: OP_MSG

        return $header . $body;
    }

    /**
     * Encode a simple BSON document (supports string, int, bool, and nested documents)
     *
     * @param  array<string, mixed>  $doc
     */
    private function encodeBsonDocument(array $doc): string
    {
        $body = '';

        foreach ($doc as $key => $value) {
            if (\is_string($value)) {
                // Type 0x02: string
                $body .= "\x02" . $key . "\x00" . \pack('V', \strlen($value) + 1) . $value . "\x00";
            } elseif (\is_int($value)) {
                // Type 0x10: int32
                $body .= "\x10" . $key . "\x00" . \pack('V', $value);
            } elseif (\is_bool($value)) {
                // Type 0x08: boolean
                $body .= "\x08" . $key . "\x00" . ($value ? "\x01" : "\x00");
            } elseif (\is_array($value)) {
                // Type 0x03: embedded document
                $body .= "\x03" . $key . "\x00" . $this->encodeBsonDocument($value);
            }
        }

        $body .= "\x00"; // terminator

        return \pack('V', 4 + \strlen($body)) . $body;
    }

    // -- Read Commands --

    public function testFindCommand(): void
    {
        $data = $this->buildOpMsg(['find' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testAggregateCommand(): void
    {
        $data = $this->buildOpMsg(['aggregate' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testCountCommand(): void
    {
        $data = $this->buildOpMsg(['count' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testDistinctCommand(): void
    {
        $data = $this->buildOpMsg(['distinct' => 'users', 'key' => 'name', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testListCollectionsCommand(): void
    {
        $data = $this->buildOpMsg(['listCollections' => 1, '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testListDatabasesCommand(): void
    {
        $data = $this->buildOpMsg(['listDatabases' => 1, '$db' => 'admin']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testListIndexesCommand(): void
    {
        $data = $this->buildOpMsg(['listIndexes' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testDbStatsCommand(): void
    {
        $data = $this->buildOpMsg(['dbStats' => 1, '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testCollStatsCommand(): void
    {
        $data = $this->buildOpMsg(['collStats' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testExplainCommand(): void
    {
        $data = $this->buildOpMsg(['explain' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testGetMoreCommand(): void
    {
        $data = $this->buildOpMsg(['getMore' => 12345, '$db' => 'mydb']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testServerStatusCommand(): void
    {
        $data = $this->buildOpMsg(['serverStatus' => 1, '$db' => 'admin']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testPingCommand(): void
    {
        $data = $this->buildOpMsg(['ping' => 1, '$db' => 'admin']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testHelloCommand(): void
    {
        $data = $this->buildOpMsg(['hello' => 1, '$db' => 'admin']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    public function testIsMasterCommand(): void
    {
        $data = $this->buildOpMsg(['isMaster' => 1, '$db' => 'admin']);
        $this->assertSame(Type::Read, $this->parser->parse($data));
    }

    // -- Write Commands --

    public function testInsertCommand(): void
    {
        $data = $this->buildOpMsg(['insert' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testUpdateCommand(): void
    {
        $data = $this->buildOpMsg(['update' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testDeleteCommand(): void
    {
        $data = $this->buildOpMsg(['delete' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testFindAndModifyCommand(): void
    {
        $data = $this->buildOpMsg(['findAndModify' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testCreateCommand(): void
    {
        $data = $this->buildOpMsg(['create' => 'new_collection', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testDropCommand(): void
    {
        $data = $this->buildOpMsg(['drop' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testCreateIndexesCommand(): void
    {
        $data = $this->buildOpMsg(['createIndexes' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testDropIndexesCommand(): void
    {
        $data = $this->buildOpMsg(['dropIndexes' => 'users', '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testDropDatabaseCommand(): void
    {
        $data = $this->buildOpMsg(['dropDatabase' => 1, '$db' => 'mydb']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    public function testRenameCollectionCommand(): void
    {
        $data = $this->buildOpMsg(['renameCollection' => 'users', '$db' => 'admin']);
        $this->assertSame(Type::Write, $this->parser->parse($data));
    }

    // -- Transaction Commands --

    public function testStartTransaction(): void
    {
        $data = $this->buildOpMsg(['find' => 'users', '$db' => 'mydb', 'startTransaction' => true]);
        $this->assertSame(Type::TransactionBegin, $this->parser->parse($data));
    }

    public function testCommitTransaction(): void
    {
        $data = $this->buildOpMsg(['commitTransaction' => 1, '$db' => 'admin']);
        $this->assertSame(Type::TransactionEnd, $this->parser->parse($data));
    }

    public function testAbortTransaction(): void
    {
        $data = $this->buildOpMsg(['abortTransaction' => 1, '$db' => 'admin']);
        $this->assertSame(Type::TransactionEnd, $this->parser->parse($data));
    }

    // -- Edge Cases --

    public function testTooShortPacket(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->parse("\x00\x00\x00\x00"));
    }

    public function testWrongOpcode(): void
    {
        // Build a packet with opcode 2004 (OP_QUERY, legacy) instead of 2013
        $bson = $this->encodeBsonDocument(['find' => 'users']);
        $body = \pack('V', 0) . "\x00" . $bson;
        $header = \pack('V', 16 + \strlen($body))
            . \pack('V', 1)
            . \pack('V', 0)
            . \pack('V', 2004); // wrong opcode

        $this->assertSame(Type::Unknown, $this->parser->parse($header . $body));
    }

    public function testUnknownCommand(): void
    {
        $data = $this->buildOpMsg(['customCommand' => 1, '$db' => 'mydb']);
        $this->assertSame(Type::Unknown, $this->parser->parse($data));
    }

    public function testEmptyBsonDocument(): void
    {
        // OP_MSG with an empty BSON document (just 5 bytes: length + terminator)
        $bson = \pack('V', 5) . "\x00";
        $body = \pack('V', 0) . "\x00" . $bson;
        $header = \pack('V', 16 + \strlen($body))
            . \pack('V', 1)
            . \pack('V', 0)
            . \pack('V', 2013);

        $this->assertSame(Type::Unknown, $this->parser->parse($header . $body));
    }

    public function testClassifySqlReturnsUnknown(): void
    {
        $this->assertSame(Type::Unknown, $this->parser->classifySQL('SELECT * FROM users'));
    }

    public function testExtractKeywordReturnsEmpty(): void
    {
        $this->assertSame('', $this->parser->extractKeyword('SELECT'));
    }

    // -- Performance --

    public function testParsePerformance(): void
    {
        $data = $this->buildOpMsg(['find' => 'users', '$db' => 'mydb']);
        $iterations = 100_000;

        $start = \hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->parser->parse($data);
        }
        $elapsed = (\hrtime(true) - $start) / 1_000_000_000;
        $perQuery = ($elapsed / $iterations) * 1_000_000;

        $this->assertLessThan(
            2.0,
            $perQuery,
            \sprintf('MongoDB parse took %.3f us/query (target: < 2.0 us)', $perQuery)
        );
    }

    public function testTransactionScanPerformance(): void
    {
        // Document with many keys before startTransaction to test scanning
        $data = $this->buildOpMsg([
            'find' => 'users',
            '$db' => 'mydb',
            'filter' => ['active' => 1],
            'projection' => ['name' => 1],
            'sort' => ['created' => 1],
            'startTransaction' => true,
        ]);
        $iterations = 100_000;

        $start = \hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->parser->parse($data);
        }
        $elapsed = (\hrtime(true) - $start) / 1_000_000_000;
        $perQuery = ($elapsed / $iterations) * 1_000_000;

        $this->assertLessThan(
            5.0,
            $perQuery,
            \sprintf('MongoDB transaction scan took %.3f us/query (target: < 5.0 us)', $perQuery)
        );
    }
}
