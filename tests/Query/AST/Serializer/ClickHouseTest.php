<?php

namespace Tests\Query\AST\Serializer;

use PHPUnit\Framework\TestCase;
use Utopia\Query\AST\Parser;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Serializer\ClickHouse;
use Utopia\Query\Tokenizer\Tokenizer;

class ClickHouseTest extends TestCase
{
    private function parse(string $sql): SelectStatement
    {
        $tokenizer = new \Utopia\Query\Tokenizer\ClickHouse();
        $tokens = Tokenizer::filter($tokenizer->tokenize($sql));
        $parser = new Parser();
        return $parser->parse($tokens);
    }

    private function serialize(SelectStatement $stmt): string
    {
        $serializer = new ClickHouse();
        return $serializer->serialize($stmt);
    }

    private function roundTrip(string $sql): string
    {
        return $this->serialize($this->parse($sql));
    }

    public function testBacktickQuoting(): void
    {
        $serializer = new ClickHouse();
        $stmt = $this->parse('SELECT name, email FROM events');
        $result = $serializer->serialize($stmt);

        $this->assertSame('SELECT `name`, `email` FROM `events`', $result);
    }

    public function testRoundTrip(): void
    {
        $result = $this->roundTrip("SELECT user_id, COUNT(*) AS cnt FROM events WHERE type = 'click' GROUP BY user_id ORDER BY cnt DESC LIMIT 100");

        $expected = "SELECT `user_id`, COUNT(*) AS `cnt` FROM `events` WHERE `type` = 'click' GROUP BY `user_id` ORDER BY `cnt` DESC LIMIT 100";

        $this->assertSame($expected, $result);
    }
}
