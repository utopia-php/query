<?php

namespace Tests\Query\Tokenizer;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Tokenizer\MySQL;
use Utopia\Query\Tokenizer\Token;
use Utopia\Query\Tokenizer\Tokenizer;
use Utopia\Query\Tokenizer\TokenType;

class MySQLTest extends TestCase
{
    private MySQL $tokenizer;

    protected function setUp(): void
    {
        $this->tokenizer = new MySQL();
    }

    /**
     * @return Token[]
     */
    private function meaningful(string $sql): array
    {
        return Tokenizer::filter($this->tokenizer->tokenize($sql));
    }

    /**
     * @param Token[] $tokens
     * @return TokenType[]
     */
    private function types(array $tokens): array
    {
        return array_map(fn(Token $t) => $t->type, $tokens);
    }

    /**
     * @param Token[] $tokens
     * @return string[]
     */
    private function values(array $tokens): array
    {
        return array_map(fn(Token $t) => $t->value, $tokens);
    }

    public function testHashComment(): void
    {
        $all = $this->tokenizer->tokenize("SELECT * # comment\nFROM users");

        $comments = array_values(array_filter(
            $all,
            fn(Token $t) => $t->type === TokenType::LineComment
        ));

        $this->assertCount(1, $comments);
        $this->assertSame('-- comment', $comments[0]->value);
    }

    public function testHashCommentFilteredOut(): void
    {
        $all = $this->tokenizer->tokenize("SELECT * # this is a hash comment\nFROM users");
        $filtered = Tokenizer::filter($all);

        $types = $this->types($filtered);
        $this->assertNotContains(TokenType::LineComment, $types);

        $this->assertSame(
            [TokenType::Keyword, TokenType::Star, TokenType::Keyword, TokenType::Identifier, TokenType::Eof],
            $types
        );
    }

    public function testBacktickQuoting(): void
    {
        $tokens = $this->meaningful('SELECT `name` FROM `users`');

        $this->assertSame(
            [TokenType::Keyword, TokenType::QuotedIdentifier, TokenType::Keyword, TokenType::QuotedIdentifier, TokenType::Eof],
            $this->types($tokens)
        );
        $this->assertSame('`name`', $tokens[1]->value);
        $this->assertSame('`users`', $tokens[3]->value);
    }
}
