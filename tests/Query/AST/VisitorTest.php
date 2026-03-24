<?php

namespace Tests\Query\AST;

use PHPUnit\Framework\TestCase;
use Utopia\Query\AST\AliasedExpr;
use Utopia\Query\AST\BinaryExpr;
use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\CteDefinition;
use Utopia\Query\AST\ExistsExpr;
use Utopia\Query\AST\FunctionCall;
use Utopia\Query\AST\InExpr;
use Utopia\Query\AST\JoinClause;
use Utopia\Query\AST\Literal;
use Utopia\Query\AST\OrderByItem;
use Utopia\Query\AST\Parser;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Serializer;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\SubqueryExpr;
use Utopia\Query\AST\SubquerySource;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\Walker;
use Utopia\Query\AST\Visitor\ColumnValidator;
use Utopia\Query\AST\Visitor\FilterInjector;
use Utopia\Query\AST\Visitor\TableRenamer;
use Utopia\Query\Exception;
use Utopia\Query\Tokenizer\Tokenizer;

class VisitorTest extends TestCase
{
    private function parse(string $sql): SelectStatement
    {
        $tokenizer = new Tokenizer();
        $tokens = Tokenizer::filter($tokenizer->tokenize($sql));
        $parser = new Parser();
        return $parser->parse($tokens);
    }

    private function serialize(SelectStatement $stmt): string
    {
        $serializer = new Serializer();
        return $serializer->serialize($stmt);
    }

    public function testTableRenamerSingleTable(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['users' => 'accounts']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT * FROM `accounts`', $this->serialize($result));
    }

    public function testTableRenamerInJoin(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users', 'u'),
            joins: [
                new JoinClause(
                    'JOIN',
                    new TableRef('orders', 'o'),
                    new BinaryExpr(
                        new ColumnRef('id', 'u'),
                        '=',
                        new ColumnRef('user_id', 'o'),
                    ),
                ),
            ],
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['orders' => 'purchases']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'SELECT * FROM `users` AS `u` JOIN `purchases` AS `o` ON `u`.`id` = `o`.`user_id`',
            $this->serialize($result),
        );
    }

    public function testTableRenamerInColumnRef(): void
    {
        $stmt = new SelectStatement(
            columns: [
                new ColumnRef('name', 'u'),
                new ColumnRef('email', 'u'),
            ],
            from: new TableRef('users', 'u'),
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['u' => 'a']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT `a`.`name`, `a`.`email` FROM `users` AS `a`', $this->serialize($result));
    }

    public function testTableRenamerInStar(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star('users')],
            from: new TableRef('users'),
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['users' => 'accounts']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT `accounts`.* FROM `accounts`', $this->serialize($result));
    }

    public function testTableRenamerMultiple(): void
    {
        $stmt = new SelectStatement(
            columns: [
                new ColumnRef('name', 'users'),
                new ColumnRef('title', 'orders'),
            ],
            from: new TableRef('users'),
            joins: [
                new JoinClause(
                    'JOIN',
                    new TableRef('orders'),
                    new BinaryExpr(
                        new ColumnRef('id', 'users'),
                        '=',
                        new ColumnRef('user_id', 'orders'),
                    ),
                ),
            ],
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['users' => 'accounts', 'orders' => 'purchases']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'SELECT `accounts`.`name`, `purchases`.`title` FROM `accounts` JOIN `purchases` ON `accounts`.`id` = `purchases`.`user_id`',
            $this->serialize($result),
        );
    }

    public function testTableRenamerNoMatch(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['products' => 'items']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT * FROM `users`', $this->serialize($result));
    }

    public function testColumnValidatorAllowed(): void
    {
        $stmt = new SelectStatement(
            columns: [
                new ColumnRef('name'),
                new ColumnRef('email'),
            ],
            from: new TableRef('users'),
        );

        $walker = new Walker();
        $visitor = new ColumnValidator(['name', 'email', 'id']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT `name`, `email` FROM `users`', $this->serialize($result));
    }

    public function testColumnValidatorDisallowed(): void
    {
        $stmt = new SelectStatement(
            columns: [
                new ColumnRef('name'),
                new ColumnRef('password'),
            ],
            from: new TableRef('users'),
        );

        $walker = new Walker();
        $visitor = new ColumnValidator(['name', 'email', 'id']);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Column 'password' is not in the allowed list");
        $walker->walk($stmt, $visitor);
    }

    public function testColumnValidatorInWhere(): void
    {
        $stmt = new SelectStatement(
            columns: [new ColumnRef('name')],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('secret'),
                '=',
                new Literal('foo'),
            ),
        );

        $walker = new Walker();
        $visitor = new ColumnValidator(['name', 'email']);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Column 'secret' is not in the allowed list");
        $walker->walk($stmt, $visitor);
    }

    public function testColumnValidatorInOrderBy(): void
    {
        $stmt = new SelectStatement(
            columns: [new ColumnRef('name')],
            from: new TableRef('users'),
            orderBy: [
                new OrderByItem(new ColumnRef('hidden')),
            ],
        );

        $walker = new Walker();
        $visitor = new ColumnValidator(['name', 'email']);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Column 'hidden' is not in the allowed list");
        $walker->walk($stmt, $visitor);
    }

    public function testFilterInjectorEmptyWhere(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
        );

        $condition = new BinaryExpr(
            new ColumnRef('active'),
            '=',
            new Literal(true),
        );

        $walker = new Walker();
        $visitor = new FilterInjector($condition);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame('SELECT * FROM `users` WHERE `active` = TRUE', $this->serialize($result));
    }

    public function testFilterInjectorExistingWhere(): void
    {
        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('age'),
                '>',
                new Literal(18),
            ),
        );

        $condition = new BinaryExpr(
            new ColumnRef('active'),
            '=',
            new Literal(true),
        );

        $walker = new Walker();
        $visitor = new FilterInjector($condition);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'SELECT * FROM `users` WHERE `age` > 18 AND `active` = TRUE',
            $this->serialize($result),
        );
    }

    public function testFilterInjectorPreservesOther(): void
    {
        $stmt = new SelectStatement(
            columns: [new ColumnRef('name')],
            from: new TableRef('users'),
            orderBy: [new OrderByItem(new ColumnRef('name'))],
            limit: new Literal(10),
        );

        $condition = new BinaryExpr(
            new ColumnRef('active'),
            '=',
            new Literal(true),
        );

        $walker = new Walker();
        $visitor = new FilterInjector($condition);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'SELECT `name` FROM `users` WHERE `active` = TRUE ORDER BY `name` ASC LIMIT 10',
            $this->serialize($result),
        );
    }

    public function testComposedVisitors(): void
    {
        $stmt = new SelectStatement(
            columns: [new ColumnRef('name')],
            from: new TableRef('users'),
        );

        $walker = new Walker();

        $result = $walker->walk($stmt, new TableRenamer(['users' => 'accounts']));
        $result = $walker->walk($result, new FilterInjector(
            new BinaryExpr(new ColumnRef('active'), '=', new Literal(true)),
        ));

        $this->assertSame(
            'SELECT `name` FROM `accounts` WHERE `active` = TRUE',
            $this->serialize($result),
        );
    }

    public function testVisitorWithSubquery(): void
    {
        $subquery = new SelectStatement(
            columns: [new ColumnRef('id')],
            from: new TableRef('orders'),
        );

        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new InExpr(
                new ColumnRef('id'),
                $subquery,
            ),
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['orders' => 'purchases']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'SELECT * FROM `users` WHERE `id` IN (SELECT `id` FROM `purchases`)',
            $this->serialize($result),
        );
    }

    public function testVisitorWithCte(): void
    {
        $cteQuery = new SelectStatement(
            columns: [new ColumnRef('id'), new ColumnRef('name')],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('active'),
                '=',
                new Literal(true),
            ),
        );

        $stmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('active_users'),
            ctes: [
                new CteDefinition('active_users', $cteQuery),
            ],
        );

        $walker = new Walker();
        $visitor = new TableRenamer(['users' => 'accounts']);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame(
            'WITH `active_users` AS (SELECT `id`, `name` FROM `accounts` WHERE `active` = TRUE) SELECT * FROM `active_users`',
            $this->serialize($result),
        );
    }

    public function testWalkerRoundTrip(): void
    {
        $stmt = new SelectStatement(
            columns: [
                new ColumnRef('name', 'u'),
                new AliasedExpr(new FunctionCall('COUNT', [new Star()]), 'total'),
            ],
            from: new TableRef('users', 'u'),
            joins: [
                new JoinClause(
                    'LEFT JOIN',
                    new TableRef('orders', 'o'),
                    new BinaryExpr(
                        new ColumnRef('id', 'u'),
                        '=',
                        new ColumnRef('user_id', 'o'),
                    ),
                ),
            ],
            where: new BinaryExpr(
                new ColumnRef('active', 'u'),
                '=',
                new Literal(true),
            ),
            groupBy: [new ColumnRef('name', 'u')],
            having: new BinaryExpr(
                new FunctionCall('COUNT', [new Star()]),
                '>',
                new Literal(5),
            ),
            orderBy: [new OrderByItem(new ColumnRef('name', 'u'))],
            limit: new Literal(10),
            offset: new Literal(0),
        );

        $before = $this->serialize($stmt);

        $walker = new Walker();
        $visitor = new TableRenamer([]);
        $result = $walker->walk($stmt, $visitor);

        $this->assertSame($before, $this->serialize($result));
    }
}
