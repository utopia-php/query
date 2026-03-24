<?php

namespace Tests\Query\AST;

use PHPUnit\Framework\TestCase;
use Utopia\Query\AST\AliasedExpr;
use Utopia\Query\AST\BetweenExpr;
use Utopia\Query\AST\BinaryExpr;
use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\CteDefinition;
use Utopia\Query\AST\FunctionCall;
use Utopia\Query\AST\InExpr;
use Utopia\Query\AST\JoinClause;
use Utopia\Query\AST\Literal;
use Utopia\Query\AST\OrderByItem;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\UnaryExpr;
use Utopia\Query\Builder\MySQL;
use Utopia\Query\Builder\PostgreSQL;
use Utopia\Query\Query;

class BuilderIntegrationTest extends TestCase
{
    public function testToAstSimpleSelect(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->select(['id', 'name', 'email']);

        $ast = $builder->toAst();

        $this->assertInstanceOf(SelectStatement::class, $ast);
        $this->assertInstanceOf(TableRef::class, $ast->from);
        $this->assertSame('users', $ast->from->name);
        $this->assertCount(3, $ast->columns);
        $this->assertInstanceOf(ColumnRef::class, $ast->columns[0]);
        $this->assertSame('id', $ast->columns[0]->name);
        $this->assertInstanceOf(ColumnRef::class, $ast->columns[1]);
        $this->assertSame('name', $ast->columns[1]->name);
        $this->assertInstanceOf(ColumnRef::class, $ast->columns[2]);
        $this->assertSame('email', $ast->columns[2]->name);
    }

    public function testToAstWithWhere(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BinaryExpr::class, $ast->where);
        $this->assertSame('AND', $ast->where->operator);

        $left = $ast->where->left;
        $this->assertInstanceOf(BinaryExpr::class, $left);
        $this->assertSame('=', $left->operator);
        $this->assertInstanceOf(ColumnRef::class, $left->left);
        $this->assertSame('status', $left->left->name);
        $this->assertInstanceOf(Literal::class, $left->right);
        $this->assertSame('active', $left->right->value);

        $right = $ast->where->right;
        $this->assertInstanceOf(BinaryExpr::class, $right);
        $this->assertSame('>', $right->operator);
    }

    public function testToAstWithJoin(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id');

        $ast = $builder->toAst();

        $this->assertCount(1, $ast->joins);
        $join = $ast->joins[0];
        $this->assertInstanceOf(JoinClause::class, $join);
        $this->assertSame('JOIN', $join->type);
        $this->assertInstanceOf(TableRef::class, $join->table);
        $this->assertSame('orders', $join->table->name);
        $this->assertNotNull($join->condition);
    }

    public function testToAstWithOrderBy(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->sortAsc('name')
            ->sortDesc('created_at');

        $ast = $builder->toAst();

        $this->assertCount(2, $ast->orderBy);
        $this->assertInstanceOf(OrderByItem::class, $ast->orderBy[0]);
        $this->assertSame('ASC', $ast->orderBy[0]->direction);
        $this->assertInstanceOf(ColumnRef::class, $ast->orderBy[0]->expr);
        $this->assertSame('name', $ast->orderBy[0]->expr->name);

        $this->assertSame('DESC', $ast->orderBy[1]->direction);
        $this->assertInstanceOf(ColumnRef::class, $ast->orderBy[1]->expr);
        $this->assertSame('created_at', $ast->orderBy[1]->expr->name);
    }

    public function testToAstWithGroupBy(): void
    {
        $builder = (new MySQL())
            ->from('orders')
            ->groupBy(['status', 'region']);

        $ast = $builder->toAst();

        $this->assertCount(2, $ast->groupBy);
        $this->assertInstanceOf(ColumnRef::class, $ast->groupBy[0]);
        $this->assertSame('status', $ast->groupBy[0]->name);
        $this->assertInstanceOf(ColumnRef::class, $ast->groupBy[1]);
        $this->assertSame('region', $ast->groupBy[1]->name);
    }

    public function testToAstWithHaving(): void
    {
        $builder = (new MySQL())
            ->from('orders')
            ->count('*', 'order_count')
            ->groupBy(['status'])
            ->having([Query::greaterThan('order_count', 5)]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->having);
    }

    public function testToAstWithLimitOffset(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->limit(10)
            ->offset(20);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->limit);
        $this->assertInstanceOf(Literal::class, $ast->limit);
        $this->assertSame(10, $ast->limit->value);

        $this->assertNotNull($ast->offset);
        $this->assertInstanceOf(Literal::class, $ast->offset);
        $this->assertSame(20, $ast->offset->value);
    }

    public function testToAstWithDistinct(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->distinct()
            ->select(['email']);

        $ast = $builder->toAst();

        $this->assertTrue($ast->distinct);
    }

    public function testToAstWithAggregates(): void
    {
        $builder = (new MySQL())
            ->from('orders')
            ->count('*', 'total_count')
            ->sum('amount', 'total_amount');

        $ast = $builder->toAst();

        $this->assertCount(2, $ast->columns);

        $countCol = $ast->columns[0];
        $this->assertInstanceOf(AliasedExpr::class, $countCol);
        $this->assertSame('total_count', $countCol->alias);
        $this->assertInstanceOf(FunctionCall::class, $countCol->expr);
        $this->assertSame('COUNT', $countCol->expr->name);

        $sumCol = $ast->columns[1];
        $this->assertInstanceOf(AliasedExpr::class, $sumCol);
        $this->assertSame('total_amount', $sumCol->alias);
        $this->assertInstanceOf(FunctionCall::class, $sumCol->expr);
        $this->assertSame('SUM', $sumCol->expr->name);
    }

    public function testFromAstSimpleSelect(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertSame('SELECT * FROM `users`', $result->query);
    }

    public function testFromAstWithWhere(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('status'),
                '=',
                new Literal('active'),
            ),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('`status`', $result->query);
    }

    public function testFromAstWithJoin(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            joins: [
                new JoinClause(
                    'LEFT JOIN',
                    new TableRef('orders'),
                    new BinaryExpr(
                        new ColumnRef('id', 'users'),
                        '=',
                        new ColumnRef('user_id', 'orders'),
                    ),
                ),
            ],
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('LEFT JOIN', $result->query);
        $this->assertStringContainsString('`orders`', $result->query);
    }

    public function testFromAstWithOrderBy(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            orderBy: [
                new OrderByItem(new ColumnRef('name'), 'ASC'),
                new OrderByItem(new ColumnRef('age'), 'DESC'),
            ],
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('ORDER BY', $result->query);
        $this->assertStringContainsString('`name` ASC', $result->query);
        $this->assertStringContainsString('`age` DESC', $result->query);
    }

    public function testFromAstWithLimitOffset(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            limit: new Literal(25),
            offset: new Literal(50),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('LIMIT', $result->query);
        $this->assertStringContainsString('OFFSET', $result->query);
        $this->assertContains(25, $result->bindings);
        $this->assertContains(50, $result->bindings);
    }

    public function testRoundTripBuilderToAst(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 21),
            ])
            ->sortAsc('name')
            ->limit(10)
            ->offset(0);

        $original = $builder->build();

        $ast = $builder->toAst();
        $rebuilt = MySQL::fromAst($ast);
        $result = $rebuilt->build();

        $this->assertSame($original->query, $result->query);
        $this->assertSame($original->bindings, $result->bindings);
    }

    public function testRoundTripAstToBuilder(): void
    {
        $ast = new SelectStatement(
            columns: [new ColumnRef('id'), new ColumnRef('name')],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('age'),
                '>',
                new Literal(18),
            ),
            orderBy: [new OrderByItem(new ColumnRef('name'), 'ASC')],
            limit: new Literal(10),
        );

        $builder = MySQL::fromAst($ast);
        $result1 = $builder->build();

        $ast2 = $builder->toAst();
        $builder2 = MySQL::fromAst($ast2);
        $result2 = $builder2->build();

        $this->assertSame($result1->query, $result2->query);
        $this->assertSame($result1->bindings, $result2->bindings);
    }

    public function testFromAstComplexQuery(): void
    {
        $ast = new SelectStatement(
            columns: [
                new ColumnRef('id'),
                new AliasedExpr(new FunctionCall('COUNT', [new Star()]), 'order_count'),
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
                new ColumnRef('status'),
                '=',
                new Literal('active'),
            ),
            groupBy: [new ColumnRef('id')],
            orderBy: [new OrderByItem(new FunctionCall('COUNT', [new Star()]), 'DESC')],
            limit: new Literal(10),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('SELECT', $result->query);
        $this->assertStringContainsString('LEFT JOIN', $result->query);
        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('ORDER BY', $result->query);
        $this->assertStringContainsString('LIMIT', $result->query);
    }

    public function testFromAstWithCte(): void
    {
        $innerStmt = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new ColumnRef('active'),
                '=',
                new Literal(true),
            ),
        );

        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('active_users'),
            ctes: [
                new CteDefinition('active_users', $innerStmt),
            ],
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('WITH', $result->query);
        $this->assertStringContainsString('`active_users`', $result->query);
    }

    public function testMySQLDialect(): void
    {
        $builder = (new MySQL())
            ->from('products')
            ->select(['name', 'price'])
            ->filter([Query::greaterThan('price', 100)])
            ->sortDesc('price')
            ->limit(5);

        $ast = $builder->toAst();
        $rebuilt = MySQL::fromAst($ast);
        $result = $rebuilt->build();

        $original = $builder->build();

        $this->assertSame($original->query, $result->query);
        $this->assertSame($original->bindings, $result->bindings);
    }

    public function testPostgreSQLDialect(): void
    {
        $builder = (new PostgreSQL())
            ->from('products')
            ->select(['name', 'price'])
            ->filter([Query::greaterThan('price', 100)])
            ->sortDesc('price')
            ->limit(5);

        $ast = $builder->toAst();
        $rebuilt = PostgreSQL::fromAst($ast);
        $result = $rebuilt->build();

        $original = $builder->build();

        $this->assertSame($original->query, $result->query);
        $this->assertSame($original->bindings, $result->bindings);
    }

    public function testToAstEqualWithMultipleValues(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::equal('status', ['active', 'pending', 'review'])]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(InExpr::class, $ast->where);
        $this->assertFalse($ast->where->negated);
    }

    public function testToAstNotEqual(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::notEqual('status', 'deleted')]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
    }

    public function testToAstBetween(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::between('age', 18, 65)]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BetweenExpr::class, $ast->where);
        $this->assertFalse($ast->where->negated);
    }

    public function testToAstIsNull(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::isNull('deleted_at')]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(UnaryExpr::class, $ast->where);
        $this->assertSame('IS NULL', $ast->where->operator);
    }

    public function testToAstIsNotNull(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::isNotNull('email')]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(UnaryExpr::class, $ast->where);
        $this->assertSame('IS NOT NULL', $ast->where->operator);
    }

    public function testToAstWithTableAlias(): void
    {
        $builder = (new MySQL())
            ->from('users', 'u')
            ->select(['id']);

        $ast = $builder->toAst();

        $this->assertInstanceOf(TableRef::class, $ast->from);
        $this->assertSame('users', $ast->from->name);
        $this->assertSame('u', $ast->from->alias);
    }

    public function testToAstLeftJoin(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->leftJoin('orders', 'users.id', 'orders.user_id');

        $ast = $builder->toAst();

        $this->assertCount(1, $ast->joins);
        $this->assertSame('LEFT JOIN', $ast->joins[0]->type);
    }

    public function testToAstCrossJoin(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->crossJoin('roles');

        $ast = $builder->toAst();

        $this->assertCount(1, $ast->joins);
        $this->assertSame('CROSS JOIN', $ast->joins[0]->type);
        $this->assertNull($ast->joins[0]->condition);
    }

    public function testToAstNoColumns(): void
    {
        $builder = (new MySQL())
            ->from('users');

        $ast = $builder->toAst();

        $this->assertCount(1, $ast->columns);
        $this->assertInstanceOf(Star::class, $ast->columns[0]);
    }

    public function testFromAstWithDistinct(): void
    {
        $ast = new SelectStatement(
            columns: [new ColumnRef('email')],
            from: new TableRef('users'),
            distinct: true,
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
    }

    public function testFromAstWithGroupBy(): void
    {
        $ast = new SelectStatement(
            columns: [
                new ColumnRef('department'),
                new AliasedExpr(new FunctionCall('COUNT', [new Star()]), 'cnt'),
            ],
            from: new TableRef('employees'),
            groupBy: [new ColumnRef('department')],
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('`department`', $result->query);
    }

    public function testFromAstWithBetween(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new BetweenExpr(
                new ColumnRef('age'),
                new Literal(18),
                new Literal(65),
            ),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('BETWEEN', $result->query);
    }

    public function testFromAstWithInExpr(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new InExpr(
                new ColumnRef('status'),
                [new Literal('active'), new Literal('pending')],
            ),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('IN', $result->query);
    }

    public function testFromAstAndCombinedFilters(): void
    {
        $ast = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            where: new BinaryExpr(
                new BinaryExpr(new ColumnRef('age'), '>', new Literal(18)),
                'AND',
                new BinaryExpr(new ColumnRef('status'), '=', new Literal('active')),
            ),
        );

        $builder = MySQL::fromAst($ast);
        $result = $builder->build();

        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testToAstNotBetween(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::notBetween('age', 0, 17)]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BetweenExpr::class, $ast->where);
        $this->assertTrue($ast->where->negated);
    }

    public function testToAstStartsWith(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::startsWith('name', 'Jo')]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BinaryExpr::class, $ast->where);
        $this->assertSame('LIKE', $ast->where->operator);
    }

    public function testToAstEndsWith(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([Query::endsWith('email', '.com')]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BinaryExpr::class, $ast->where);
        $this->assertSame('LIKE', $ast->where->operator);
    }

    public function testToAstOrGroup(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([
                Query::or([
                    Query::equal('status', ['active']),
                    Query::equal('status', ['pending']),
                ]),
            ]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BinaryExpr::class, $ast->where);
        $this->assertSame('OR', $ast->where->operator);
    }

    public function testToAstAndGroup(): void
    {
        $builder = (new MySQL())
            ->from('users')
            ->filter([
                Query::and([
                    Query::greaterThan('age', 18),
                    Query::equal('verified', [true]),
                ]),
            ]);

        $ast = $builder->toAst();

        $this->assertNotNull($ast->where);
        $this->assertInstanceOf(BinaryExpr::class, $ast->where);
        $this->assertSame('AND', $ast->where->operator);
    }

    public function testToAstCountDistinct(): void
    {
        $builder = (new MySQL())
            ->from('orders')
            ->countDistinct('user_id', 'unique_users');

        $ast = $builder->toAst();

        $this->assertCount(1, $ast->columns);
        $col = $ast->columns[0];
        $this->assertInstanceOf(AliasedExpr::class, $col);
        $this->assertSame('unique_users', $col->alias);
        $this->assertInstanceOf(FunctionCall::class, $col->expr);
        $this->assertSame('COUNT', $col->expr->name);
        $this->assertTrue($col->expr->distinct);
    }
}
