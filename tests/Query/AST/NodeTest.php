<?php

namespace Tests\Query\AST;

use PHPUnit\Framework\TestCase;
use Utopia\Query\AST\AliasedExpr;
use Utopia\Query\AST\BetweenExpr;
use Utopia\Query\AST\BinaryExpr;
use Utopia\Query\AST\CaseExpr;
use Utopia\Query\AST\CaseWhen;
use Utopia\Query\AST\CastExpr;
use Utopia\Query\AST\ColumnRef;
use Utopia\Query\AST\CteDefinition;
use Utopia\Query\AST\ExistsExpr;
use Utopia\Query\AST\Expr;
use Utopia\Query\AST\FunctionCall;
use Utopia\Query\AST\InExpr;
use Utopia\Query\AST\JoinClause;
use Utopia\Query\AST\Literal;
use Utopia\Query\AST\OrderByItem;
use Utopia\Query\AST\Placeholder;
use Utopia\Query\AST\Raw;
use Utopia\Query\AST\SelectStatement;
use Utopia\Query\AST\Star;
use Utopia\Query\AST\SubqueryExpr;
use Utopia\Query\AST\SubquerySource;
use Utopia\Query\AST\TableRef;
use Utopia\Query\AST\UnaryExpr;
use Utopia\Query\AST\WindowDefinition;
use Utopia\Query\AST\WindowExpr;
use Utopia\Query\AST\WindowSpec;

class NodeTest extends TestCase
{
    public function testColumnRef(): void
    {
        $col = new ColumnRef('id');
        $this->assertInstanceOf(Expr::class, $col);
        $this->assertSame('id', $col->name);
        $this->assertNull($col->table);
        $this->assertNull($col->schema);

        $col = new ColumnRef('id', 'users');
        $this->assertSame('id', $col->name);
        $this->assertSame('users', $col->table);
        $this->assertNull($col->schema);

        $col = new ColumnRef('id', 'users', 'public');
        $this->assertSame('id', $col->name);
        $this->assertSame('users', $col->table);
        $this->assertSame('public', $col->schema);
    }

    public function testLiteral(): void
    {
        $str = new Literal('hello');
        $this->assertInstanceOf(Expr::class, $str);
        $this->assertSame('hello', $str->value);

        $int = new Literal(42);
        $this->assertSame(42, $int->value);

        $float = new Literal(3.14);
        $this->assertSame(3.14, $float->value);

        $bool = new Literal(true);
        $this->assertSame(true, $bool->value);

        $null = new Literal(null);
        $this->assertNull($null->value);
    }

    public function testStar(): void
    {
        $star = new Star();
        $this->assertInstanceOf(Expr::class, $star);
        $this->assertNull($star->table);

        $star = new Star('users');
        $this->assertSame('users', $star->table);
    }

    public function testPlaceholder(): void
    {
        $q = new Placeholder('?');
        $this->assertInstanceOf(Expr::class, $q);
        $this->assertSame('?', $q->value);

        $named = new Placeholder(':name');
        $this->assertSame(':name', $named->value);

        $numbered = new Placeholder('$1');
        $this->assertSame('$1', $numbered->value);
    }

    public function testRaw(): void
    {
        $raw = new Raw('NOW() + INTERVAL 1 DAY');
        $this->assertInstanceOf(Expr::class, $raw);
        $this->assertSame('NOW() + INTERVAL 1 DAY', $raw->sql);
    }

    public function testBinaryExpr(): void
    {
        $left = new ColumnRef('age');
        $right = new Literal(18);
        $expr = new BinaryExpr($left, '>=', $right);

        $this->assertInstanceOf(Expr::class, $expr);
        $this->assertSame($left, $expr->left);
        $this->assertSame('>=', $expr->operator);
        $this->assertSame($right, $expr->right);

        $and = new BinaryExpr(
            new BinaryExpr(new ColumnRef('a'), '=', new Literal(1)),
            'AND',
            new BinaryExpr(new ColumnRef('b'), '=', new Literal(2)),
        );
        $this->assertSame('AND', $and->operator);
    }

    public function testUnaryExprPrefix(): void
    {
        $operand = new ColumnRef('active');
        $not = new UnaryExpr('NOT', $operand);

        $this->assertInstanceOf(Expr::class, $not);
        $this->assertSame('NOT', $not->operator);
        $this->assertSame($operand, $not->operand);
        $this->assertTrue($not->prefix);

        $neg = new UnaryExpr('-', new Literal(5));
        $this->assertSame('-', $neg->operator);
        $this->assertTrue($neg->prefix);
    }

    public function testUnaryExprPostfix(): void
    {
        $operand = new ColumnRef('deleted_at');
        $isNull = new UnaryExpr('IS NULL', $operand, false);

        $this->assertInstanceOf(Expr::class, $isNull);
        $this->assertSame('IS NULL', $isNull->operator);
        $this->assertSame($operand, $isNull->operand);
        $this->assertFalse($isNull->prefix);

        $isNotNull = new UnaryExpr('IS NOT NULL', $operand, false);
        $this->assertSame('IS NOT NULL', $isNotNull->operator);
        $this->assertFalse($isNotNull->prefix);
    }

    public function testFunctionCall(): void
    {
        $fn = new FunctionCall('UPPER', [new ColumnRef('name')]);
        $this->assertInstanceOf(Expr::class, $fn);
        $this->assertSame('UPPER', $fn->name);
        $this->assertCount(1, $fn->arguments);
        $this->assertFalse($fn->distinct);

        $noArgs = new FunctionCall('NOW');
        $this->assertSame('NOW', $noArgs->name);
        $this->assertSame([], $noArgs->arguments);
    }

    public function testFunctionCallDistinct(): void
    {
        $count = new FunctionCall('COUNT', [new ColumnRef('id')], true);
        $this->assertSame('COUNT', $count->name);
        $this->assertTrue($count->distinct);
        $this->assertCount(1, $count->arguments);
    }

    public function testInExpr(): void
    {
        $col = new ColumnRef('status');
        $list = [new Literal('active'), new Literal('pending')];
        $in = new InExpr($col, $list);

        $this->assertInstanceOf(Expr::class, $in);
        $this->assertSame($col, $in->expr);
        $this->assertSame($list, $in->list);
        $this->assertFalse($in->negated);

        $notIn = new InExpr($col, $list, true);
        $this->assertTrue($notIn->negated);

        $subquery = new SelectStatement(
            columns: [new ColumnRef('id')],
            from: new TableRef('other'),
        );
        $inSub = new InExpr($col, $subquery);
        $this->assertInstanceOf(SelectStatement::class, $inSub->list);
    }

    public function testBetweenExpr(): void
    {
        $col = new ColumnRef('age');
        $low = new Literal(18);
        $high = new Literal(65);
        $between = new BetweenExpr($col, $low, $high);

        $this->assertInstanceOf(Expr::class, $between);
        $this->assertSame($col, $between->expr);
        $this->assertSame($low, $between->low);
        $this->assertSame($high, $between->high);
        $this->assertFalse($between->negated);

        $notBetween = new BetweenExpr($col, $low, $high, true);
        $this->assertTrue($notBetween->negated);
    }

    public function testExistsExpr(): void
    {
        $subquery = new SelectStatement(
            columns: [new Literal(1)],
            from: new TableRef('users'),
            where: new BinaryExpr(new ColumnRef('id'), '=', new Literal(1)),
        );

        $exists = new ExistsExpr($subquery);
        $this->assertInstanceOf(Expr::class, $exists);
        $this->assertSame($subquery, $exists->subquery);
        $this->assertFalse($exists->negated);

        $notExists = new ExistsExpr($subquery, true);
        $this->assertTrue($notExists->negated);
    }

    public function testCaseExpr(): void
    {
        $whens = [
            new CaseWhen(
                new BinaryExpr(new ColumnRef('status'), '=', new Literal('active')),
                new Literal(1),
            ),
            new CaseWhen(
                new BinaryExpr(new ColumnRef('status'), '=', new Literal('inactive')),
                new Literal(0),
            ),
        ];
        $else = new Literal(-1);
        $searched = new CaseExpr(null, $whens, $else);

        $this->assertInstanceOf(Expr::class, $searched);
        $this->assertNull($searched->operand);
        $this->assertCount(2, $searched->whens);
        $this->assertSame($else, $searched->else);

        $simple = new CaseExpr(new ColumnRef('status'), $whens);
        $this->assertInstanceOf(Expr::class, $simple);
        $this->assertNotNull($simple->operand);
        $this->assertNull($simple->else);
    }

    public function testCastExpr(): void
    {
        $expr = new ColumnRef('price');
        $cast = new CastExpr($expr, 'INTEGER');

        $this->assertInstanceOf(Expr::class, $cast);
        $this->assertSame($expr, $cast->expr);
        $this->assertSame('INTEGER', $cast->type);
    }

    public function testAliasedExpr(): void
    {
        $expr = new FunctionCall('COUNT', [new Star()]);
        $aliased = new AliasedExpr($expr, 'total');

        $this->assertInstanceOf(Expr::class, $aliased);
        $this->assertSame($expr, $aliased->expr);
        $this->assertSame('total', $aliased->alias);
    }

    public function testSubqueryExpr(): void
    {
        $query = new SelectStatement(
            columns: [new FunctionCall('MAX', [new ColumnRef('salary')])],
            from: new TableRef('employees'),
        );
        $sub = new SubqueryExpr($query);

        $this->assertInstanceOf(Expr::class, $sub);
        $this->assertSame($query, $sub->query);
    }

    public function testWindowExpr(): void
    {
        $fn = new FunctionCall('ROW_NUMBER');
        $spec = new WindowSpec(
            partitionBy: [new ColumnRef('department')],
            orderBy: [new OrderByItem(new ColumnRef('salary'), 'DESC')],
        );
        $window = new WindowExpr($fn, spec: $spec);

        $this->assertInstanceOf(Expr::class, $window);
        $this->assertSame($fn, $window->function);
        $this->assertNull($window->windowName);
        $this->assertSame($spec, $window->spec);

        $namedWindow = new WindowExpr($fn, windowName: 'w');
        $this->assertSame('w', $namedWindow->windowName);
        $this->assertNull($namedWindow->spec);
    }

    public function testWindowSpec(): void
    {
        $spec = new WindowSpec();
        $this->assertSame([], $spec->partitionBy);
        $this->assertSame([], $spec->orderBy);
        $this->assertNull($spec->frameType);
        $this->assertNull($spec->frameStart);
        $this->assertNull($spec->frameEnd);

        $spec = new WindowSpec(
            partitionBy: [new ColumnRef('dept')],
            orderBy: [new OrderByItem(new ColumnRef('hire_date'))],
            frameType: 'ROWS',
            frameStart: 'UNBOUNDED PRECEDING',
            frameEnd: 'CURRENT ROW',
        );
        $this->assertCount(1, $spec->partitionBy);
        $this->assertCount(1, $spec->orderBy);
        $this->assertSame('ROWS', $spec->frameType);
        $this->assertSame('UNBOUNDED PRECEDING', $spec->frameStart);
        $this->assertSame('CURRENT ROW', $spec->frameEnd);
    }

    public function testTableRef(): void
    {
        $table = new TableRef('users');
        $this->assertSame('users', $table->name);
        $this->assertNull($table->alias);
        $this->assertNull($table->schema);

        $aliased = new TableRef('users', 'u');
        $this->assertSame('users', $aliased->name);
        $this->assertSame('u', $aliased->alias);

        $schemed = new TableRef('users', 'u', 'public');
        $this->assertSame('public', $schemed->schema);
    }

    public function testSubquerySource(): void
    {
        $query = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
        );
        $source = new SubquerySource($query, 'sub');

        $this->assertSame($query, $source->query);
        $this->assertSame('sub', $source->alias);
    }

    public function testJoinClause(): void
    {
        $table = new TableRef('orders', 'o');
        $condition = new BinaryExpr(
            new ColumnRef('id', 'u'),
            '=',
            new ColumnRef('user_id', 'o'),
        );

        $join = new JoinClause('JOIN', $table, $condition);
        $this->assertSame('JOIN', $join->type);
        $this->assertSame($table, $join->table);
        $this->assertSame($condition, $join->condition);

        $leftJoin = new JoinClause('LEFT JOIN', $table, $condition);
        $this->assertSame('LEFT JOIN', $leftJoin->type);

        $cross = new JoinClause('CROSS JOIN', $table);
        $this->assertNull($cross->condition);

        $subSource = new SubquerySource(
            new SelectStatement(columns: [new Star()], from: new TableRef('items')),
            'i',
        );
        $subJoin = new JoinClause('LEFT JOIN', $subSource, $condition);
        $this->assertInstanceOf(SubquerySource::class, $subJoin->table);
    }

    public function testOrderByItem(): void
    {
        $item = new OrderByItem(new ColumnRef('name'));
        $this->assertSame('ASC', $item->direction);
        $this->assertNull($item->nulls);

        $desc = new OrderByItem(new ColumnRef('created_at'), 'DESC');
        $this->assertSame('DESC', $desc->direction);

        $nullsFirst = new OrderByItem(new ColumnRef('score'), 'ASC', 'FIRST');
        $this->assertSame('FIRST', $nullsFirst->nulls);

        $nullsLast = new OrderByItem(new ColumnRef('score'), 'DESC', 'LAST');
        $this->assertSame('LAST', $nullsLast->nulls);
    }

    public function testWindowDefinition(): void
    {
        $spec = new WindowSpec(
            partitionBy: [new ColumnRef('dept')],
            orderBy: [new OrderByItem(new ColumnRef('salary'), 'DESC')],
        );
        $def = new WindowDefinition('w', $spec);

        $this->assertSame('w', $def->name);
        $this->assertSame($spec, $def->spec);
    }

    public function testCteDefinition(): void
    {
        $query = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('employees'),
            where: new BinaryExpr(new ColumnRef('active'), '=', new Literal(true)),
        );

        $cte = new CteDefinition('active_employees', $query);
        $this->assertSame('active_employees', $cte->name);
        $this->assertSame($query, $cte->query);
        $this->assertSame([], $cte->columns);
        $this->assertFalse($cte->recursive);

        $cteWithCols = new CteDefinition('ranked', $query, ['id', 'name', 'rank']);
        $this->assertSame(['id', 'name', 'rank'], $cteWithCols->columns);

        $recursive = new CteDefinition('hierarchy', $query, recursive: true);
        $this->assertTrue($recursive->recursive);
    }

    public function testSelectStatement(): void
    {
        $select = new SelectStatement(
            columns: [
                new ColumnRef('name', 'u'),
                new AliasedExpr(new FunctionCall('COUNT', [new Star()]), 'order_count'),
            ],
            from: new TableRef('users', 'u'),
            joins: [
                new JoinClause(
                    'LEFT JOIN',
                    new TableRef('orders', 'o'),
                    new BinaryExpr(new ColumnRef('id', 'u'), '=', new ColumnRef('user_id', 'o')),
                ),
            ],
            where: new BinaryExpr(new ColumnRef('active', 'u'), '=', new Literal(true)),
            groupBy: [new ColumnRef('name', 'u')],
            having: new BinaryExpr(
                new FunctionCall('COUNT', [new Star()]),
                '>',
                new Literal(5),
            ),
            orderBy: [new OrderByItem(new ColumnRef('name', 'u'))],
            limit: new Literal(10),
            offset: new Literal(0),
            distinct: true,
        );

        $this->assertCount(2, $select->columns);
        $this->assertInstanceOf(TableRef::class, $select->from);
        $this->assertCount(1, $select->joins);
        $this->assertNotNull($select->where);
        $this->assertCount(1, $select->groupBy);
        $this->assertNotNull($select->having);
        $this->assertCount(1, $select->orderBy);
        $this->assertNotNull($select->limit);
        $this->assertNotNull($select->offset);
        $this->assertTrue($select->distinct);
        $this->assertSame([], $select->ctes);
        $this->assertSame([], $select->windows);
    }

    public function testSelectStatementWith(): void
    {
        $original = new SelectStatement(
            columns: [new Star()],
            from: new TableRef('users'),
            limit: new Literal(10),
            distinct: false,
        );

        $modified = $original->with(
            limit: new Literal(20),
            distinct: true,
        );

        $this->assertNotSame($original, $modified);
        $this->assertSame($original->columns, $modified->columns);
        $this->assertSame($original->from, $modified->from);
        $this->assertTrue($modified->distinct);
        $this->assertInstanceOf(Literal::class, $modified->limit);
        $this->assertSame(20, $modified->limit->value);
        $this->assertSame(10, $original->limit->value);
        $this->assertFalse($original->distinct);

        $withWhere = $original->with(
            where: new BinaryExpr(new ColumnRef('id'), '=', new Literal(1)),
        );
        $this->assertNotNull($withWhere->where);
        $this->assertNull($original->where);

        $withNullWhere = $withWhere->with(where: null);
        $this->assertNull($withNullWhere->where);

        $withFrom = $original->with(from: null);
        $this->assertNull($withFrom->from);
        $this->assertNotNull($original->from);

        $unchanged = $original->with();
        $this->assertNotSame($original, $unchanged);
        $this->assertSame($original->columns, $unchanged->columns);
        $this->assertSame($original->from, $unchanged->from);
        $this->assertSame($original->limit, $unchanged->limit);

        $withCtes = $original->with(
            ctes: [
                new CteDefinition('sub', new SelectStatement(columns: [new Literal(1)])),
            ],
        );
        $this->assertCount(1, $withCtes->ctes);
        $this->assertSame([], $original->ctes);

        $withWindows = $original->with(
            windows: [
                new WindowDefinition('w', new WindowSpec(
                    orderBy: [new OrderByItem(new ColumnRef('id'))],
                )),
            ],
        );
        $this->assertCount(1, $withWindows->windows);
    }
}
