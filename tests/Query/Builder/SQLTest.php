<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\SQL as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Query;

class SQLTest extends TestCase
{
    // ── Compiler compliance ──

    public function testImplementsCompiler(): void
    {
        $builder = new Builder();
        $this->assertInstanceOf(Compiler::class, $builder);
    }

    public function testStandaloneCompile(): void
    {
        $builder = new Builder();

        $filter = Query::greaterThan('age', 18);
        $sql = $filter->compile($builder);
        $this->assertEquals('`age` > ?', $sql);
        $this->assertEquals([18], $builder->getBindings());
    }

    // ── Fluent API ──

    public function testFluentSelectFromFilterSortLimitOffset(): void
    {
        $result = (new Builder())
            ->select(['name', 'email'])
            ->from('users')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])
            ->sortAsc('name')
            ->limit(25)
            ->offset(0)
            ->build();

        $this->assertEquals(
            'SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals(['active', 18, 25, 0], $result['bindings']);
    }

    // ── Batch mode ──

    public function testBatchModeProducesSameOutput(): void
    {
        $result = (new Builder())
            ->from('users')
            ->queries([
                Query::select(['name', 'email']),
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
                Query::orderAsc('name'),
                Query::limit(25),
                Query::offset(0),
            ])
            ->build();

        $this->assertEquals(
            'SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals(['active', 18, 25, 0], $result['bindings']);
    }

    // ── Filter types ──

    public function testEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active', 'pending'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?, ?)', $result['query']);
        $this->assertEquals(['active', 'pending'], $result['bindings']);
    }

    public function testNotEqualSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', 'guest')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `role` != ?', $result['query']);
        $this->assertEquals(['guest'], $result['bindings']);
    }

    public function testNotEqualMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', ['guest', 'banned'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `role` NOT IN (?, ?)', $result['query']);
        $this->assertEquals(['guest', 'banned'], $result['bindings']);
    }

    public function testLessThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('price', 100)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `price` < ?', $result['query']);
        $this->assertEquals([100], $result['bindings']);
    }

    public function testLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThanEqual('price', 100)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `price` <= ?', $result['query']);
        $this->assertEquals([100], $result['bindings']);
    }

    public function testGreaterThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('age', 18)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `age` > ?', $result['query']);
        $this->assertEquals([18], $result['bindings']);
    }

    public function testGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 90)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `score` >= ?', $result['query']);
        $this->assertEquals([90], $result['bindings']);
    }

    public function testBetween(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 18, 65)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([18, 65], $result['bindings']);
    }

    public function testNotBetween(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notBetween('age', 18, 65)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `age` NOT BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([18, 65], $result['bindings']);
    }

    public function testStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', 'Jo')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result['query']);
        $this->assertEquals(['Jo%'], $result['bindings']);
    }

    public function testNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notStartsWith('name', 'Jo')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` NOT LIKE ?', $result['query']);
        $this->assertEquals(['Jo%'], $result['bindings']);
    }

    public function testEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('email', '.com')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `email` LIKE ?', $result['query']);
        $this->assertEquals(['%.com'], $result['bindings']);
    }

    public function testNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEndsWith('email', '.com')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `email` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%.com'], $result['bindings']);
    }

    public function testContainsSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result['query']);
        $this->assertEquals(['%php%'], $result['bindings']);
    }

    public function testContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php', 'js'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`bio` LIKE ? OR `bio` LIKE ?)', $result['query']);
        $this->assertEquals(['%php%', '%js%'], $result['bindings']);
    }

    public function testContainsAny(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAny('tags', ['a', 'b'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `tags` IN (?, ?)', $result['query']);
        $this->assertEquals(['a', 'b'], $result['bindings']);
    }

    public function testContainsAll(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('perms', ['read', 'write'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`perms` LIKE ? AND `perms` LIKE ?)', $result['query']);
        $this->assertEquals(['%read%', '%write%'], $result['bindings']);
    }

    public function testNotContainsSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['php'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%php%'], $result['bindings']);
    }

    public function testNotContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['php', 'js'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`bio` NOT LIKE ? AND `bio` NOT LIKE ?)', $result['query']);
        $this->assertEquals(['%php%', '%js%'], $result['bindings']);
    }

    public function testSearch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?)', $result['query']);
        $this->assertEquals(['hello'], $result['bindings']);
    }

    public function testNotSearch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('content', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?))', $result['query']);
        $this->assertEquals(['hello'], $result['bindings']);
    }

    public function testRegex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^[a-z]+$')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `slug` REGEXP ?', $result['query']);
        $this->assertEquals(['^[a-z]+$'], $result['bindings']);
    }

    public function testIsNull(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `deleted` IS NULL', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testIsNotNull(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNotNull('verified')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `verified` IS NOT NULL', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testExists(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name', 'email'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`name` IS NOT NULL AND `email` IS NOT NULL)', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testNotExists(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['legacy'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`legacy` IS NULL)', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ── Logical / nested ──

    public function testAndLogical(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::greaterThan('age', 18),
                    Query::equal('status', ['active']),
                ]),
            ])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`age` > ? AND `status` IN (?))', $result['query']);
        $this->assertEquals([18, 'active'], $result['bindings']);
    }

    public function testOrLogical(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::equal('role', ['admin']),
                    Query::equal('role', ['mod']),
                ]),
            ])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`role` IN (?) OR `role` IN (?))', $result['query']);
        $this->assertEquals(['admin', 'mod'], $result['bindings']);
    }

    public function testDeeplyNested(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::greaterThan('age', 18),
                    Query::or([
                        Query::equal('role', ['admin']),
                        Query::equal('role', ['mod']),
                    ]),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`age` > ? AND (`role` IN (?) OR `role` IN (?)))',
            $result['query']
        );
        $this->assertEquals([18, 'admin', 'mod'], $result['bindings']);
    }

    // ── Sort ──

    public function testSortAsc(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY `name` ASC', $result['query']);
    }

    public function testSortDesc(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortDesc('score')
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY `score` DESC', $result['query']);
    }

    public function testSortRandom(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND()', $result['query']);
    }

    public function testMultipleSorts(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY `name` ASC, `age` DESC', $result['query']);
    }

    // ── Pagination ──

    public function testLimitOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(10)
            ->build();

        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testOffsetOnly(): void
    {
        // OFFSET without LIMIT is invalid in MySQL/ClickHouse, so offset is suppressed
        $result = (new Builder())
            ->from('t')
            ->offset(50)
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testCursorAfter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc123')
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `_cursor` > ?', $result['query']);
        $this->assertEquals(['abc123'], $result['bindings']);
    }

    public function testCursorBefore(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorBefore('xyz789')
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `_cursor` < ?', $result['query']);
        $this->assertEquals(['xyz789'], $result['bindings']);
    }

    // ── Combined full query ──

    public function testFullCombinedQuery(): void
    {
        $result = (new Builder())
            ->select(['id', 'name'])
            ->from('users')
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])
            ->sortAsc('name')
            ->sortDesc('age')
            ->limit(25)
            ->offset(10)
            ->build();

        $this->assertEquals(
            'SELECT `id`, `name` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC, `age` DESC LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals(['active', 18, 25, 10], $result['bindings']);
    }

    // ── Multiple filter() calls (additive) ──

    public function testMultipleFilterCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->filter([Query::equal('b', [2])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?) AND `b` IN (?)', $result['query']);
        $this->assertEquals([1, 2], $result['bindings']);
    }

    // ── Reset ──

    public function testResetClearsState(): void
    {
        $builder = (new Builder())
            ->select(['name'])
            ->from('users')
            ->filter([Query::equal('x', [1])])
            ->limit(10);

        $builder->build();

        $builder->reset();

        $result = $builder
            ->from('orders')
            ->filter([Query::greaterThan('total', 100)])
            ->build();

        $this->assertEquals('SELECT * FROM `orders` WHERE `total` > ?', $result['query']);
        $this->assertEquals([100], $result['bindings']);
    }

    // ── Extension points ──

    public function testAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('users')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$id' => '_uid',
                '$createdAt' => '_createdAt',
                default => $a,
            })
            ->filter([Query::equal('$id', ['abc'])])
            ->sortAsc('$createdAt')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `_uid` IN (?) ORDER BY `_createdAt` ASC',
            $result['query']
        );
        $this->assertEquals(['abc'], $result['bindings']);
    }

    public function testWrapChar(): void
    {
        $result = (new Builder())
            ->from('users')
            ->setWrapChar('"')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals(
            'SELECT "name" FROM "users" WHERE "status" IN (?)',
            $result['query']
        );
    }

    public function testConditionProvider(): void
    {
        $result = (new Builder())
            ->from('users')
            ->addConditionProvider(fn (string $table): array => [
                "_uid IN (SELECT _document FROM {$table}_perms WHERE _type = 'read')",
                [],
            ])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals(
            "SELECT * FROM `users` WHERE `status` IN (?) AND _uid IN (SELECT _document FROM users_perms WHERE _type = 'read')",
            $result['query']
        );
        $this->assertEquals(['active'], $result['bindings']);
    }

    public function testConditionProviderWithBindings(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->addConditionProvider(fn (string $table): array => [
                '_tenant = ?',
                ['tenant_abc'],
            ])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `docs` WHERE `status` IN (?) AND _tenant = ?',
            $result['query']
        );
        // filter bindings first, then provider bindings
        $this->assertEquals(['active', 'tenant_abc'], $result['bindings']);
    }

    public function testBindingOrderingWithProviderAndCursor(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->addConditionProvider(fn (string $table): array => [
                '_tenant = ?',
                ['t1'],
            ])
            ->filter([Query::equal('status', ['active'])])
            ->cursorAfter('cursor_val')
            ->limit(10)
            ->offset(5)
            ->build();

        // binding order: filter, provider, cursor, limit, offset
        $this->assertEquals(['active', 't1', 'cursor_val', 10, 5], $result['bindings']);
    }

    // ── Select with no columns defaults to * ──

    public function testDefaultSelectStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    // ── Aggregations ──

    public function testCountStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->build();

        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testCountWithAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->build();

        $this->assertEquals('SELECT COUNT(*) AS `total` FROM `t`', $result['query']);
    }

    public function testSumColumn(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sum('price', 'total_price')
            ->build();

        $this->assertEquals('SELECT SUM(`price`) AS `total_price` FROM `orders`', $result['query']);
    }

    public function testAvgColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->avg('score')
            ->build();

        $this->assertEquals('SELECT AVG(`score`) FROM `t`', $result['query']);
    }

    public function testMinColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->min('price')
            ->build();

        $this->assertEquals('SELECT MIN(`price`) FROM `t`', $result['query']);
    }

    public function testMaxColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->max('price')
            ->build();

        $this->assertEquals('SELECT MAX(`price`) FROM `t`', $result['query']);
    }

    public function testAggregationWithSelection(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->select(['status'])
            ->groupBy(['status'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, `status` FROM `orders` GROUP BY `status`',
            $result['query']
        );
    }

    // ── Group By ──

    public function testGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status`',
            $result['query']
        );
    }

    public function testGroupByMultiple(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status', 'country'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status`, `country`',
            $result['query']
        );
    }

    // ── Having ──

    public function testHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 5)])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status` HAVING `total` > ?',
            $result['query']
        );
        $this->assertEquals([5], $result['bindings']);
    }

    // ── Distinct ──

    public function testDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->build();

        $this->assertEquals('SELECT DISTINCT `status` FROM `t`', $result['query']);
    }

    public function testDistinctStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->build();

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result['query']);
    }

    // ── Joins ──

    public function testJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result['query']
        );
    }

    public function testLeftJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id`',
            $result['query']
        );
    }

    public function testRightJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->rightJoin('orders', 'users.id', 'orders.user_id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` RIGHT JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result['query']
        );
    }

    public function testCrossJoin(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `sizes` CROSS JOIN `colors`',
            $result['query']
        );
    }

    public function testJoinWithFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->filter([Query::greaterThan('orders.total', 100)])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ?',
            $result['query']
        );
        $this->assertEquals([100], $result['bindings']);
    }

    // ── Raw ──

    public function testRawFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('score > ? AND score < ?', [10, 100])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE score > ? AND score < ?', $result['query']);
        $this->assertEquals([10, 100], $result['bindings']);
    }

    public function testRawFilterNoBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('1 = 1')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 1', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ── Union ──

    public function testUnion(): void
    {
        $admins = (new Builder())->from('admins')->filter([Query::equal('role', ['admin'])]);
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->union($admins)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `status` IN (?) UNION SELECT * FROM `admins` WHERE `role` IN (?)',
            $result['query']
        );
        $this->assertEquals(['active', 'admin'], $result['bindings']);
    }

    public function testUnionAll(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('current')
            ->unionAll($other)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `current` UNION ALL SELECT * FROM `archive`',
            $result['query']
        );
    }

    // ── when() ──

    public function testWhenTrue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?)', $result['query']);
        $this->assertEquals(['active'], $result['bindings']);
    }

    public function testWhenFalse(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ── page() ──

    public function testPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(3, 10)
            ->build();

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result['query']);
        $this->assertEquals([10, 20], $result['bindings']);
    }

    public function testPageDefaultPerPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(1)
            ->build();

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result['query']);
        $this->assertEquals([25, 0], $result['bindings']);
    }

    // ── toRawSql() ──

    public function testToRawSql(): void
    {
        $sql = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->toRawSql();

        $this->assertEquals(
            "SELECT * FROM `users` WHERE `status` IN ('active') LIMIT 10",
            $sql
        );
    }

    public function testToRawSqlNumericBindings(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('age', 18)])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `t` WHERE `age` > 18", $sql);
    }

    // ── Combined complex query ──

    public function testCombinedAggregationJoinGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'order_count')
            ->sum('total', 'total_amount')
            ->select(['users.name'])
            ->join('users', 'orders.user_id', 'users.id')
            ->groupBy(['users.name'])
            ->having([Query::greaterThan('order_count', 5)])
            ->sortDesc('total_amount')
            ->limit(10)
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `order_count`, SUM(`total`) AS `total_amount`, `users`.`name` FROM `orders` JOIN `users` ON `orders`.`user_id` = `users`.`id` GROUP BY `users`.`name` HAVING `order_count` > ? ORDER BY `total_amount` DESC LIMIT ?',
            $result['query']
        );
        $this->assertEquals([5, 10], $result['bindings']);
    }

    // ── Reset clears unions ──

    public function testResetClearsUnions(): void
    {
        $other = (new Builder())->from('archive');
        $builder = (new Builder())
            ->from('current')
            ->union($other);

        $builder->build();
        $builder->reset();

        $result = $builder->from('fresh')->build();

        $this->assertEquals('SELECT * FROM `fresh`', $result['query']);
    }

    // ══════════════════════════════════════════
    //  EDGE CASES & COMBINATIONS
    // ══════════════════════════════════════════

    // ── Aggregation edge cases ──

    public function testCountWithNamedColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('id')
            ->build();

        $this->assertEquals('SELECT COUNT(`id`) FROM `t`', $result['query']);
    }

    public function testCountWithEmptyStringAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('')
            ->build();

        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result['query']);
    }

    public function testMultipleAggregations(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->sum('price', 'total')
            ->avg('score', 'avg_score')
            ->min('age', 'youngest')
            ->max('age', 'oldest')
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `cnt`, SUM(`price`) AS `total`, AVG(`score`) AS `avg_score`, MIN(`age`) AS `youngest`, MAX(`age`) AS `oldest` FROM `t`',
            $result['query']
        );
        $this->assertEquals([], $result['bindings']);
    }

    public function testAggregationWithoutGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sum('total', 'grand_total')
            ->build();

        $this->assertEquals('SELECT SUM(`total`) AS `grand_total` FROM `orders`', $result['query']);
    }

    public function testAggregationWithFilter(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->filter([Query::equal('status', ['completed'])])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` WHERE `status` IN (?)',
            $result['query']
        );
        $this->assertEquals(['completed'], $result['bindings']);
    }

    public function testAggregationWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->sum('price')
            ->build();

        $this->assertEquals('SELECT COUNT(*), SUM(`price`) FROM `t`', $result['query']);
    }

    // ── Group By edge cases ──

    public function testGroupByEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->groupBy([])
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    public function testMultipleGroupByCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->groupBy(['country'])
            ->build();

        // Both groupBy calls should merge since groupByType merges values
        $this->assertStringContainsString('GROUP BY', $result['query']);
        $this->assertStringContainsString('`status`', $result['query']);
        $this->assertStringContainsString('`country`', $result['query']);
    }

    // ── Having edge cases ──

    public function testHavingEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([])
            ->build();

        $this->assertStringNotContainsString('HAVING', $result['query']);
    }

    public function testHavingMultipleConditions(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->sum('price', 'sum_price')
            ->groupBy(['status'])
            ->having([
                Query::greaterThan('total', 5),
                Query::lessThan('sum_price', 1000),
            ])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, SUM(`price`) AS `sum_price` FROM `t` GROUP BY `status` HAVING `total` > ? AND `sum_price` < ?',
            $result['query']
        );
        $this->assertEquals([5, 1000], $result['bindings']);
    }

    public function testHavingWithLogicalOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([
                Query::or([
                    Query::greaterThan('total', 10),
                    Query::lessThan('total', 2),
                ]),
            ])
            ->build();

        $this->assertStringContainsString('HAVING (`total` > ? OR `total` < ?)', $result['query']);
        $this->assertEquals([10, 2], $result['bindings']);
    }

    public function testHavingWithoutGroupBy(): void
    {
        // SQL allows HAVING without GROUP BY in some engines
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->having([Query::greaterThan('total', 0)])
            ->build();

        $this->assertStringContainsString('HAVING', $result['query']);
        $this->assertStringNotContainsString('GROUP BY', $result['query']);
    }

    public function testMultipleHavingCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 1)])
            ->having([Query::lessThan('total', 100)])
            ->build();

        $this->assertStringContainsString('HAVING `total` > ? AND `total` < ?', $result['query']);
        $this->assertEquals([1, 100], $result['bindings']);
    }

    // ── Distinct edge cases ──

    public function testDistinctWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->count('*', 'total')
            ->build();

        $this->assertEquals('SELECT DISTINCT COUNT(*) AS `total` FROM `t`', $result['query']);
    }

    public function testDistinctMultipleCalls(): void
    {
        // Multiple distinct() calls should still produce single DISTINCT keyword
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->distinct()
            ->build();

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result['query']);
    }

    public function testDistinctWithJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->distinct()
            ->select(['users.name'])
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();

        $this->assertEquals(
            'SELECT DISTINCT `users`.`name` FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result['query']
        );
    }

    public function testDistinctWithFilterAndSort(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->filter([Query::isNotNull('status')])
            ->sortAsc('status')
            ->build();

        $this->assertEquals(
            'SELECT DISTINCT `status` FROM `t` WHERE `status` IS NOT NULL ORDER BY `status` ASC',
            $result['query']
        );
    }

    // ── Join combinations ──

    public function testMultipleJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->rightJoin('departments', 'users.dept_id', 'departments.id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id` RIGHT JOIN `departments` ON `users`.`dept_id` = `departments`.`id`',
            $result['query']
        );
    }

    public function testJoinWithAggregationAndGroupBy(): void
    {
        $result = (new Builder())
            ->from('users')
            ->count('*', 'order_count')
            ->join('orders', 'users.id', 'orders.user_id')
            ->groupBy(['users.name'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `order_count` FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` GROUP BY `users`.`name`',
            $result['query']
        );
    }

    public function testJoinWithSortAndPagination(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->filter([Query::greaterThan('orders.total', 50)])
            ->sortDesc('orders.total')
            ->limit(10)
            ->offset(20)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ? ORDER BY `orders`.`total` DESC LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals([50, 10, 20], $result['bindings']);
    }

    public function testJoinWithCustomOperator(): void
    {
        $result = (new Builder())
            ->from('a')
            ->join('b', 'a.val', 'b.val', '!=')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `a` JOIN `b` ON `a`.`val` != `b`.`val`',
            $result['query']
        );
    }

    public function testCrossJoinWithOtherJoins(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->leftJoin('inventory', 'sizes.id', 'inventory.size_id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `sizes` CROSS JOIN `colors` LEFT JOIN `inventory` ON `sizes`.`id` = `inventory`.`size_id`',
            $result['query']
        );
    }

    // ── Raw edge cases ──

    public function testRawWithMixedBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('a = ? AND b = ? AND c = ?', ['str', 42, 3.14])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE a = ? AND b = ? AND c = ?', $result['query']);
        $this->assertEquals(['str', 42, 3.14], $result['bindings']);
    }

    public function testRawCombinedWithRegularFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('status', ['active']),
                Query::raw('custom_func(col) > ?', [10]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) AND custom_func(col) > ?',
            $result['query']
        );
        $this->assertEquals(['active', 10], $result['bindings']);
    }

    public function testRawWithEmptySql(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('')])
            ->build();

        // Empty raw SQL still appears as a WHERE clause
        $this->assertStringContainsString('WHERE', $result['query']);
    }

    // ── Union edge cases ──

    public function testMultipleUnions(): void
    {
        $q1 = (new Builder())->from('admins');
        $q2 = (new Builder())->from('mods');

        $result = (new Builder())
            ->from('users')
            ->union($q1)
            ->union($q2)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` UNION SELECT * FROM `admins` UNION SELECT * FROM `mods`',
            $result['query']
        );
    }

    public function testMixedUnionAndUnionAll(): void
    {
        $q1 = (new Builder())->from('admins');
        $q2 = (new Builder())->from('mods');

        $result = (new Builder())
            ->from('users')
            ->union($q1)
            ->unionAll($q2)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` UNION SELECT * FROM `admins` UNION ALL SELECT * FROM `mods`',
            $result['query']
        );
    }

    public function testUnionWithFiltersAndBindings(): void
    {
        $q1 = (new Builder())->from('admins')->filter([Query::equal('level', [1])]);
        $q2 = (new Builder())->from('mods')->filter([Query::greaterThan('score', 50)]);

        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->union($q1)
            ->unionAll($q2)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `status` IN (?) UNION SELECT * FROM `admins` WHERE `level` IN (?) UNION ALL SELECT * FROM `mods` WHERE `score` > ?',
            $result['query']
        );
        $this->assertEquals(['active', 1, 50], $result['bindings']);
    }

    public function testUnionWithAggregation(): void
    {
        $q1 = (new Builder())->from('orders_2023')->count('*', 'total');

        $result = (new Builder())
            ->from('orders_2024')
            ->count('*', 'total')
            ->unionAll($q1)
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders_2024` UNION ALL SELECT COUNT(*) AS `total` FROM `orders_2023`',
            $result['query']
        );
    }

    // ── when() edge cases ──

    public function testWhenNested(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, function (Builder $b) {
                $b->when(true, fn (Builder $b2) => $b2->filter([Query::equal('a', [1])]));
            })
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $result['query']);
    }

    public function testWhenMultipleCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('a', [1])]))
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('b', [2])]))
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('c', [3])]))
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?) AND `c` IN (?)', $result['query']);
        $this->assertEquals([1, 3], $result['bindings']);
    }

    // ── page() edge cases ──

    public function testPageZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(0, 10)
            ->build();

        // page 0 → offset clamped to 0
        $this->assertEquals([10, 0], $result['bindings']);
    }

    public function testPageOnePerPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(5, 1)
            ->build();

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result['query']);
        $this->assertEquals([1, 4], $result['bindings']);
    }

    public function testPageLargeValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(1000, 100)
            ->build();

        $this->assertEquals([100, 99900], $result['bindings']);
    }

    // ── toRawSql() edge cases ──

    public function testToRawSqlWithBooleanBindings(): void
    {
        // Booleans must be handled in toRawSql
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::raw('active = ?', [true])]);

        $sql = $builder->toRawSql();
        $this->assertEquals("SELECT * FROM `t` WHERE active = 1", $sql);
    }

    public function testToRawSqlWithNullBinding(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::raw('deleted_at = ?', [null])]);

        $sql = $builder->toRawSql();
        $this->assertEquals("SELECT * FROM `t` WHERE deleted_at = NULL", $sql);
    }

    public function testToRawSqlWithFloatBinding(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::raw('price > ?', [9.99])]);

        $sql = $builder->toRawSql();
        $this->assertEquals("SELECT * FROM `t` WHERE price > 9.99", $sql);
    }

    public function testToRawSqlComplexQuery(): void
    {
        $sql = (new Builder())
            ->from('users')
            ->select(['name'])
            ->filter([
                Query::equal('status', ['active']),
                Query::greaterThan('age', 18),
            ])
            ->sortAsc('name')
            ->limit(25)
            ->offset(10)
            ->toRawSql();

        $this->assertEquals(
            "SELECT `name` FROM `users` WHERE `status` IN ('active') AND `age` > 18 ORDER BY `name` ASC LIMIT 25 OFFSET 10",
            $sql
        );
    }

    // ── Exception paths ──

    public function testCompileFilterUnsupportedType(): void
    {
        $builder = new Builder();
        $query = new Query('totallyInvalid', 'x', [1]);

        $this->expectException(\Utopia\Query\Exception::class);
        $this->expectExceptionMessage('Unsupported filter type: totallyInvalid');
        $builder->compileFilter($query);
    }

    public function testCompileOrderUnsupportedType(): void
    {
        $builder = new Builder();
        $query = new Query('equal', 'x', [1]);

        $this->expectException(\Utopia\Query\Exception::class);
        $this->expectExceptionMessage('Unsupported order type: equal');
        $builder->compileOrder($query);
    }

    public function testCompileJoinUnsupportedType(): void
    {
        $builder = new Builder();
        $query = new Query('equal', 't', ['a', '=', 'b']);

        $this->expectException(\Utopia\Query\Exception::class);
        $this->expectExceptionMessage('Unsupported join type: equal');
        $builder->compileJoin($query);
    }

    // ── Binding order edge cases ──

    public function testBindingOrderFilterProviderCursorLimitOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $table): array => [
                '_tenant = ?',
                ['tenant1'],
            ])
            ->filter([
                Query::equal('a', ['x']),
                Query::greaterThan('b', 5),
            ])
            ->cursorAfter('cursor_abc')
            ->limit(10)
            ->offset(20)
            ->build();

        // Order: filter bindings, provider bindings, cursor, limit, offset
        $this->assertEquals(['x', 5, 'tenant1', 'cursor_abc', 10, 20], $result['bindings']);
    }

    public function testBindingOrderMultipleProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $table): array => ['p1 = ?', ['v1']])
            ->addConditionProvider(fn (string $table): array => ['p2 = ?', ['v2']])
            ->filter([Query::equal('a', ['x'])])
            ->build();

        $this->assertEquals(['x', 'v1', 'v2'], $result['bindings']);
    }

    public function testBindingOrderHavingAfterFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->filter([Query::equal('status', ['active'])])
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 5)])
            ->limit(10)
            ->build();

        // Filter bindings, then having bindings, then limit
        $this->assertEquals(['active', 5, 10], $result['bindings']);
    }

    public function testBindingOrderUnionAppendedLast(): void
    {
        $sub = (new Builder())->from('other')->filter([Query::equal('x', ['y'])]);

        $result = (new Builder())
            ->from('main')
            ->filter([Query::equal('a', ['b'])])
            ->limit(5)
            ->union($sub)
            ->build();

        // Main filter, main limit, then union bindings
        $this->assertEquals(['b', 5, 'y'], $result['bindings']);
    }

    public function testBindingOrderComplexMixed(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);

        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->addConditionProvider(fn (string $t): array => ['_org = ?', ['org1']])
            ->filter([Query::equal('status', ['paid'])])
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->cursorAfter('cur1')
            ->limit(10)
            ->offset(5)
            ->union($sub)
            ->build();

        // filter, provider, cursor, having, limit, offset, union
        $this->assertEquals(['paid', 'org1', 'cur1', 1, 10, 5, 2023], $result['bindings']);
    }

    // ── Attribute resolver with new features ──

    public function testAttributeResolverWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$price' => '_price',
                default => $a,
            })
            ->sum('$price', 'total')
            ->build();

        $this->assertEquals('SELECT SUM(`_price`) AS `total` FROM `t`', $result['query']);
    }

    public function testAttributeResolverWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$status' => '_status',
                default => $a,
            })
            ->count('*', 'total')
            ->groupBy(['$status'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `t` GROUP BY `_status`',
            $result['query']
        );
    }

    public function testAttributeResolverWithJoin(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$id' => '_uid',
                '$ref' => '_ref',
                default => $a,
            })
            ->join('other', '$id', '$ref')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` JOIN `other` ON `_uid` = `_ref`',
            $result['query']
        );
    }

    public function testAttributeResolverWithHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$total' => '_total',
                default => $a,
            })
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('$total', 5)])
            ->build();

        $this->assertStringContainsString('HAVING `_total` > ?', $result['query']);
    }

    // ── Wrap char with new features ──

    public function testWrapCharWithJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->setWrapChar('"')
            ->join('orders', 'users.id', 'orders.uid')
            ->build();

        $this->assertEquals(
            'SELECT * FROM "users" JOIN "orders" ON "users"."id" = "orders"."uid"',
            $result['query']
        );
    }

    public function testWrapCharWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setWrapChar('"')
            ->count('id', 'total')
            ->groupBy(['status'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT("id") AS "total" FROM "t" GROUP BY "status"',
            $result['query']
        );
    }

    public function testWrapCharEmpty(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setWrapChar('')
            ->select(['name'])
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals('SELECT name FROM t WHERE status IN (?)', $result['query']);
    }

    // ── Condition provider with new features ──

    public function testConditionProviderWithJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->addConditionProvider(fn (string $table): array => [
                'users.org_id = ?',
                ['org1'],
            ])
            ->filter([Query::greaterThan('orders.total', 100)])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ? AND users.org_id = ?',
            $result['query']
        );
        $this->assertEquals([100, 'org1'], $result['bindings']);
    }

    public function testConditionProviderWithAggregation(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->addConditionProvider(fn (string $table): array => [
                'org_id = ?',
                ['org1'],
            ])
            ->groupBy(['status'])
            ->build();

        $this->assertStringContainsString('WHERE org_id = ?', $result['query']);
        $this->assertEquals(['org1'], $result['bindings']);
    }

    // ── Multiple build() calls ──

    public function testMultipleBuildsConsistentOutput(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->limit(10);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1['query'], $result2['query']);
        $this->assertEquals($result1['bindings'], $result2['bindings']);
    }

    // ── Reset behavior ──

    public function testResetDoesNotClearWrapCharOrResolver(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->setWrapChar('"')
            ->setAttributeResolver(fn (string $a): string => '_' . $a)
            ->filter([Query::equal('x', [1])]);

        $builder->build();
        $builder->reset();

        // wrapChar and resolver should persist since reset() only clears queries/bindings/table/unions
        $result = $builder->from('t2')->filter([Query::equal('y', [2])])->build();
        $this->assertEquals('SELECT * FROM "t2" WHERE "_y" IN (?)', $result['query']);
    }

    // ── Empty query ──

    public function testEmptyBuilderNoFrom(): void
    {
        $result = (new Builder())->from('')->build();
        $this->assertEquals('SELECT * FROM ``', $result['query']);
    }

    // ── Cursor with other pagination ──

    public function testCursorWithLimitAndOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc')
            ->limit(10)
            ->offset(5)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `_cursor` > ? LIMIT ? OFFSET ?',
            $result['query']
        );
        $this->assertEquals(['abc', 10, 5], $result['bindings']);
    }

    public function testCursorWithPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc')
            ->page(2, 10)
            ->build();

        // Cursor + limit from page + offset from page; first limit/offset wins
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
    }

    // ── Full kitchen sink ──

    public function testKitchenSinkQuery(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);

        $result = (new Builder())
            ->from('orders')
            ->distinct()
            ->count('*', 'cnt')
            ->sum('total', 'sum_total')
            ->select(['status'])
            ->join('users', 'orders.user_id', 'users.id')
            ->leftJoin('coupons', 'orders.coupon_id', 'coupons.id')
            ->filter([
                Query::equal('orders.status', ['paid']),
                Query::greaterThan('orders.total', 0),
            ])
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['o1']])
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->sortDesc('sum_total')
            ->limit(25)
            ->offset(50)
            ->union($sub)
            ->build();

        // Verify structural elements
        $this->assertStringContainsString('SELECT DISTINCT', $result['query']);
        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result['query']);
        $this->assertStringContainsString('SUM(`total`) AS `sum_total`', $result['query']);
        $this->assertStringContainsString('`status`', $result['query']);
        $this->assertStringContainsString('FROM `orders`', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('LEFT JOIN `coupons`', $result['query']);
        $this->assertStringContainsString('WHERE', $result['query']);
        $this->assertStringContainsString('GROUP BY `status`', $result['query']);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
        $this->assertStringContainsString('ORDER BY `sum_total` DESC', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);

        // Verify SQL clause ordering
        $query = $result['query'];
        $this->assertLessThan(strpos($query, 'FROM'), strpos($query, 'SELECT'));
        $this->assertLessThan(strpos($query, 'JOIN'), (int) strpos($query, 'FROM'));
        $this->assertLessThan(strpos($query, 'WHERE'), (int) strpos($query, 'JOIN'));
        $this->assertLessThan(strpos($query, 'GROUP BY'), (int) strpos($query, 'WHERE'));
        $this->assertLessThan(strpos($query, 'HAVING'), (int) strpos($query, 'GROUP BY'));
        $this->assertLessThan(strpos($query, 'ORDER BY'), (int) strpos($query, 'HAVING'));
        $this->assertLessThan(strpos($query, 'LIMIT'), (int) strpos($query, 'ORDER BY'));
        $this->assertLessThan(strpos($query, 'OFFSET'), (int) strpos($query, 'LIMIT'));
        $this->assertLessThan(strpos($query, 'UNION'), (int) strpos($query, 'OFFSET'));
    }

    // ── Filter empty arrays ──

    public function testFilterEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([])
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    public function testSelectEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select([])
            ->build();

        // Empty select produces empty column list
        $this->assertEquals('SELECT  FROM `t`', $result['query']);
    }

    // ── Limit/offset edge values ──

    public function testLimitZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(0)
            ->build();

        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([0], $result['bindings']);
    }

    public function testOffsetZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->offset(0)
            ->build();

        // OFFSET without LIMIT is suppressed
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ── Fluent chaining returns same instance ──

    public function testFluentChainingReturnsSameInstance(): void
    {
        $builder = new Builder();

        $this->assertSame($builder, $builder->from('t'));
        $this->assertSame($builder, $builder->select(['a']));
        $this->assertSame($builder, $builder->filter([]));
        $this->assertSame($builder, $builder->sortAsc('a'));
        $this->assertSame($builder, $builder->sortDesc('a'));
        $this->assertSame($builder, $builder->sortRandom());
        $this->assertSame($builder, $builder->limit(1));
        $this->assertSame($builder, $builder->offset(0));
        $this->assertSame($builder, $builder->cursorAfter('x'));
        $this->assertSame($builder, $builder->cursorBefore('x'));
        $this->assertSame($builder, $builder->queries([]));
        $this->assertSame($builder, $builder->setWrapChar('`'));
        $this->assertSame($builder, $builder->count());
        $this->assertSame($builder, $builder->sum('a'));
        $this->assertSame($builder, $builder->avg('a'));
        $this->assertSame($builder, $builder->min('a'));
        $this->assertSame($builder, $builder->max('a'));
        $this->assertSame($builder, $builder->groupBy(['a']));
        $this->assertSame($builder, $builder->having([]));
        $this->assertSame($builder, $builder->distinct());
        $this->assertSame($builder, $builder->join('t', 'a', 'b'));
        $this->assertSame($builder, $builder->leftJoin('t', 'a', 'b'));
        $this->assertSame($builder, $builder->rightJoin('t', 'a', 'b'));
        $this->assertSame($builder, $builder->crossJoin('t'));
        $this->assertSame($builder, $builder->when(false, fn ($b) => $b));
        $this->assertSame($builder, $builder->page(1));
        $this->assertSame($builder, $builder->reset());
    }

    public function testUnionFluentChainingReturnsSameInstance(): void
    {
        $builder = new Builder();
        $other = (new Builder())->from('t');
        $this->assertSame($builder, $builder->from('t')->union($other));

        $builder->reset();
        $other2 = (new Builder())->from('t');
        $this->assertSame($builder, $builder->from('t')->unionAll($other2));
    }

    // ══════════════════════════════════════════
    //  1. SQL-Specific: REGEXP
    // ══════════════════════════════════════════

    public function testRegexWithEmptyPattern(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `slug` REGEXP ?', $result['query']);
        $this->assertEquals([''], $result['bindings']);
    }

    public function testRegexWithDotChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a.b')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` REGEXP ?', $result['query']);
        $this->assertEquals(['a.b'], $result['bindings']);
    }

    public function testRegexWithStarChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a*b')])
            ->build();

        $this->assertEquals(['a*b'], $result['bindings']);
    }

    public function testRegexWithPlusChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a+')])
            ->build();

        $this->assertEquals(['a+'], $result['bindings']);
    }

    public function testRegexWithQuestionMarkChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'colou?r')])
            ->build();

        $this->assertEquals(['colou?r'], $result['bindings']);
    }

    public function testRegexWithCaretAndDollar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('code', '^[A-Z]+$')])
            ->build();

        $this->assertEquals(['^[A-Z]+$'], $result['bindings']);
    }

    public function testRegexWithPipeChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('color', 'red|blue|green')])
            ->build();

        $this->assertEquals(['red|blue|green'], $result['bindings']);
    }

    public function testRegexWithBackslash(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('path', '\\\\server\\\\share')])
            ->build();

        $this->assertEquals(['\\\\server\\\\share'], $result['bindings']);
    }

    public function testRegexWithBracketsAndBraces(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('zip', '[0-9]{5}')])
            ->build();

        $this->assertEquals('[0-9]{5}', $result['bindings'][0]);
    }

    public function testRegexWithParentheses(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('phone', '(\\+1)?[0-9]{10}')])
            ->build();

        $this->assertEquals(['(\\+1)?[0-9]{10}'], $result['bindings']);
    }

    public function testRegexCombinedWithOtherFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('status', ['active']),
                Query::regex('slug', '^[a-z-]+$'),
                Query::greaterThan('age', 18),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) AND `slug` REGEXP ? AND `age` > ?',
            $result['query']
        );
        $this->assertEquals(['active', '^[a-z-]+$', 18], $result['bindings']);
    }

    public function testRegexWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$slug' => '_slug',
                default => $a,
            })
            ->filter([Query::regex('$slug', '^test')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `_slug` REGEXP ?', $result['query']);
        $this->assertEquals(['^test'], $result['bindings']);
    }

    public function testRegexWithDifferentWrapChar(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::regex('slug', '^[a-z]+$')])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE "slug" REGEXP ?', $result['query']);
    }

    public function testRegexStandaloneCompileFilter(): void
    {
        $builder = new Builder();
        $query = Query::regex('col', '^abc');
        $sql = $builder->compileFilter($query);

        $this->assertEquals('`col` REGEXP ?', $sql);
        $this->assertEquals(['^abc'], $builder->getBindings());
    }

    public function testRegexBindingPreservedExactly(): void
    {
        $pattern = '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('email', $pattern)])
            ->build();

        $this->assertSame($pattern, $result['bindings'][0]);
    }

    public function testRegexWithVeryLongPattern(): void
    {
        $pattern = str_repeat('[a-z]', 500);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('col', $pattern)])
            ->build();

        $this->assertEquals($pattern, $result['bindings'][0]);
        $this->assertStringContainsString('REGEXP ?', $result['query']);
    }

    public function testMultipleRegexFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::regex('name', '^A'),
                Query::regex('email', '@test\\.com$'),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `name` REGEXP ? AND `email` REGEXP ?',
            $result['query']
        );
        $this->assertEquals(['^A', '@test\\.com$'], $result['bindings']);
    }

    public function testRegexInAndLogicalGroup(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::regex('slug', '^[a-z]+$'),
                    Query::equal('status', ['active']),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`slug` REGEXP ? AND `status` IN (?))',
            $result['query']
        );
        $this->assertEquals(['^[a-z]+$', 'active'], $result['bindings']);
    }

    public function testRegexInOrLogicalGroup(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::regex('name', '^Admin'),
                    Query::regex('name', '^Mod'),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`name` REGEXP ? OR `name` REGEXP ?)',
            $result['query']
        );
        $this->assertEquals(['^Admin', '^Mod'], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  2. SQL-Specific: MATCH AGAINST / Search
    // ══════════════════════════════════════════

    public function testSearchWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', '')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?)', $result['query']);
        $this->assertEquals([''], $result['bindings']);
    }

    public function testSearchWithSpecialCharacters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('body', 'hello "world" +required -excluded')])
            ->build();

        $this->assertEquals(['hello "world" +required -excluded'], $result['bindings']);
    }

    public function testSearchCombinedWithOtherFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::search('content', 'hello'),
                Query::equal('status', ['published']),
                Query::greaterThan('views', 100),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?) AND `status` IN (?) AND `views` > ?',
            $result['query']
        );
        $this->assertEquals(['hello', 'published', 100], $result['bindings']);
    }

    public function testNotSearchCombinedWithOtherFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::notSearch('content', 'spam'),
                Query::equal('status', ['published']),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?)) AND `status` IN (?)',
            $result['query']
        );
        $this->assertEquals(['spam', 'published'], $result['bindings']);
    }

    public function testSearchWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$body' => '_body',
                default => $a,
            })
            ->filter([Query::search('$body', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`_body`) AGAINST(?)', $result['query']);
    }

    public function testSearchWithDifferentWrapChar(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::search('content', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE MATCH("content") AGAINST(?)', $result['query']);
    }

    public function testSearchStandaloneCompileFilter(): void
    {
        $builder = new Builder();
        $query = Query::search('body', 'test');
        $sql = $builder->compileFilter($query);

        $this->assertEquals('MATCH(`body`) AGAINST(?)', $sql);
        $this->assertEquals(['test'], $builder->getBindings());
    }

    public function testNotSearchStandaloneCompileFilter(): void
    {
        $builder = new Builder();
        $query = Query::notSearch('body', 'spam');
        $sql = $builder->compileFilter($query);

        $this->assertEquals('NOT (MATCH(`body`) AGAINST(?))', $sql);
        $this->assertEquals(['spam'], $builder->getBindings());
    }

    public function testSearchBindingPreservedExactly(): void
    {
        $searchTerm = 'hello world "exact phrase" +required -excluded';
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', $searchTerm)])
            ->build();

        $this->assertSame($searchTerm, $result['bindings'][0]);
    }

    public function testSearchWithVeryLongText(): void
    {
        $longText = str_repeat('keyword ', 1000);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', $longText)])
            ->build();

        $this->assertEquals($longText, $result['bindings'][0]);
    }

    public function testMultipleSearchFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::search('title', 'hello'),
                Query::search('body', 'world'),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`title`) AGAINST(?) AND MATCH(`body`) AGAINST(?)',
            $result['query']
        );
        $this->assertEquals(['hello', 'world'], $result['bindings']);
    }

    public function testSearchInAndLogicalGroup(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::search('content', 'hello'),
                    Query::equal('status', ['active']),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (MATCH(`content`) AGAINST(?) AND `status` IN (?))',
            $result['query']
        );
    }

    public function testSearchInOrLogicalGroup(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::search('title', 'hello'),
                    Query::search('body', 'hello'),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (MATCH(`title`) AGAINST(?) OR MATCH(`body`) AGAINST(?))',
            $result['query']
        );
        $this->assertEquals(['hello', 'hello'], $result['bindings']);
    }

    public function testSearchAndRegexCombined(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::search('content', 'hello world'),
                Query::regex('slug', '^[a-z-]+$'),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?) AND `slug` REGEXP ?',
            $result['query']
        );
        $this->assertEquals(['hello world', '^[a-z-]+$'], $result['bindings']);
    }

    public function testNotSearchStandalone(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('content', 'spam')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?))', $result['query']);
        $this->assertEquals(['spam'], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  3. SQL-Specific: RAND()
    // ══════════════════════════════════════════

    public function testRandomSortStandaloneCompile(): void
    {
        $builder = new Builder();
        $query = Query::orderRandom();
        $sql = $builder->compileOrder($query);

        $this->assertEquals('RAND()', $sql);
    }

    public function testRandomSortCombinedWithAscDesc(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortRandom()
            ->sortDesc('age')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` ORDER BY `name` ASC, RAND(), `age` DESC',
            $result['query']
        );
    }

    public function testRandomSortWithFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->sortRandom()
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) ORDER BY RAND()',
            $result['query']
        );
        $this->assertEquals(['active'], $result['bindings']);
    }

    public function testRandomSortWithLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->limit(5)
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ?', $result['query']);
        $this->assertEquals([5], $result['bindings']);
    }

    public function testRandomSortWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['category'])
            ->sortRandom()
            ->build();

        $this->assertStringContainsString('ORDER BY RAND()', $result['query']);
        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
    }

    public function testRandomSortWithJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->sortRandom()
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
        $this->assertStringContainsString('ORDER BY RAND()', $result['query']);
    }

    public function testRandomSortWithDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->sortRandom()
            ->build();

        $this->assertEquals(
            'SELECT DISTINCT `status` FROM `t` ORDER BY RAND()',
            $result['query']
        );
    }

    public function testRandomSortInBatchMode(): void
    {
        $result = (new Builder())
            ->from('t')
            ->queries([
                Query::orderRandom(),
                Query::limit(10),
            ])
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testRandomSortWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => '_' . $a)
            ->sortRandom()
            ->build();

        $this->assertStringContainsString('ORDER BY RAND()', $result['query']);
    }

    public function testMultipleRandomSorts(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->sortRandom()
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND(), RAND()', $result['query']);
    }

    public function testRandomSortWithOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->limit(10)
            ->offset(5)
            ->build();

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ? OFFSET ?', $result['query']);
        $this->assertEquals([10, 5], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  4. setWrapChar comprehensive
    // ══════════════════════════════════════════

    public function testWrapCharSingleQuote(): void
    {
        $result = (new Builder())
            ->setWrapChar("'")
            ->from('t')
            ->select(['name'])
            ->build();

        $this->assertEquals("SELECT 'name' FROM 't'", $result['query']);
    }

    public function testWrapCharSquareBracket(): void
    {
        $result = (new Builder())
            ->setWrapChar('[')
            ->from('t')
            ->select(['name'])
            ->build();

        $this->assertEquals('SELECT [name[ FROM [t[', $result['query']);
    }

    public function testWrapCharUnicode(): void
    {
        $result = (new Builder())
            ->setWrapChar("\xC2\xAB")
            ->from('t')
            ->select(['name'])
            ->build();

        $this->assertEquals("SELECT \xC2\xABname\xC2\xAB FROM \xC2\xABt\xC2\xAB", $result['query']);
    }

    public function testWrapCharAffectsSelect(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->select(['a', 'b', 'c'])
            ->build();

        $this->assertEquals('SELECT "a", "b", "c" FROM "t"', $result['query']);
    }

    public function testWrapCharAffectsFrom(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('my_table')
            ->build();

        $this->assertEquals('SELECT * FROM "my_table"', $result['query']);
    }

    public function testWrapCharAffectsFilter(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::equal('col', [1])])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE "col" IN (?)', $result['query']);
    }

    public function testWrapCharAffectsSort(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();

        $this->assertEquals('SELECT * FROM "t" ORDER BY "name" ASC, "age" DESC', $result['query']);
    }

    public function testWrapCharAffectsJoin(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->build();

        $this->assertEquals(
            'SELECT * FROM "users" JOIN "orders" ON "users"."id" = "orders"."uid"',
            $result['query']
        );
    }

    public function testWrapCharAffectsLeftJoin(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('users')
            ->leftJoin('profiles', 'users.id', 'profiles.uid')
            ->build();

        $this->assertEquals(
            'SELECT * FROM "users" LEFT JOIN "profiles" ON "users"."id" = "profiles"."uid"',
            $result['query']
        );
    }

    public function testWrapCharAffectsRightJoin(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('users')
            ->rightJoin('orders', 'users.id', 'orders.uid')
            ->build();

        $this->assertEquals(
            'SELECT * FROM "users" RIGHT JOIN "orders" ON "users"."id" = "orders"."uid"',
            $result['query']
        );
    }

    public function testWrapCharAffectsCrossJoin(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('a')
            ->crossJoin('b')
            ->build();

        $this->assertEquals('SELECT * FROM "a" CROSS JOIN "b"', $result['query']);
    }

    public function testWrapCharAffectsAggregation(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->sum('price', 'total')
            ->build();

        $this->assertEquals('SELECT SUM("price") AS "total" FROM "t"', $result['query']);
    }

    public function testWrapCharAffectsGroupBy(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status', 'country'])
            ->build();

        $this->assertEquals(
            'SELECT COUNT(*) AS "cnt" FROM "t" GROUP BY "status", "country"',
            $result['query']
        );
    }

    public function testWrapCharAffectsHaving(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();

        $this->assertStringContainsString('HAVING "cnt" > ?', $result['query']);
    }

    public function testWrapCharAffectsDistinct(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->build();

        $this->assertEquals('SELECT DISTINCT "status" FROM "t"', $result['query']);
    }

    public function testWrapCharAffectsRegex(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::regex('slug', '^test')])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE "slug" REGEXP ?', $result['query']);
    }

    public function testWrapCharAffectsSearch(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::search('body', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE MATCH("body") AGAINST(?)', $result['query']);
    }

    public function testWrapCharEmptyForSelect(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->select(['a', 'b'])
            ->build();

        $this->assertEquals('SELECT a, b FROM t', $result['query']);
    }

    public function testWrapCharEmptyForFilter(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->filter([Query::greaterThan('age', 18)])
            ->build();

        $this->assertEquals('SELECT * FROM t WHERE age > ?', $result['query']);
    }

    public function testWrapCharEmptyForSort(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->sortAsc('name')
            ->build();

        $this->assertEquals('SELECT * FROM t ORDER BY name ASC', $result['query']);
    }

    public function testWrapCharEmptyForJoin(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->build();

        $this->assertEquals('SELECT * FROM users JOIN orders ON users.id = orders.uid', $result['query']);
    }

    public function testWrapCharEmptyForAggregation(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->count('id', 'total')
            ->build();

        $this->assertEquals('SELECT COUNT(id) AS total FROM t', $result['query']);
    }

    public function testWrapCharEmptyForGroupBy(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->build();

        $this->assertEquals('SELECT COUNT(*) AS cnt FROM t GROUP BY status', $result['query']);
    }

    public function testWrapCharEmptyForDistinct(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->distinct()
            ->select(['name'])
            ->build();

        $this->assertEquals('SELECT DISTINCT name FROM t', $result['query']);
    }

    public function testWrapCharDoubleQuoteForSelect(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->select(['x', 'y'])
            ->build();

        $this->assertEquals('SELECT "x", "y" FROM "t"', $result['query']);
    }

    public function testWrapCharDoubleQuoteForIsNull(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::isNull('deleted')])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE "deleted" IS NULL', $result['query']);
    }

    public function testWrapCharCalledMultipleTimesLastWins(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->setWrapChar("'")
            ->setWrapChar('`')
            ->from('t')
            ->select(['name'])
            ->build();

        $this->assertEquals('SELECT `name` FROM `t`', $result['query']);
    }

    public function testWrapCharDoesNotAffectRawExpressions(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::raw('custom_func(col) > ?', [10])])
            ->build();

        $this->assertEquals('SELECT * FROM "t" WHERE custom_func(col) > ?', $result['query']);
    }

    public function testWrapCharPersistsAcrossMultipleBuilds(): void
    {
        $builder = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->select(['name']);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals('SELECT "name" FROM "t"', $result1['query']);
        $this->assertEquals('SELECT "name" FROM "t"', $result2['query']);
    }

    public function testWrapCharWithConditionProviderNotWrapped(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->addConditionProvider(fn (string $table): array => [
                'raw_condition = 1',
                [],
            ])
            ->build();

        $this->assertStringContainsString('WHERE raw_condition = 1', $result['query']);
        $this->assertStringContainsString('FROM "t"', $result['query']);
    }

    public function testWrapCharEmptyForRegex(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->filter([Query::regex('slug', '^test')])
            ->build();

        $this->assertEquals('SELECT * FROM t WHERE slug REGEXP ?', $result['query']);
    }

    public function testWrapCharEmptyForSearch(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->filter([Query::search('body', 'hello')])
            ->build();

        $this->assertEquals('SELECT * FROM t WHERE MATCH(body) AGAINST(?)', $result['query']);
    }

    public function testWrapCharEmptyForHaving(): void
    {
        $result = (new Builder())
            ->setWrapChar('')
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();

        $this->assertStringContainsString('HAVING cnt > ?', $result['query']);
    }

    // ══════════════════════════════════════════
    //  5. Standalone Compiler method calls
    // ══════════════════════════════════════════

    public function testCompileFilterEqual(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::equal('col', ['a', 'b']));
        $this->assertEquals('`col` IN (?, ?)', $sql);
        $this->assertEquals(['a', 'b'], $builder->getBindings());
    }

    public function testCompileFilterNotEqual(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notEqual('col', 'a'));
        $this->assertEquals('`col` != ?', $sql);
        $this->assertEquals(['a'], $builder->getBindings());
    }

    public function testCompileFilterLessThan(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::lessThan('col', 10));
        $this->assertEquals('`col` < ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testCompileFilterLessThanEqual(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::lessThanEqual('col', 10));
        $this->assertEquals('`col` <= ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testCompileFilterGreaterThan(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::greaterThan('col', 10));
        $this->assertEquals('`col` > ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testCompileFilterGreaterThanEqual(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::greaterThanEqual('col', 10));
        $this->assertEquals('`col` >= ?', $sql);
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testCompileFilterBetween(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::between('col', 1, 100));
        $this->assertEquals('`col` BETWEEN ? AND ?', $sql);
        $this->assertEquals([1, 100], $builder->getBindings());
    }

    public function testCompileFilterNotBetween(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notBetween('col', 1, 100));
        $this->assertEquals('`col` NOT BETWEEN ? AND ?', $sql);
        $this->assertEquals([1, 100], $builder->getBindings());
    }

    public function testCompileFilterStartsWith(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::startsWith('col', 'abc'));
        $this->assertEquals('`col` LIKE ?', $sql);
        $this->assertEquals(['abc%'], $builder->getBindings());
    }

    public function testCompileFilterNotStartsWith(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notStartsWith('col', 'abc'));
        $this->assertEquals('`col` NOT LIKE ?', $sql);
        $this->assertEquals(['abc%'], $builder->getBindings());
    }

    public function testCompileFilterEndsWith(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::endsWith('col', 'xyz'));
        $this->assertEquals('`col` LIKE ?', $sql);
        $this->assertEquals(['%xyz'], $builder->getBindings());
    }

    public function testCompileFilterNotEndsWith(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notEndsWith('col', 'xyz'));
        $this->assertEquals('`col` NOT LIKE ?', $sql);
        $this->assertEquals(['%xyz'], $builder->getBindings());
    }

    public function testCompileFilterContainsSingle(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::contains('col', ['val']));
        $this->assertEquals('`col` LIKE ?', $sql);
        $this->assertEquals(['%val%'], $builder->getBindings());
    }

    public function testCompileFilterContainsMultiple(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::contains('col', ['a', 'b']));
        $this->assertEquals('(`col` LIKE ? OR `col` LIKE ?)', $sql);
        $this->assertEquals(['%a%', '%b%'], $builder->getBindings());
    }

    public function testCompileFilterContainsAny(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::containsAny('col', ['a', 'b']));
        $this->assertEquals('`col` IN (?, ?)', $sql);
        $this->assertEquals(['a', 'b'], $builder->getBindings());
    }

    public function testCompileFilterContainsAll(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::containsAll('col', ['a', 'b']));
        $this->assertEquals('(`col` LIKE ? AND `col` LIKE ?)', $sql);
        $this->assertEquals(['%a%', '%b%'], $builder->getBindings());
    }

    public function testCompileFilterNotContainsSingle(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notContains('col', ['val']));
        $this->assertEquals('`col` NOT LIKE ?', $sql);
        $this->assertEquals(['%val%'], $builder->getBindings());
    }

    public function testCompileFilterNotContainsMultiple(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notContains('col', ['a', 'b']));
        $this->assertEquals('(`col` NOT LIKE ? AND `col` NOT LIKE ?)', $sql);
        $this->assertEquals(['%a%', '%b%'], $builder->getBindings());
    }

    public function testCompileFilterIsNull(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::isNull('col'));
        $this->assertEquals('`col` IS NULL', $sql);
        $this->assertEquals([], $builder->getBindings());
    }

    public function testCompileFilterIsNotNull(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::isNotNull('col'));
        $this->assertEquals('`col` IS NOT NULL', $sql);
        $this->assertEquals([], $builder->getBindings());
    }

    public function testCompileFilterAnd(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::and([
            Query::equal('a', [1]),
            Query::greaterThan('b', 2),
        ]));
        $this->assertEquals('(`a` IN (?) AND `b` > ?)', $sql);
        $this->assertEquals([1, 2], $builder->getBindings());
    }

    public function testCompileFilterOr(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::or([
            Query::equal('a', [1]),
            Query::equal('b', [2]),
        ]));
        $this->assertEquals('(`a` IN (?) OR `b` IN (?))', $sql);
        $this->assertEquals([1, 2], $builder->getBindings());
    }

    public function testCompileFilterExists(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::exists(['a', 'b']));
        $this->assertEquals('(`a` IS NOT NULL AND `b` IS NOT NULL)', $sql);
    }

    public function testCompileFilterNotExists(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notExists(['a', 'b']));
        $this->assertEquals('(`a` IS NULL AND `b` IS NULL)', $sql);
    }

    public function testCompileFilterRaw(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::raw('x > ? AND y < ?', [1, 2]));
        $this->assertEquals('x > ? AND y < ?', $sql);
        $this->assertEquals([1, 2], $builder->getBindings());
    }

    public function testCompileFilterSearch(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::search('body', 'hello'));
        $this->assertEquals('MATCH(`body`) AGAINST(?)', $sql);
        $this->assertEquals(['hello'], $builder->getBindings());
    }

    public function testCompileFilterNotSearch(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::notSearch('body', 'spam'));
        $this->assertEquals('NOT (MATCH(`body`) AGAINST(?))', $sql);
        $this->assertEquals(['spam'], $builder->getBindings());
    }

    public function testCompileFilterRegex(): void
    {
        $builder = new Builder();
        $sql = $builder->compileFilter(Query::regex('col', '^abc'));
        $this->assertEquals('`col` REGEXP ?', $sql);
        $this->assertEquals(['^abc'], $builder->getBindings());
    }

    public function testCompileOrderAsc(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderAsc('name'));
        $this->assertEquals('`name` ASC', $sql);
    }

    public function testCompileOrderDesc(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderDesc('name'));
        $this->assertEquals('`name` DESC', $sql);
    }

    public function testCompileOrderRandom(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOrder(Query::orderRandom());
        $this->assertEquals('RAND()', $sql);
    }

    public function testCompileLimitStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileLimit(Query::limit(25));
        $this->assertEquals('LIMIT ?', $sql);
        $this->assertEquals([25], $builder->getBindings());
    }

    public function testCompileOffsetStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOffset(Query::offset(50));
        $this->assertEquals('OFFSET ?', $sql);
        $this->assertEquals([50], $builder->getBindings());
    }

    public function testCompileSelectStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileSelect(Query::select(['a', 'b', 'c']));
        $this->assertEquals('`a`, `b`, `c`', $sql);
    }

    public function testCompileCursorAfterStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileCursor(Query::cursorAfter('abc'));
        $this->assertEquals('`_cursor` > ?', $sql);
        $this->assertEquals(['abc'], $builder->getBindings());
    }

    public function testCompileCursorBeforeStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileCursor(Query::cursorBefore('xyz'));
        $this->assertEquals('`_cursor` < ?', $sql);
        $this->assertEquals(['xyz'], $builder->getBindings());
    }

    public function testCompileAggregateCountStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::count('*', 'total'));
        $this->assertEquals('COUNT(*) AS `total`', $sql);
    }

    public function testCompileAggregateCountWithoutAlias(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::count());
        $this->assertEquals('COUNT(*)', $sql);
    }

    public function testCompileAggregateSumStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::sum('price', 'total'));
        $this->assertEquals('SUM(`price`) AS `total`', $sql);
    }

    public function testCompileAggregateAvgStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::avg('score', 'avg_score'));
        $this->assertEquals('AVG(`score`) AS `avg_score`', $sql);
    }

    public function testCompileAggregateMinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::min('price', 'lowest'));
        $this->assertEquals('MIN(`price`) AS `lowest`', $sql);
    }

    public function testCompileAggregateMaxStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::max('price', 'highest'));
        $this->assertEquals('MAX(`price`) AS `highest`', $sql);
    }

    public function testCompileGroupByStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileGroupBy(Query::groupBy(['status', 'country']));
        $this->assertEquals('`status`, `country`', $sql);
    }

    public function testCompileJoinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileJoin(Query::join('orders', 'users.id', 'orders.uid'));
        $this->assertEquals('JOIN `orders` ON `users`.`id` = `orders`.`uid`', $sql);
    }

    public function testCompileLeftJoinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileJoin(Query::leftJoin('profiles', 'users.id', 'profiles.uid'));
        $this->assertEquals('LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`uid`', $sql);
    }

    public function testCompileRightJoinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileJoin(Query::rightJoin('orders', 'users.id', 'orders.uid'));
        $this->assertEquals('RIGHT JOIN `orders` ON `users`.`id` = `orders`.`uid`', $sql);
    }

    public function testCompileCrossJoinStandalone(): void
    {
        $builder = new Builder();
        $sql = $builder->compileJoin(Query::crossJoin('colors'));
        $this->assertEquals('CROSS JOIN `colors`', $sql);
    }

    // ══════════════════════════════════════════
    //  6. Filter edge cases
    // ══════════════════════════════════════════

    public function testEqualWithSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?)', $result['query']);
        $this->assertEquals(['active'], $result['bindings']);
    }

    public function testEqualWithManyValues(): void
    {
        $values = range(1, 10);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('id', $values)])
            ->build();

        $placeholders = implode(', ', array_fill(0, 10, '?'));
        $this->assertEquals("SELECT * FROM `t` WHERE `id` IN ({$placeholders})", $result['query']);
        $this->assertEquals($values, $result['bindings']);
    }

    public function testEqualWithEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('id', [])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 0', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testNotEqualWithExactlyTwoValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', ['guest', 'banned'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `role` NOT IN (?, ?)', $result['query']);
        $this->assertEquals(['guest', 'banned'], $result['bindings']);
    }

    public function testBetweenWithSameMinAndMax(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 25, 25)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([25, 25], $result['bindings']);
    }

    public function testStartsWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', '')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result['query']);
        $this->assertEquals(['%'], $result['bindings']);
    }

    public function testEndsWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('name', '')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result['query']);
        $this->assertEquals(['%'], $result['bindings']);
    }

    public function testContainsWithSingleEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', [''])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result['query']);
        $this->assertEquals(['%%'], $result['bindings']);
    }

    public function testContainsWithManyValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['a', 'b', 'c', 'd', 'e'])])
            ->build();

        $this->assertStringContainsString('(`bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ?)', $result['query']);
        $this->assertEquals(['%a%', '%b%', '%c%', '%d%', '%e%'], $result['bindings']);
    }

    public function testContainsAllWithSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('perms', ['read'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`perms` LIKE ?)', $result['query']);
        $this->assertEquals(['%read%'], $result['bindings']);
    }

    public function testNotContainsWithEmptyStringValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', [''])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` NOT LIKE ?', $result['query']);
        $this->assertEquals(['%%'], $result['bindings']);
    }

    public function testComparisonWithFloatValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('price', 9.99)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `price` > ?', $result['query']);
        $this->assertEquals([9.99], $result['bindings']);
    }

    public function testComparisonWithNegativeValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('balance', -100)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `balance` < ?', $result['query']);
        $this->assertEquals([-100], $result['bindings']);
    }

    public function testComparisonWithZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 0)])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `score` >= ?', $result['query']);
        $this->assertEquals([0], $result['bindings']);
    }

    public function testComparisonWithVeryLargeInteger(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('id', 9999999999999)])
            ->build();

        $this->assertEquals([9999999999999], $result['bindings']);
    }

    public function testComparisonWithStringValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('name', 'M')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `name` > ?', $result['query']);
        $this->assertEquals(['M'], $result['bindings']);
    }

    public function testBetweenWithStringValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('created_at', '2024-01-01', '2024-12-31')])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `created_at` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals(['2024-01-01', '2024-12-31'], $result['bindings']);
    }

    public function testIsNullCombinedWithIsNotNullOnDifferentColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::isNull('deleted_at'),
                Query::isNotNull('verified_at'),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `deleted_at` IS NULL AND `verified_at` IS NOT NULL',
            $result['query']
        );
        $this->assertEquals([], $result['bindings']);
    }

    public function testMultipleIsNullFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::isNull('a'),
                Query::isNull('b'),
                Query::isNull('c'),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `a` IS NULL AND `b` IS NULL AND `c` IS NULL',
            $result['query']
        );
    }

    public function testExistsWithSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`name` IS NOT NULL)', $result['query']);
    }

    public function testExistsWithManyAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['a', 'b', 'c', 'd'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IS NOT NULL AND `b` IS NOT NULL AND `c` IS NOT NULL AND `d` IS NOT NULL)',
            $result['query']
        );
    }

    public function testNotExistsWithManyAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['a', 'b', 'c'])])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IS NULL AND `b` IS NULL AND `c` IS NULL)',
            $result['query']
        );
    }

    public function testAndWithSingleSubQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::equal('a', [1]),
                ]),
            ])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?))', $result['query']);
        $this->assertEquals([1], $result['bindings']);
    }

    public function testOrWithSingleSubQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::equal('a', [1]),
                ]),
            ])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?))', $result['query']);
        $this->assertEquals([1], $result['bindings']);
    }

    public function testAndWithManySubQueries(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::equal('a', [1]),
                    Query::equal('b', [2]),
                    Query::equal('c', [3]),
                    Query::equal('d', [4]),
                    Query::equal('e', [5]),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IN (?) AND `b` IN (?) AND `c` IN (?) AND `d` IN (?) AND `e` IN (?))',
            $result['query']
        );
        $this->assertEquals([1, 2, 3, 4, 5], $result['bindings']);
    }

    public function testOrWithManySubQueries(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::equal('a', [1]),
                    Query::equal('b', [2]),
                    Query::equal('c', [3]),
                    Query::equal('d', [4]),
                    Query::equal('e', [5]),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IN (?) OR `b` IN (?) OR `c` IN (?) OR `d` IN (?) OR `e` IN (?))',
            $result['query']
        );
    }

    public function testDeeplyNestedAndOrAnd(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::or([
                        Query::and([
                            Query::equal('a', [1]),
                            Query::equal('b', [2]),
                        ]),
                        Query::equal('c', [3]),
                    ]),
                    Query::equal('d', [4]),
                ]),
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (((`a` IN (?) AND `b` IN (?)) OR `c` IN (?)) AND `d` IN (?))',
            $result['query']
        );
        $this->assertEquals([1, 2, 3, 4], $result['bindings']);
    }

    public function testRawWithManyBindings(): void
    {
        $bindings = range(1, 10);
        $placeholders = implode(' AND ', array_map(fn ($i) => "col{$i} = ?", range(1, 10)));
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw($placeholders, $bindings)])
            ->build();

        $this->assertEquals("SELECT * FROM `t` WHERE {$placeholders}", $result['query']);
        $this->assertEquals($bindings, $result['bindings']);
    }

    public function testFilterWithDotsInAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('table.column', ['value'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `table`.`column` IN (?)', $result['query']);
    }

    public function testFilterWithUnderscoresInAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('my_column_name', ['value'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `my_column_name` IN (?)', $result['query']);
    }

    public function testFilterWithNumericAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('123', ['value'])])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `123` IN (?)', $result['query']);
    }

    // ══════════════════════════════════════════
    //  7. Aggregation edge cases
    // ══════════════════════════════════════════

    public function testCountWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->count()->build();
        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
    }

    public function testSumWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->sum('price')->build();
        $this->assertEquals('SELECT SUM(`price`) FROM `t`', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
    }

    public function testAvgWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->avg('score')->build();
        $this->assertEquals('SELECT AVG(`score`) FROM `t`', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
    }

    public function testMinWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->min('price')->build();
        $this->assertEquals('SELECT MIN(`price`) FROM `t`', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
    }

    public function testMaxWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->max('price')->build();
        $this->assertEquals('SELECT MAX(`price`) FROM `t`', $result['query']);
        $this->assertStringNotContainsString(' AS ', $result['query']);
    }

    public function testCountWithAlias2(): void
    {
        $result = (new Builder())->from('t')->count('*', 'cnt')->build();
        $this->assertStringContainsString('AS `cnt`', $result['query']);
    }

    public function testSumWithAlias(): void
    {
        $result = (new Builder())->from('t')->sum('price', 'total')->build();
        $this->assertStringContainsString('AS `total`', $result['query']);
    }

    public function testAvgWithAlias(): void
    {
        $result = (new Builder())->from('t')->avg('score', 'avg_s')->build();
        $this->assertStringContainsString('AS `avg_s`', $result['query']);
    }

    public function testMinWithAlias(): void
    {
        $result = (new Builder())->from('t')->min('price', 'lowest')->build();
        $this->assertStringContainsString('AS `lowest`', $result['query']);
    }

    public function testMaxWithAlias(): void
    {
        $result = (new Builder())->from('t')->max('price', 'highest')->build();
        $this->assertStringContainsString('AS `highest`', $result['query']);
    }

    public function testMultipleSameAggregationType(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('id', 'count_id')
            ->count('*', 'count_all')
            ->build();

        $this->assertEquals(
            'SELECT COUNT(`id`) AS `count_id`, COUNT(*) AS `count_all` FROM `t`',
            $result['query']
        );
    }

    public function testAggregationStarAndNamedColumnMixed(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->sum('price', 'price_sum')
            ->select(['category'])
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
        $this->assertStringContainsString('SUM(`price`) AS `price_sum`', $result['query']);
        $this->assertStringContainsString('`category`', $result['query']);
    }

    public function testAggregationFilterSortLimitCombined(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->filter([Query::equal('status', ['paid'])])
            ->groupBy(['category'])
            ->sortDesc('cnt')
            ->limit(5)
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result['query']);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
        $this->assertStringContainsString('GROUP BY `category`', $result['query']);
        $this->assertStringContainsString('ORDER BY `cnt` DESC', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertEquals(['paid', 5], $result['bindings']);
    }

    public function testAggregationJoinGroupByHavingSortLimitFullPipeline(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->sum('total', 'revenue')
            ->select(['users.name'])
            ->join('users', 'orders.user_id', 'users.id')
            ->filter([Query::greaterThan('orders.total', 0)])
            ->groupBy(['users.name'])
            ->having([Query::greaterThan('cnt', 2)])
            ->sortDesc('revenue')
            ->limit(20)
            ->offset(10)
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result['query']);
        $this->assertStringContainsString('SUM(`total`) AS `revenue`', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('WHERE `orders`.`total` > ?', $result['query']);
        $this->assertStringContainsString('GROUP BY `users`.`name`', $result['query']);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
        $this->assertStringContainsString('ORDER BY `revenue` DESC', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        $this->assertEquals([0, 2, 20, 10], $result['bindings']);
    }

    public function testAggregationWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$amount' => '_amount',
                default => $a,
            })
            ->sum('$amount', 'total')
            ->build();

        $this->assertEquals('SELECT SUM(`_amount`) AS `total` FROM `t`', $result['query']);
    }

    public function testAggregationWithWrapChar(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->avg('score', 'average')
            ->build();

        $this->assertEquals('SELECT AVG("score") AS "average" FROM "t"', $result['query']);
    }

    public function testMinMaxWithStringColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->min('name', 'first_name')
            ->max('name', 'last_name')
            ->build();

        $this->assertEquals(
            'SELECT MIN(`name`) AS `first_name`, MAX(`name`) AS `last_name` FROM `t`',
            $result['query']
        );
    }

    // ══════════════════════════════════════════
    //  8. Join edge cases
    // ══════════════════════════════════════════

    public function testSelfJoin(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->join('employees', 'employees.manager_id', 'employees.id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `employees` JOIN `employees` ON `employees`.`manager_id` = `employees`.`id`',
            $result['query']
        );
    }

    public function testJoinWithVeryLongTableAndColumnNames(): void
    {
        $longTable = str_repeat('a', 100);
        $longLeft = str_repeat('b', 100);
        $longRight = str_repeat('c', 100);
        $result = (new Builder())
            ->from('main')
            ->join($longTable, $longLeft, $longRight)
            ->build();

        $this->assertStringContainsString("JOIN `{$longTable}`", $result['query']);
        $this->assertStringContainsString("ON `{$longLeft}` = `{$longRight}`", $result['query']);
    }

    public function testJoinFilterSortLimitOffsetCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->filter([
                Query::equal('orders.status', ['paid']),
                Query::greaterThan('orders.total', 100),
            ])
            ->sortDesc('orders.total')
            ->limit(25)
            ->offset(50)
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
        $this->assertStringContainsString('WHERE `orders`.`status` IN (?) AND `orders`.`total` > ?', $result['query']);
        $this->assertStringContainsString('ORDER BY `orders`.`total` DESC', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertStringContainsString('OFFSET ?', $result['query']);
        $this->assertEquals(['paid', 100, 25, 50], $result['bindings']);
    }

    public function testJoinAggregationGroupByHavingCombined(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->join('users', 'orders.user_id', 'users.id')
            ->groupBy(['users.name'])
            ->having([Query::greaterThan('cnt', 3)])
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result['query']);
        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('GROUP BY `users`.`name`', $result['query']);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
        $this->assertEquals([3], $result['bindings']);
    }

    public function testJoinWithDistinct(): void
    {
        $result = (new Builder())
            ->from('users')
            ->distinct()
            ->select(['users.name'])
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();

        $this->assertStringContainsString('SELECT DISTINCT `users`.`name`', $result['query']);
        $this->assertStringContainsString('JOIN `orders`', $result['query']);
    }

    public function testJoinWithUnion(): void
    {
        $sub = (new Builder())
            ->from('archived_users')
            ->join('archived_orders', 'archived_users.id', 'archived_orders.user_id');

        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->union($sub)
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
        $this->assertStringContainsString('JOIN `archived_orders`', $result['query']);
    }

    public function testFourJoins(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('users', 'orders.user_id', 'users.id')
            ->leftJoin('products', 'orders.product_id', 'products.id')
            ->rightJoin('categories', 'products.cat_id', 'categories.id')
            ->crossJoin('promotions')
            ->build();

        $this->assertStringContainsString('JOIN `users`', $result['query']);
        $this->assertStringContainsString('LEFT JOIN `products`', $result['query']);
        $this->assertStringContainsString('RIGHT JOIN `categories`', $result['query']);
        $this->assertStringContainsString('CROSS JOIN `promotions`', $result['query']);
    }

    public function testJoinWithAttributeResolverOnJoinColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => match ($a) {
                '$id' => '_uid',
                '$ref' => '_ref_id',
                default => $a,
            })
            ->join('other', '$id', '$ref')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` JOIN `other` ON `_uid` = `_ref_id`',
            $result['query']
        );
    }

    public function testCrossJoinCombinedWithFilter(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->filter([Query::equal('sizes.active', [true])])
            ->build();

        $this->assertStringContainsString('CROSS JOIN `colors`', $result['query']);
        $this->assertStringContainsString('WHERE `sizes`.`active` IN (?)', $result['query']);
    }

    public function testCrossJoinFollowedByRegularJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->join('c', 'a.id', 'c.a_id')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `a` CROSS JOIN `b` JOIN `c` ON `a`.`id` = `c`.`a_id`',
            $result['query']
        );
    }

    public function testMultipleJoinsWithFiltersOnEach(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->filter([
                Query::greaterThan('orders.total', 50),
                Query::isNotNull('profiles.avatar'),
            ])
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
        $this->assertStringContainsString('LEFT JOIN `profiles`', $result['query']);
        $this->assertStringContainsString('`orders`.`total` > ?', $result['query']);
        $this->assertStringContainsString('`profiles`.`avatar` IS NOT NULL', $result['query']);
    }

    public function testJoinWithCustomOperatorLessThan(): void
    {
        $result = (new Builder())
            ->from('a')
            ->join('b', 'a.start', 'b.end', '<')
            ->build();

        $this->assertEquals(
            'SELECT * FROM `a` JOIN `b` ON `a`.`start` < `b`.`end`',
            $result['query']
        );
    }

    public function testFiveJoins(): void
    {
        $result = (new Builder())
            ->from('t1')
            ->join('t2', 't1.id', 't2.t1_id')
            ->join('t3', 't2.id', 't3.t2_id')
            ->join('t4', 't3.id', 't4.t3_id')
            ->join('t5', 't4.id', 't5.t4_id')
            ->join('t6', 't5.id', 't6.t5_id')
            ->build();

        $query = $result['query'];
        $this->assertEquals(5, substr_count($query, 'JOIN'));
    }

    // ══════════════════════════════════════════
    //  9. Union edge cases
    // ══════════════════════════════════════════

    public function testUnionWithThreeSubQueries(): void
    {
        $q1 = (new Builder())->from('a');
        $q2 = (new Builder())->from('b');
        $q3 = (new Builder())->from('c');

        $result = (new Builder())
            ->from('main')
            ->union($q1)
            ->union($q2)
            ->union($q3)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `main` UNION SELECT * FROM `a` UNION SELECT * FROM `b` UNION SELECT * FROM `c`',
            $result['query']
        );
    }

    public function testUnionAllWithThreeSubQueries(): void
    {
        $q1 = (new Builder())->from('a');
        $q2 = (new Builder())->from('b');
        $q3 = (new Builder())->from('c');

        $result = (new Builder())
            ->from('main')
            ->unionAll($q1)
            ->unionAll($q2)
            ->unionAll($q3)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `main` UNION ALL SELECT * FROM `a` UNION ALL SELECT * FROM `b` UNION ALL SELECT * FROM `c`',
            $result['query']
        );
    }

    public function testMixedUnionAndUnionAllWithThreeSubQueries(): void
    {
        $q1 = (new Builder())->from('a');
        $q2 = (new Builder())->from('b');
        $q3 = (new Builder())->from('c');

        $result = (new Builder())
            ->from('main')
            ->union($q1)
            ->unionAll($q2)
            ->union($q3)
            ->build();

        $this->assertEquals(
            'SELECT * FROM `main` UNION SELECT * FROM `a` UNION ALL SELECT * FROM `b` UNION SELECT * FROM `c`',
            $result['query']
        );
    }

    public function testUnionWhereSubQueryHasJoins(): void
    {
        $sub = (new Builder())
            ->from('archived_users')
            ->join('archived_orders', 'archived_users.id', 'archived_orders.user_id');

        $result = (new Builder())
            ->from('users')
            ->union($sub)
            ->build();

        $this->assertStringContainsString(
            'UNION SELECT * FROM `archived_users` JOIN `archived_orders`',
            $result['query']
        );
    }

    public function testUnionWhereSubQueryHasAggregation(): void
    {
        $sub = (new Builder())
            ->from('orders_2023')
            ->count('*', 'cnt')
            ->groupBy(['status']);

        $result = (new Builder())
            ->from('orders_2024')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->union($sub)
            ->build();

        $this->assertStringContainsString('UNION SELECT COUNT(*) AS `cnt` FROM `orders_2023` GROUP BY `status`', $result['query']);
    }

    public function testUnionWhereSubQueryHasSortAndLimit(): void
    {
        $sub = (new Builder())
            ->from('archive')
            ->sortDesc('created_at')
            ->limit(10);

        $result = (new Builder())
            ->from('current')
            ->union($sub)
            ->build();

        $this->assertStringContainsString('UNION SELECT * FROM `archive` ORDER BY `created_at` DESC LIMIT ?', $result['query']);
    }

    public function testUnionWithConditionProviders(): void
    {
        $sub = (new Builder())
            ->from('other')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org2']]);

        $result = (new Builder())
            ->from('main')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->union($sub)
            ->build();

        $this->assertStringContainsString('WHERE org = ?', $result['query']);
        $this->assertStringContainsString('UNION SELECT * FROM `other` WHERE org = ?', $result['query']);
        $this->assertEquals(['org1', 'org2'], $result['bindings']);
    }

    public function testUnionBindingOrderWithComplexSubQueries(): void
    {
        $sub = (new Builder())
            ->from('archive')
            ->filter([Query::equal('year', [2023])])
            ->limit(5);

        $result = (new Builder())
            ->from('current')
            ->filter([Query::equal('status', ['active'])])
            ->limit(10)
            ->union($sub)
            ->build();

        $this->assertEquals(['active', 10, 2023, 5], $result['bindings']);
    }

    public function testUnionWithDistinct(): void
    {
        $sub = (new Builder())
            ->from('archive')
            ->distinct()
            ->select(['name']);

        $result = (new Builder())
            ->from('current')
            ->distinct()
            ->select(['name'])
            ->union($sub)
            ->build();

        $this->assertStringContainsString('SELECT DISTINCT `name` FROM `current`', $result['query']);
        $this->assertStringContainsString('UNION SELECT DISTINCT `name` FROM `archive`', $result['query']);
    }

    public function testUnionWithWrapChar(): void
    {
        $sub = (new Builder())
            ->setWrapChar('"')
            ->from('archive');

        $result = (new Builder())
            ->setWrapChar('"')
            ->from('current')
            ->union($sub)
            ->build();

        $this->assertEquals(
            'SELECT * FROM "current" UNION SELECT * FROM "archive"',
            $result['query']
        );
    }

    public function testUnionAfterReset(): void
    {
        $builder = (new Builder())->from('old');
        $builder->build();
        $builder->reset();

        $sub = (new Builder())->from('other');
        $result = $builder->from('fresh')->union($sub)->build();

        $this->assertEquals(
            'SELECT * FROM `fresh` UNION SELECT * FROM `other`',
            $result['query']
        );
    }

    public function testUnionChainedWithComplexBindings(): void
    {
        $q1 = (new Builder())
            ->from('a')
            ->filter([Query::equal('x', [1]), Query::greaterThan('y', 2)]);
        $q2 = (new Builder())
            ->from('b')
            ->filter([Query::between('z', 10, 20)]);

        $result = (new Builder())
            ->from('main')
            ->filter([Query::equal('status', ['active'])])
            ->union($q1)
            ->unionAll($q2)
            ->build();

        $this->assertEquals(['active', 1, 2, 10, 20], $result['bindings']);
    }

    public function testUnionWithFourSubQueries(): void
    {
        $q1 = (new Builder())->from('t1');
        $q2 = (new Builder())->from('t2');
        $q3 = (new Builder())->from('t3');
        $q4 = (new Builder())->from('t4');

        $result = (new Builder())
            ->from('main')
            ->union($q1)
            ->union($q2)
            ->union($q3)
            ->union($q4)
            ->build();

        $this->assertEquals(4, substr_count($result['query'], 'UNION'));
    }

    public function testUnionAllWithFilteredSubQueries(): void
    {
        $q1 = (new Builder())->from('orders_2022')->filter([Query::equal('status', ['paid'])]);
        $q2 = (new Builder())->from('orders_2023')->filter([Query::equal('status', ['paid'])]);
        $q3 = (new Builder())->from('orders_2024')->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->from('orders_2025')
            ->filter([Query::equal('status', ['paid'])])
            ->unionAll($q1)
            ->unionAll($q2)
            ->unionAll($q3)
            ->build();

        $this->assertEquals(['paid', 'paid', 'paid', 'paid'], $result['bindings']);
        $this->assertEquals(3, substr_count($result['query'], 'UNION ALL'));
    }

    // ══════════════════════════════════════════
    //  10. toRawSql edge cases
    // ══════════════════════════════════════════

    public function testToRawSqlWithAllBindingTypesInOneQuery(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('name', ['Alice']),
                Query::greaterThan('age', 18),
                Query::raw('active = ?', [true]),
                Query::raw('deleted = ?', [null]),
                Query::raw('score > ?', [9.5]),
            ])
            ->limit(10)
            ->toRawSql();

        $this->assertStringContainsString("'Alice'", $sql);
        $this->assertStringContainsString('18', $sql);
        $this->assertStringContainsString('= 1', $sql);
        $this->assertStringContainsString('= NULL', $sql);
        $this->assertStringContainsString('9.5', $sql);
        $this->assertStringContainsString('10', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithEmptyStringBinding(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::equal('name', [''])])
            ->toRawSql();

        $this->assertStringContainsString("''", $sql);
    }

    public function testToRawSqlWithStringContainingSingleQuotes(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::equal('name', ["O'Brien"])])
            ->toRawSql();

        $this->assertStringContainsString("O''Brien", $sql);
    }

    public function testToRawSqlWithVeryLargeNumber(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('id', 99999999999)])
            ->toRawSql();

        $this->assertStringContainsString('99999999999', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithNegativeNumber(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('balance', -500)])
            ->toRawSql();

        $this->assertStringContainsString('-500', $sql);
    }

    public function testToRawSqlWithZero(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::equal('count', [0])])
            ->toRawSql();

        $this->assertStringContainsString('IN (0)', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithFalseBoolean(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::raw('active = ?', [false])])
            ->toRawSql();

        $this->assertStringContainsString('active = 0', $sql);
    }

    public function testToRawSqlWithMultipleNullBindings(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::raw('a = ? AND b = ?', [null, null])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM `t` WHERE a = NULL AND b = NULL", $sql);
    }

    public function testToRawSqlWithAggregationQuery(): void
    {
        $sql = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 5)])
            ->toRawSql();

        $this->assertStringContainsString('COUNT(*) AS `total`', $sql);
        $this->assertStringContainsString('HAVING `total` > 5', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithJoinQuery(): void
    {
        $sql = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->filter([Query::greaterThan('orders.total', 100)])
            ->toRawSql();

        $this->assertStringContainsString('JOIN `orders`', $sql);
        $this->assertStringContainsString('100', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithUnionQuery(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);

        $sql = (new Builder())
            ->from('current')
            ->filter([Query::equal('year', [2024])])
            ->union($sub)
            ->toRawSql();

        $this->assertStringContainsString('2024', $sql);
        $this->assertStringContainsString('2023', $sql);
        $this->assertStringContainsString('UNION', $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlWithRegexAndSearch(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([
                Query::regex('slug', '^test'),
                Query::search('content', 'hello'),
            ])
            ->toRawSql();

        $this->assertStringContainsString("REGEXP '^test'", $sql);
        $this->assertStringContainsString("AGAINST('hello')", $sql);
        $this->assertStringNotContainsString('?', $sql);
    }

    public function testToRawSqlCalledTwiceGivesSameResult(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->limit(10);

        $sql1 = $builder->toRawSql();
        $sql2 = $builder->toRawSql();

        $this->assertEquals($sql1, $sql2);
    }

    public function testToRawSqlWithWrapChar(): void
    {
        $sql = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->toRawSql();

        $this->assertEquals("SELECT * FROM \"t\" WHERE \"status\" IN ('active')", $sql);
    }

    // ══════════════════════════════════════════
    //  11. when() edge cases
    // ══════════════════════════════════════════

    public function testWhenWithComplexCallbackAddingMultipleFeatures(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, function (Builder $b) {
                $b->filter([Query::equal('status', ['active'])])
                    ->sortAsc('name')
                    ->limit(10);
            })
            ->build();

        $this->assertStringContainsString('WHERE `status` IN (?)', $result['query']);
        $this->assertStringContainsString('ORDER BY `name` ASC', $result['query']);
        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertEquals(['active', 10], $result['bindings']);
    }

    public function testWhenChainedFiveTimes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('a', [1])]))
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('b', [2])]))
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('c', [3])]))
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('d', [4])]))
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('e', [5])]))
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `a` IN (?) AND `b` IN (?) AND `d` IN (?) AND `e` IN (?)',
            $result['query']
        );
        $this->assertEquals([1, 2, 4, 5], $result['bindings']);
    }

    public function testWhenInsideWhenThreeLevelsDeep(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, function (Builder $b) {
                $b->when(true, function (Builder $b2) {
                    $b2->when(true, fn (Builder $b3) => $b3->filter([Query::equal('deep', [1])]));
                });
            })
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE `deep` IN (?)', $result['query']);
        $this->assertEquals([1], $result['bindings']);
    }

    public function testWhenThatAddsJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->when(true, fn (Builder $b) => $b->join('orders', 'users.id', 'orders.uid'))
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
    }

    public function testWhenThatAddsAggregations(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->count('*', 'total')->groupBy(['status']))
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
        $this->assertStringContainsString('GROUP BY `status`', $result['query']);
    }

    public function testWhenThatAddsUnions(): void
    {
        $sub = (new Builder())->from('archive');

        $result = (new Builder())
            ->from('current')
            ->when(true, fn (Builder $b) => $b->union($sub))
            ->build();

        $this->assertStringContainsString('UNION', $result['query']);
    }

    public function testWhenFalseDoesNotAffectFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('status', ['banned'])]))
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testWhenFalseDoesNotAffectJoins(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->join('other', 'a', 'b'))
            ->build();

        $this->assertStringNotContainsString('JOIN', $result['query']);
    }

    public function testWhenFalseDoesNotAffectAggregations(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->count('*', 'total'))
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    public function testWhenFalseDoesNotAffectSort(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->sortAsc('name'))
            ->build();

        $this->assertStringNotContainsString('ORDER BY', $result['query']);
    }

    // ══════════════════════════════════════════
    //  12. Condition provider edge cases
    // ══════════════════════════════════════════

    public function testThreeConditionProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['p1 = ?', ['v1']])
            ->addConditionProvider(fn (string $t): array => ['p2 = ?', ['v2']])
            ->addConditionProvider(fn (string $t): array => ['p3 = ?', ['v3']])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE p1 = ? AND p2 = ? AND p3 = ?',
            $result['query']
        );
        $this->assertEquals(['v1', 'v2', 'v3'], $result['bindings']);
    }

    public function testProviderReturningEmptyConditionString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['', []])
            ->build();

        // Empty string still appears as a WHERE clause element
        $this->assertStringContainsString('WHERE', $result['query']);
    }

    public function testProviderWithManyBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => [
                'a IN (?, ?, ?, ?, ?)',
                [1, 2, 3, 4, 5],
            ])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE a IN (?, ?, ?, ?, ?)',
            $result['query']
        );
        $this->assertEquals([1, 2, 3, 4, 5], $result['bindings']);
    }

    public function testProviderCombinedWithCursorFilterHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->filter([Query::equal('status', ['active'])])
            ->cursorAfter('cur1')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();

        $this->assertStringContainsString('WHERE', $result['query']);
        $this->assertStringContainsString('HAVING', $result['query']);
        // filter, provider, cursor, having
        $this->assertEquals(['active', 'org1', 'cur1', 5], $result['bindings']);
    }

    public function testProviderCombinedWithJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->build();

        $this->assertStringContainsString('JOIN `orders`', $result['query']);
        $this->assertStringContainsString('WHERE tenant = ?', $result['query']);
        $this->assertEquals(['t1'], $result['bindings']);
    }

    public function testProviderCombinedWithUnions(): void
    {
        $sub = (new Builder())->from('archive');

        $result = (new Builder())
            ->from('current')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->union($sub)
            ->build();

        $this->assertStringContainsString('WHERE org = ?', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
        $this->assertEquals(['org1'], $result['bindings']);
    }

    public function testProviderCombinedWithAggregations(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->groupBy(['status'])
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
        $this->assertStringContainsString('WHERE org = ?', $result['query']);
    }

    public function testProviderReferencesTableName(): void
    {
        $result = (new Builder())
            ->from('users')
            ->addConditionProvider(fn (string $table): array => [
                "EXISTS (SELECT 1 FROM {$table}_perms WHERE type = ?)",
                ['read'],
            ])
            ->build();

        $this->assertStringContainsString('users_perms', $result['query']);
        $this->assertEquals(['read'], $result['bindings']);
    }

    public function testProviderWithWrapCharProviderSqlIsLiteral(): void
    {
        $result = (new Builder())
            ->setWrapChar('"')
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['raw_col = ?', [1]])
            ->build();

        // Provider SQL is NOT wrapped - only the FROM clause is
        $this->assertStringContainsString('FROM "t"', $result['query']);
        $this->assertStringContainsString('raw_col = ?', $result['query']);
    }

    public function testProviderBindingOrderWithComplexQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['p1 = ?', ['pv1']])
            ->addConditionProvider(fn (string $t): array => ['p2 = ?', ['pv2']])
            ->filter([
                Query::equal('a', ['va']),
                Query::greaterThan('b', 10),
            ])
            ->cursorAfter('cur')
            ->limit(5)
            ->offset(10)
            ->build();

        // filter, provider1, provider2, cursor, limit, offset
        $this->assertEquals(['va', 10, 'pv1', 'pv2', 'cur', 5, 10], $result['bindings']);
    }

    public function testProviderPreservedAcrossReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertStringContainsString('WHERE org = ?', $result['query']);
        $this->assertEquals(['org1'], $result['bindings']);
    }

    public function testFourConditionProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['a = ?', [1]])
            ->addConditionProvider(fn (string $t): array => ['b = ?', [2]])
            ->addConditionProvider(fn (string $t): array => ['c = ?', [3]])
            ->addConditionProvider(fn (string $t): array => ['d = ?', [4]])
            ->build();

        $this->assertEquals(
            'SELECT * FROM `t` WHERE a = ? AND b = ? AND c = ? AND d = ?',
            $result['query']
        );
        $this->assertEquals([1, 2, 3, 4], $result['bindings']);
    }

    public function testProviderWithNoBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['1 = 1', []])
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 1', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  13. Reset edge cases
    // ══════════════════════════════════════════

    public function testResetPreservesAttributeResolver(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->setAttributeResolver(fn (string $a): string => '_' . $a)
            ->filter([Query::equal('x', [1])]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->filter([Query::equal('y', [2])])->build();
        $this->assertStringContainsString('`_y`', $result['query']);
    }

    public function testResetPreservesConditionProviders(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertStringContainsString('org = ?', $result['query']);
        $this->assertEquals(['org1'], $result['bindings']);
    }

    public function testResetPreservesWrapChar(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->setWrapChar('"');

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->select(['name'])->build();
        $this->assertEquals('SELECT "name" FROM "t2"', $result['query']);
    }

    public function testResetClearsPendingQueries(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->sortAsc('name')
            ->limit(10);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertEquals('SELECT * FROM `t2`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testResetClearsBindings(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])]);

        $builder->build();
        $this->assertNotEmpty($builder->getBindings());

        $builder->reset();
        $result = $builder->from('t2')->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testResetClearsTable(): void
    {
        $builder = (new Builder())->from('old_table');
        $builder->build();
        $builder->reset();

        $result = $builder->from('new_table')->build();
        $this->assertStringContainsString('`new_table`', $result['query']);
        $this->assertStringNotContainsString('`old_table`', $result['query']);
    }

    public function testResetClearsUnionsAfterBuild(): void
    {
        $sub = (new Builder())->from('other');
        $builder = (new Builder())->from('main')->union($sub);
        $builder->build();
        $builder->reset();

        $result = $builder->from('fresh')->build();
        $this->assertStringNotContainsString('UNION', $result['query']);
    }

    public function testBuildAfterResetProducesMinimalQuery(): void
    {
        $builder = (new Builder())
            ->from('complex')
            ->select(['a', 'b'])
            ->filter([Query::equal('x', [1])])
            ->sortAsc('a')
            ->limit(10)
            ->offset(5);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    public function testMultipleResetCalls(): void
    {
        $builder = (new Builder())->from('t')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();
        $builder->reset();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertEquals('SELECT * FROM `t2`', $result['query']);
    }

    public function testResetBetweenDifferentQueryTypes(): void
    {
        $builder = new Builder();

        // First: aggregation query
        $builder->from('orders')->count('*', 'total')->groupBy(['status']);
        $result1 = $builder->build();
        $this->assertStringContainsString('COUNT(*)', $result1['query']);

        $builder->reset();

        // Second: simple select query
        $builder->from('users')->select(['name'])->filter([Query::equal('active', [true])]);
        $result2 = $builder->build();
        $this->assertStringNotContainsString('COUNT', $result2['query']);
        $this->assertStringContainsString('`name`', $result2['query']);
    }

    public function testResetAfterUnion(): void
    {
        $sub = (new Builder())->from('other');
        $builder = (new Builder())->from('main')->union($sub);
        $builder->build();
        $builder->reset();

        $result = $builder->from('new')->build();
        $this->assertEquals('SELECT * FROM `new`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testResetAfterComplexQueryWithAllFeatures(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);

        $builder = (new Builder())
            ->from('orders')
            ->distinct()
            ->count('*', 'cnt')
            ->select(['status'])
            ->join('users', 'orders.uid', 'users.id')
            ->filter([Query::equal('status', ['paid'])])
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->sortDesc('cnt')
            ->limit(10)
            ->offset(5)
            ->union($sub);

        $builder->build();
        $builder->reset();

        $result = $builder->from('simple')->build();
        $this->assertEquals('SELECT * FROM `simple`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  14. Multiple build() calls
    // ══════════════════════════════════════════

    public function testBuildTwiceModifyInBetween(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])]);

        $result1 = $builder->build();

        $builder->filter([Query::equal('b', [2])]);
        $result2 = $builder->build();

        $this->assertStringNotContainsString('`b`', $result1['query']);
        $this->assertStringContainsString('`b`', $result2['query']);
    }

    public function testBuildDoesNotMutatePendingQueries(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->limit(10);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1['query'], $result2['query']);
        $this->assertEquals($result1['bindings'], $result2['bindings']);
    }

    public function testBuildResetsBindingsEachTime(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])]);

        $builder->build();
        $bindings1 = $builder->getBindings();

        $builder->build();
        $bindings2 = $builder->getBindings();

        $this->assertEquals($bindings1, $bindings2);
        $this->assertCount(1, $bindings2);
    }

    public function testBuildWithConditionProducesConsistentBindings(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->filter([Query::equal('status', ['active'])]);

        $result1 = $builder->build();
        $result2 = $builder->build();
        $result3 = $builder->build();

        $this->assertEquals($result1['bindings'], $result2['bindings']);
        $this->assertEquals($result2['bindings'], $result3['bindings']);
    }

    public function testBuildAfterAddingMoreQueries(): void
    {
        $builder = (new Builder())->from('t');

        $result1 = $builder->build();
        $this->assertEquals('SELECT * FROM `t`', $result1['query']);

        $builder->filter([Query::equal('a', [1])]);
        $result2 = $builder->build();
        $this->assertStringContainsString('WHERE', $result2['query']);

        $builder->sortAsc('a');
        $result3 = $builder->build();
        $this->assertStringContainsString('ORDER BY', $result3['query']);
    }

    public function testBuildWithUnionProducesConsistentResults(): void
    {
        $sub = (new Builder())->from('other')->filter([Query::equal('x', [1])]);
        $builder = (new Builder())->from('main')->union($sub);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1['query'], $result2['query']);
        $this->assertEquals($result1['bindings'], $result2['bindings']);
    }

    public function testBuildThreeTimesWithIncreasingComplexity(): void
    {
        $builder = (new Builder())->from('t');

        $r1 = $builder->build();
        $this->assertEquals('SELECT * FROM `t`', $r1['query']);

        $builder->filter([Query::equal('a', [1])]);
        $r2 = $builder->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $r2['query']);

        $builder->limit(10)->offset(5);
        $r3 = $builder->build();
        $this->assertStringContainsString('LIMIT ?', $r3['query']);
        $this->assertStringContainsString('OFFSET ?', $r3['query']);
    }

    public function testBuildBindingsNotAccumulated(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->limit(10);

        $builder->build();
        $builder->build();
        $builder->build();

        $this->assertCount(2, $builder->getBindings());
    }

    public function testMultipleBuildWithHavingBindings(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)]);

        $r1 = $builder->build();
        $r2 = $builder->build();

        $this->assertEquals([5], $r1['bindings']);
        $this->assertEquals([5], $r2['bindings']);
    }

    // ══════════════════════════════════════════
    //  15. Binding ordering comprehensive
    // ══════════════════════════════════════════

    public function testBindingOrderMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('a', ['v1']),
                Query::greaterThan('b', 10),
                Query::between('c', 1, 100),
            ])
            ->build();

        $this->assertEquals(['v1', 10, 1, 100], $result['bindings']);
    }

    public function testBindingOrderThreeProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['p1 = ?', ['pv1']])
            ->addConditionProvider(fn (string $t): array => ['p2 = ?', ['pv2']])
            ->addConditionProvider(fn (string $t): array => ['p3 = ?', ['pv3']])
            ->build();

        $this->assertEquals(['pv1', 'pv2', 'pv3'], $result['bindings']);
    }

    public function testBindingOrderMultipleUnions(): void
    {
        $q1 = (new Builder())->from('a')->filter([Query::equal('x', [1])]);
        $q2 = (new Builder())->from('b')->filter([Query::equal('y', [2])]);

        $result = (new Builder())
            ->from('main')
            ->filter([Query::equal('z', [3])])
            ->limit(5)
            ->union($q1)
            ->unionAll($q2)
            ->build();

        // main filter, main limit, union1 bindings, union2 bindings
        $this->assertEquals([3, 5, 1, 2], $result['bindings']);
    }

    public function testBindingOrderLogicalAndWithMultipleSubFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::equal('a', [1]),
                    Query::greaterThan('b', 2),
                    Query::lessThan('c', 3),
                ]),
            ])
            ->build();

        $this->assertEquals([1, 2, 3], $result['bindings']);
    }

    public function testBindingOrderLogicalOrWithMultipleSubFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::equal('a', [1]),
                    Query::equal('b', [2]),
                    Query::equal('c', [3]),
                ]),
            ])
            ->build();

        $this->assertEquals([1, 2, 3], $result['bindings']);
    }

    public function testBindingOrderNestedAndOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::and([
                    Query::equal('a', [1]),
                    Query::or([
                        Query::equal('b', [2]),
                        Query::equal('c', [3]),
                    ]),
                ]),
            ])
            ->build();

        $this->assertEquals([1, 2, 3], $result['bindings']);
    }

    public function testBindingOrderRawMixedWithRegularFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('a', ['v1']),
                Query::raw('custom > ?', [10]),
                Query::greaterThan('b', 20),
            ])
            ->build();

        $this->assertEquals(['v1', 10, 20], $result['bindings']);
    }

    public function testBindingOrderAggregationHavingComplexConditions(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->sum('price', 'total')
            ->filter([Query::equal('status', ['active'])])
            ->groupBy(['category'])
            ->having([
                Query::greaterThan('cnt', 5),
                Query::lessThan('total', 10000),
            ])
            ->limit(10)
            ->build();

        // filter, having1, having2, limit
        $this->assertEquals(['active', 5, 10000, 10], $result['bindings']);
    }

    public function testBindingOrderFullPipelineWithEverything(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('archived', [true])]);

        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->addConditionProvider(fn (string $t): array => ['tenant = ?', ['t1']])
            ->filter([
                Query::equal('status', ['paid']),
                Query::greaterThan('total', 0),
            ])
            ->cursorAfter('cursor_val')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->limit(25)
            ->offset(50)
            ->union($sub)
            ->build();

        // filter(paid, 0), provider(t1), cursor(cursor_val), having(1), limit(25), offset(50), union(true)
        $this->assertEquals(['paid', 0, 't1', 'cursor_val', 1, 25, 50, true], $result['bindings']);
    }

    public function testBindingOrderContainsMultipleValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::contains('bio', ['php', 'js', 'go']),
                Query::equal('status', ['active']),
            ])
            ->build();

        // contains produces three LIKE bindings, then equal
        $this->assertEquals(['%php%', '%js%', '%go%', 'active'], $result['bindings']);
    }

    public function testBindingOrderBetweenAndComparisons(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::between('age', 18, 65),
                Query::greaterThan('score', 50),
                Query::lessThan('rank', 100),
            ])
            ->build();

        $this->assertEquals([18, 65, 50, 100], $result['bindings']);
    }

    public function testBindingOrderStartsWithEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::startsWith('name', 'A'),
                Query::endsWith('email', '.com'),
            ])
            ->build();

        $this->assertEquals(['A%', '%.com'], $result['bindings']);
    }

    public function testBindingOrderSearchAndRegex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::search('content', 'hello'),
                Query::regex('slug', '^test'),
            ])
            ->build();

        $this->assertEquals(['hello', '^test'], $result['bindings']);
    }

    public function testBindingOrderWithCursorBeforeFilterAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $t): array => ['org = ?', ['org1']])
            ->filter([Query::equal('a', ['x'])])
            ->cursorBefore('my_cursor')
            ->limit(10)
            ->offset(0)
            ->build();

        // filter, provider, cursor, limit, offset
        $this->assertEquals(['x', 'org1', 'my_cursor', 10, 0], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  16. Empty/minimal queries
    // ══════════════════════════════════════════

    public function testBuildWithNoFromNoFilters(): void
    {
        $result = (new Builder())->from('')->build();
        $this->assertEquals('SELECT * FROM ``', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testBuildWithOnlyLimit(): void
    {
        $result = (new Builder())
            ->from('')
            ->limit(10)
            ->build();

        $this->assertStringContainsString('LIMIT ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testBuildWithOnlyOffset(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())
            ->from('')
            ->offset(50)
            ->build();

        $this->assertStringNotContainsString('OFFSET ?', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testBuildWithOnlySort(): void
    {
        $result = (new Builder())
            ->from('')
            ->sortAsc('name')
            ->build();

        $this->assertStringContainsString('ORDER BY `name` ASC', $result['query']);
    }

    public function testBuildWithOnlySelect(): void
    {
        $result = (new Builder())
            ->from('')
            ->select(['a', 'b'])
            ->build();

        $this->assertStringContainsString('SELECT `a`, `b`', $result['query']);
    }

    public function testBuildWithOnlyAggregationNoFrom(): void
    {
        $result = (new Builder())
            ->from('')
            ->count('*', 'total')
            ->build();

        $this->assertStringContainsString('COUNT(*) AS `total`', $result['query']);
    }

    public function testBuildWithEmptyFilterArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([])
            ->build();

        $this->assertEquals('SELECT * FROM `t`', $result['query']);
    }

    public function testBuildWithEmptySelectArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select([])
            ->build();

        $this->assertEquals('SELECT  FROM `t`', $result['query']);
    }

    public function testBuildWithOnlyHavingNoGroupBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->having([Query::greaterThan('cnt', 0)])
            ->build();

        $this->assertStringContainsString('HAVING `cnt` > ?', $result['query']);
        $this->assertStringNotContainsString('GROUP BY', $result['query']);
    }

    public function testBuildWithOnlyDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->build();

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result['query']);
    }

    // ══════════════════════════════════════════
    //  Spatial/Vector/ElemMatch Exception Tests
    // ══════════════════════════════════════════

    public function testUnsupportedFilterTypeCrosses(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::crosses('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeNotCrosses(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notCrosses('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeDistanceEqual(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceEqual('attr', [0, 0], 1)])->build();
    }

    public function testUnsupportedFilterTypeDistanceNotEqual(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceNotEqual('attr', [0, 0], 1)])->build();
    }

    public function testUnsupportedFilterTypeDistanceGreaterThan(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceGreaterThan('attr', [0, 0], 1)])->build();
    }

    public function testUnsupportedFilterTypeDistanceLessThan(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::distanceLessThan('attr', [0, 0], 1)])->build();
    }

    public function testUnsupportedFilterTypeIntersects(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::intersects('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeNotIntersects(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notIntersects('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeOverlaps(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::overlaps('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeNotOverlaps(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notOverlaps('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeTouches(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::touches('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeNotTouches(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::notTouches('attr', ['val'])])->build();
    }

    public function testUnsupportedFilterTypeVectorDot(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorDot('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeVectorCosine(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorCosine('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeVectorEuclidean(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::vectorEuclidean('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeElemMatch(): void
    {
        $this->expectException(\Utopia\Query\Exception::class);
        (new Builder())->from('t')->filter([Query::elemMatch('attr', [Query::equal('x', [1])])])->build();
    }

    // ══════════════════════════════════════════
    //  toRawSql Edge Cases
    // ══════════════════════════════════════════

    public function testToRawSqlWithBoolFalse(): void
    {
        $sql = (new Builder())->from('t')->filter([Query::equal('active', [false])])->toRawSql();
        $this->assertEquals("SELECT * FROM `t` WHERE `active` IN (0)", $sql);
    }

    public function testToRawSqlMixedBindingTypes(): void
    {
        $sql = (new Builder())->from('t')
            ->filter([
                Query::equal('name', ['str']),
                Query::greaterThan('age', 42),
                Query::lessThan('score', 9.99),
                Query::equal('active', [true]),
            ])->toRawSql();
        $this->assertStringContainsString("'str'", $sql);
        $this->assertStringContainsString('42', $sql);
        $this->assertStringContainsString('9.99', $sql);
        $this->assertStringContainsString('1', $sql);
    }

    public function testToRawSqlWithNull(): void
    {
        $sql = (new Builder())->from('t')
            ->filter([Query::raw('col = ?', [null])])
            ->toRawSql();
        $this->assertStringContainsString('NULL', $sql);
    }

    public function testToRawSqlWithUnion(): void
    {
        $other = (new Builder())->from('b')->filter([Query::equal('x', [1])]);
        $sql = (new Builder())->from('a')->filter([Query::equal('y', [2])])->union($other)->toRawSql();
        $this->assertStringContainsString("FROM `a`", $sql);
        $this->assertStringContainsString('UNION', $sql);
        $this->assertStringContainsString("FROM `b`", $sql);
        $this->assertStringContainsString('2', $sql);
        $this->assertStringContainsString('1', $sql);
    }

    public function testToRawSqlWithAggregationJoinGroupByHaving(): void
    {
        $sql = (new Builder())->from('orders')
            ->count('*', 'total')
            ->join('users', 'orders.uid', 'users.id')
            ->select(['users.country'])
            ->groupBy(['users.country'])
            ->having([Query::greaterThan('total', 5)])
            ->toRawSql();
        $this->assertStringContainsString('COUNT(*)', $sql);
        $this->assertStringContainsString('JOIN', $sql);
        $this->assertStringContainsString('GROUP BY', $sql);
        $this->assertStringContainsString('HAVING', $sql);
        $this->assertStringContainsString('5', $sql);
    }

    // ══════════════════════════════════════════
    //  Kitchen Sink Exact SQL
    // ══════════════════════════════════════════

    public function testKitchenSinkExactSql(): void
    {
        $other = (new Builder())->from('archive')->filter([Query::equal('status', ['closed'])]);
        $result = (new Builder())
            ->from('orders')
            ->distinct()
            ->count('*', 'total')
            ->select(['status'])
            ->join('users', 'orders.uid', 'users.id')
            ->filter([Query::greaterThan('amount', 100)])
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 5)])
            ->sortAsc('status')
            ->limit(10)
            ->offset(20)
            ->union($other)
            ->build();

        $this->assertEquals(
            'SELECT DISTINCT COUNT(*) AS `total`, `status` FROM `orders` JOIN `users` ON `orders`.`uid` = `users`.`id` WHERE `amount` > ? GROUP BY `status` HAVING `total` > ? ORDER BY `status` ASC LIMIT ? OFFSET ? UNION SELECT * FROM `archive` WHERE `status` IN (?)',
            $result['query']
        );
        $this->assertEquals([100, 5, 10, 20, 'closed'], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  Feature Combination Tests
    // ══════════════════════════════════════════

    public function testDistinctWithUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())->from('a')->distinct()->union($other)->build();
        $this->assertEquals('SELECT DISTINCT * FROM `a` UNION SELECT * FROM `b`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testRawInsideLogicalAnd(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::and([
                Query::greaterThan('x', 1),
                Query::raw('custom_func(y) > ?', [5]),
            ])])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`x` > ? AND custom_func(y) > ?)', $result['query']);
        $this->assertEquals([1, 5], $result['bindings']);
    }

    public function testRawInsideLogicalOr(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::or([
                Query::equal('a', [1]),
                Query::raw('b IS NOT NULL', []),
            ])])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR b IS NOT NULL)', $result['query']);
        $this->assertEquals([1], $result['bindings']);
    }

    public function testAggregationWithCursor(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->cursorAfter('abc')
            ->build();
        $this->assertStringContainsString('COUNT(*)', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        $this->assertContains('abc', $result['bindings']);
    }

    public function testGroupBySortCursorUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())->from('a')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->sortDesc('total')
            ->cursorAfter('xyz')
            ->union($other)
            ->build();
        $this->assertStringContainsString('GROUP BY', $result['query']);
        $this->assertStringContainsString('ORDER BY', $result['query']);
        $this->assertStringContainsString('UNION', $result['query']);
    }

    public function testConditionProviderWithNoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $table) => ["_tenant = ?", ['t1']])
            ->build();
        $this->assertEquals('SELECT * FROM `t` WHERE _tenant = ?', $result['query']);
        $this->assertEquals(['t1'], $result['bindings']);
    }

    public function testConditionProviderWithCursorNoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $table) => ["_tenant = ?", ['t1']])
            ->cursorAfter('abc')
            ->build();
        $this->assertStringContainsString('_tenant = ?', $result['query']);
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        // Provider bindings come before cursor bindings
        $this->assertEquals(['t1', 'abc'], $result['bindings']);
    }

    public function testConditionProviderWithDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->addConditionProvider(fn (string $table) => ["_tenant = ?", ['t1']])
            ->build();
        $this->assertEquals('SELECT DISTINCT * FROM `t` WHERE _tenant = ?', $result['query']);
        $this->assertEquals(['t1'], $result['bindings']);
    }

    public function testConditionProviderPersistsAfterReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addConditionProvider(fn (string $table) => ["_tenant = ?", ['t1']]);
        $builder->build();
        $builder->reset()->from('other');
        $result = $builder->build();
        $this->assertStringContainsString('FROM `other`', $result['query']);
        $this->assertStringContainsString('_tenant = ?', $result['query']);
        $this->assertEquals(['t1'], $result['bindings']);
    }

    public function testConditionProviderWithHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->addConditionProvider(fn (string $table) => ["_tenant = ?", ['t1']])
            ->having([Query::greaterThan('total', 5)])
            ->build();
        // Provider should be in WHERE, not HAVING
        $this->assertStringContainsString('WHERE _tenant = ?', $result['query']);
        $this->assertStringContainsString('HAVING `total` > ?', $result['query']);
        // Provider bindings before having bindings
        $this->assertEquals(['t1', 5], $result['bindings']);
    }

    public function testUnionWithConditionProvider(): void
    {
        $sub = (new Builder())
            ->from('b')
            ->addConditionProvider(fn (string $table) => ["_deleted = ?", [0]]);
        $result = (new Builder())
            ->from('a')
            ->union($sub)
            ->build();
        // Sub-query should include the condition provider
        $this->assertStringContainsString('UNION SELECT * FROM `b` WHERE _deleted = ?', $result['query']);
        $this->assertEquals([0], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  Boundary Value Tests
    // ══════════════════════════════════════════

    public function testNegativeLimit(): void
    {
        $result = (new Builder())->from('t')->limit(-1)->build();
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([-1], $result['bindings']);
    }

    public function testNegativeOffset(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(-5)->build();
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', [null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NULL', $result['query']);
        $this->assertSame([], $result['bindings']);
    }

    public function testEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', ['a', null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` IN (?) OR `col` IS NULL)', $result['query']);
        $this->assertSame(['a'], $result['bindings']);
    }

    public function testNotEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', [null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NOT NULL', $result['query']);
        $this->assertSame([], $result['bindings']);
    }

    public function testNotEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` != ? AND `col` IS NOT NULL)', $result['query']);
        $this->assertSame(['a'], $result['bindings']);
    }

    public function testNotEqualWithMultipleNonNullAndNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', 'b', null])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` NOT IN (?, ?) AND `col` IS NOT NULL)', $result['query']);
        $this->assertSame(['a', 'b'], $result['bindings']);
    }

    public function testBetweenReversedMinMax(): void
    {
        $result = (new Builder())->from('t')->filter([Query::between('age', 65, 18)])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result['query']);
        $this->assertEquals([65, 18], $result['bindings']);
    }

    public function testContainsWithSqlWildcard(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('bio', ['100%'])])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result['query']);
        $this->assertEquals(['%100\%%'], $result['bindings']);
    }

    public function testStartsWithWithWildcard(): void
    {
        $result = (new Builder())->from('t')->filter([Query::startsWith('name', '%admin')])->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result['query']);
        $this->assertEquals(['\%admin%'], $result['bindings']);
    }

    public function testCursorWithNullValue(): void
    {
        // Null cursor value is ignored by groupByType since cursor stays null
        $result = (new Builder())->from('t')->cursorAfter(null)->build();
        $this->assertStringNotContainsString('_cursor', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testCursorWithIntegerValue(): void
    {
        $result = (new Builder())->from('t')->cursorAfter(42)->build();
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        $this->assertSame([42], $result['bindings']);
    }

    public function testCursorWithFloatValue(): void
    {
        $result = (new Builder())->from('t')->cursorAfter(3.14)->build();
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        $this->assertSame([3.14], $result['bindings']);
    }

    public function testMultipleLimitsFirstWins(): void
    {
        $result = (new Builder())->from('t')->limit(10)->limit(20)->build();
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result['query']);
        $this->assertEquals([10], $result['bindings']);
    }

    public function testMultipleOffsetsFirstWins(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(5)->offset(50)->build();
        $this->assertEquals('SELECT * FROM `t`', $result['query']);
        $this->assertEquals([], $result['bindings']);
    }

    public function testCursorAfterAndBeforeFirstWins(): void
    {
        $result = (new Builder())->from('t')->cursorAfter('a')->cursorBefore('b')->build();
        $this->assertStringContainsString('`_cursor` > ?', $result['query']);
        $this->assertStringNotContainsString('`_cursor` < ?', $result['query']);
    }

    public function testEmptyTableWithJoin(): void
    {
        $result = (new Builder())->from('')->join('other', 'a', 'b')->build();
        $this->assertEquals('SELECT * FROM `` JOIN `other` ON `a` = `b`', $result['query']);
    }

    public function testBuildWithoutFromCall(): void
    {
        $result = (new Builder())->filter([Query::equal('x', [1])])->build();
        $this->assertStringContainsString('FROM ``', $result['query']);
        $this->assertStringContainsString('`x` IN (?)', $result['query']);
    }

    // ══════════════════════════════════════════
    //  Standalone Compiler Method Tests
    // ══════════════════════════════════════════

    public function testCompileSelectEmpty(): void
    {
        $builder = new Builder();
        $result = $builder->compileSelect(Query::select([]));
        $this->assertEquals('', $result);
    }

    public function testCompileGroupByEmpty(): void
    {
        $builder = new Builder();
        $result = $builder->compileGroupBy(Query::groupBy([]));
        $this->assertEquals('', $result);
    }

    public function testCompileGroupBySingleColumn(): void
    {
        $builder = new Builder();
        $result = $builder->compileGroupBy(Query::groupBy(['status']));
        $this->assertEquals('`status`', $result);
    }

    public function testCompileSumWithoutAlias(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::sum('price'));
        $this->assertEquals('SUM(`price`)', $sql);
    }

    public function testCompileAvgWithoutAlias(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::avg('score'));
        $this->assertEquals('AVG(`score`)', $sql);
    }

    public function testCompileMinWithoutAlias(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::min('price'));
        $this->assertEquals('MIN(`price`)', $sql);
    }

    public function testCompileMaxWithoutAlias(): void
    {
        $builder = new Builder();
        $sql = $builder->compileAggregate(Query::max('price'));
        $this->assertEquals('MAX(`price`)', $sql);
    }

    public function testCompileLimitZero(): void
    {
        $builder = new Builder();
        $sql = $builder->compileLimit(Query::limit(0));
        $this->assertEquals('LIMIT ?', $sql);
        $this->assertSame([0], $builder->getBindings());
    }

    public function testCompileOffsetZero(): void
    {
        $builder = new Builder();
        $sql = $builder->compileOffset(Query::offset(0));
        $this->assertEquals('OFFSET ?', $sql);
        $this->assertSame([0], $builder->getBindings());
    }

    public function testCompileOrderException(): void
    {
        $builder = new Builder();
        $this->expectException(\Utopia\Query\Exception::class);
        $builder->compileOrder(Query::limit(10));
    }

    public function testCompileJoinException(): void
    {
        $builder = new Builder();
        $this->expectException(\Utopia\Query\Exception::class);
        $builder->compileJoin(Query::equal('x', [1]));
    }

    // ══════════════════════════════════════════
    //  Query::compile() Integration Tests
    // ══════════════════════════════════════════

    public function testQueryCompileOrderAsc(): void
    {
        $builder = new Builder();
        $this->assertEquals('`name` ASC', Query::orderAsc('name')->compile($builder));
    }

    public function testQueryCompileOrderDesc(): void
    {
        $builder = new Builder();
        $this->assertEquals('`name` DESC', Query::orderDesc('name')->compile($builder));
    }

    public function testQueryCompileOrderRandom(): void
    {
        $builder = new Builder();
        $this->assertEquals('RAND()', Query::orderRandom()->compile($builder));
    }

    public function testQueryCompileLimit(): void
    {
        $builder = new Builder();
        $this->assertEquals('LIMIT ?', Query::limit(10)->compile($builder));
        $this->assertEquals([10], $builder->getBindings());
    }

    public function testQueryCompileOffset(): void
    {
        $builder = new Builder();
        $this->assertEquals('OFFSET ?', Query::offset(5)->compile($builder));
        $this->assertEquals([5], $builder->getBindings());
    }

    public function testQueryCompileCursorAfter(): void
    {
        $builder = new Builder();
        $this->assertEquals('`_cursor` > ?', Query::cursorAfter('x')->compile($builder));
        $this->assertEquals(['x'], $builder->getBindings());
    }

    public function testQueryCompileCursorBefore(): void
    {
        $builder = new Builder();
        $this->assertEquals('`_cursor` < ?', Query::cursorBefore('x')->compile($builder));
        $this->assertEquals(['x'], $builder->getBindings());
    }

    public function testQueryCompileSelect(): void
    {
        $builder = new Builder();
        $this->assertEquals('`a`, `b`', Query::select(['a', 'b'])->compile($builder));
    }

    public function testQueryCompileGroupBy(): void
    {
        $builder = new Builder();
        $this->assertEquals('`status`', Query::groupBy(['status'])->compile($builder));
    }

    // ══════════════════════════════════════════
    //  setWrapChar Edge Cases
    // ══════════════════════════════════════════

    public function testSetWrapCharWithIsNotNull(): void
    {
        $result = (new Builder())->setWrapChar('"')
            ->from('t')
            ->filter([Query::isNotNull('email')])
            ->build();
        $this->assertStringContainsString('"email" IS NOT NULL', $result['query']);
    }

    public function testSetWrapCharWithExists(): void
    {
        $result = (new Builder())->setWrapChar('"')
            ->from('t')
            ->filter([Query::exists(['a', 'b'])])
            ->build();
        $this->assertStringContainsString('"a" IS NOT NULL', $result['query']);
        $this->assertStringContainsString('"b" IS NOT NULL', $result['query']);
    }

    public function testSetWrapCharWithNotExists(): void
    {
        $result = (new Builder())->setWrapChar('"')
            ->from('t')
            ->filter([Query::notExists('c')])
            ->build();
        $this->assertStringContainsString('"c" IS NULL', $result['query']);
    }

    public function testSetWrapCharCursorNotAffected(): void
    {
        $result = (new Builder())->setWrapChar('"')
            ->from('t')
            ->cursorAfter('abc')
            ->build();
        // _cursor is now properly wrapped with the configured wrap character
        $this->assertStringContainsString('"_cursor" > ?', $result['query']);
    }

    public function testSetWrapCharWithToRawSql(): void
    {
        $sql = (new Builder())->setWrapChar('"')
            ->from('t')
            ->filter([Query::equal('name', ['test'])])
            ->limit(5)
            ->toRawSql();
        $this->assertStringContainsString('"t"', $sql);
        $this->assertStringContainsString('"name"', $sql);
        $this->assertStringContainsString("'test'", $sql);
        $this->assertStringContainsString('5', $sql);
    }

    // ══════════════════════════════════════════
    //  Reset Behavior
    // ══════════════════════════════════════════

    public function testResetFollowedByUnion(): void
    {
        $builder = (new Builder())
            ->from('a')
            ->union((new Builder())->from('old'));
        $builder->reset()->from('b');
        $result = $builder->build();
        $this->assertEquals('SELECT * FROM `b`', $result['query']);
        $this->assertStringNotContainsString('UNION', $result['query']);
    }

    public function testResetClearsBindingsAfterBuild(): void
    {
        $builder = (new Builder())->from('t')->filter([Query::equal('x', [1])]);
        $builder->build();
        $this->assertNotEmpty($builder->getBindings());
        $builder->reset()->from('t');
        $result = $builder->build();
        $this->assertEquals([], $result['bindings']);
    }

    // ══════════════════════════════════════════
    //  Missing Binding Assertions
    // ══════════════════════════════════════════

    public function testSortAscBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortAsc('name')->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testSortDescBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortDesc('name')->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testSortRandomBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortRandom()->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testDistinctBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->distinct()->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testJoinBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->join('other', 'a', 'b')->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testCrossJoinBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->crossJoin('other')->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testGroupByBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->groupBy(['status'])->build();
        $this->assertEquals([], $result['bindings']);
    }

    public function testCountWithAliasBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->count('*', 'total')->build();
        $this->assertEquals([], $result['bindings']);
    }
}
