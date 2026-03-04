<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Query;

class BuilderTest extends TestCase
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

        $this->assertEquals('SELECT * FROM `t` WHERE NOT MATCH(`content`) AGAINST(?)', $result['query']);
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
        $result = (new Builder())
            ->from('t')
            ->offset(50)
            ->build();

        $this->assertEquals('SELECT * FROM `t` OFFSET ?', $result['query']);
        $this->assertEquals([50], $result['bindings']);
    }

    public function testCursorAfter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc123')
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE _cursor > ?', $result['query']);
        $this->assertEquals(['abc123'], $result['bindings']);
    }

    public function testCursorBefore(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorBefore('xyz789')
            ->build();

        $this->assertEquals('SELECT * FROM `t` WHERE _cursor < ?', $result['query']);
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
            'SELECT * FROM `users` JOIN `orders` ON `users.id` = `orders.user_id`',
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
            'SELECT * FROM `users` LEFT JOIN `profiles` ON `users.id` = `profiles.user_id`',
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
            'SELECT * FROM `users` RIGHT JOIN `orders` ON `users.id` = `orders.user_id`',
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
            'SELECT * FROM `users` JOIN `orders` ON `users.id` = `orders.user_id` WHERE `orders.total` > ?',
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
            'SELECT COUNT(*) AS `order_count`, SUM(`total`) AS `total_amount`, `users.name` FROM `orders` JOIN `users` ON `orders.user_id` = `users.id` GROUP BY `users.name` HAVING `order_count` > ? ORDER BY `total_amount` DESC LIMIT ?',
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
}
