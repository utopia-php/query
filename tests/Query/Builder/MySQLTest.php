<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\Case\Builder as CaseBuilder;
use Utopia\Query\Builder\Case\Expression;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\Feature\Aggregates;
use Utopia\Query\Builder\Feature\CTEs;
use Utopia\Query\Builder\Feature\Deletes;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\Hooks;
use Utopia\Query\Builder\Feature\Inserts;
use Utopia\Query\Builder\Feature\Joins;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\Locking;
use Utopia\Query\Builder\Feature\Selects;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Builder\Feature\Transactions;
use Utopia\Query\Builder\Feature\Unions;
use Utopia\Query\Builder\Feature\Updates;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Builder\Feature\VectorSearch;
use Utopia\Query\Builder\Feature\Windows;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Builder\MySQL as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook;
use Utopia\Query\Hook\Attribute;
use Utopia\Query\Hook\Attribute\Map as AttributeMap;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Method;
use Utopia\Query\Query;

class MySQLTest extends TestCase
{
    use AssertsBindingCount;
    public function testImplementsCompiler(): void
    {
        $builder = new Builder();
        $this->assertInstanceOf(Compiler::class, $builder);
    }

    public function testImplementsTransactions(): void
    {
        $this->assertInstanceOf(Transactions::class, new Builder());
    }

    public function testImplementsLocking(): void
    {
        $this->assertInstanceOf(Locking::class, new Builder());
    }

    public function testImplementsUpsert(): void
    {
        $this->assertInstanceOf(Upsert::class, new Builder());
    }

    public function testImplementsSelects(): void
    {
        $this->assertInstanceOf(Selects::class, new Builder());
    }

    public function testImplementsAggregates(): void
    {
        $this->assertInstanceOf(Aggregates::class, new Builder());
    }

    public function testImplementsJoins(): void
    {
        $this->assertInstanceOf(Joins::class, new Builder());
    }

    public function testImplementsUnions(): void
    {
        $this->assertInstanceOf(Unions::class, new Builder());
    }

    public function testImplementsCTEs(): void
    {
        $this->assertInstanceOf(CTEs::class, new Builder());
    }

    public function testImplementsInserts(): void
    {
        $this->assertInstanceOf(Inserts::class, new Builder());
    }

    public function testImplementsUpdates(): void
    {
        $this->assertInstanceOf(Updates::class, new Builder());
    }

    public function testImplementsDeletes(): void
    {
        $this->assertInstanceOf(Deletes::class, new Builder());
    }

    public function testImplementsHooks(): void
    {
        $this->assertInstanceOf(Hooks::class, new Builder());
    }

    public function testStandaloneCompile(): void
    {
        $builder = new Builder();

        $filter = Query::greaterThan('age', 18);
        $sql = $filter->compile($builder);
        $this->assertEquals('`age` > ?', $sql);
        $this->assertEquals([18], $builder->getBindings());
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['active', 18, 25, 0], $result->bindings);
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `name`, `email` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['active', 18, 25, 0], $result->bindings);
    }

    public function testEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active', 'pending'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?, ?)', $result->query);
        $this->assertEquals(['active', 'pending'], $result->bindings);
    }

    public function testNotEqualSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', 'guest')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `role` != ?', $result->query);
        $this->assertEquals(['guest'], $result->bindings);
    }

    public function testNotEqualMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', ['guest', 'banned'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `role` NOT IN (?, ?)', $result->query);
        $this->assertEquals(['guest', 'banned'], $result->bindings);
    }

    public function testLessThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('price', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `price` < ?', $result->query);
        $this->assertEquals([100], $result->bindings);
    }

    public function testLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThanEqual('price', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `price` <= ?', $result->query);
        $this->assertEquals([100], $result->bindings);
    }

    public function testGreaterThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `age` > ?', $result->query);
        $this->assertEquals([18], $result->bindings);
    }

    public function testGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 90)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `score` >= ?', $result->query);
        $this->assertEquals([90], $result->bindings);
    }

    public function testBetween(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testNotBetween(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notBetween('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `age` NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', 'Jo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result->query);
        $this->assertEquals(['Jo%'], $result->bindings);
    }

    public function testNotStartsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notStartsWith('name', 'Jo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` NOT LIKE ?', $result->query);
        $this->assertEquals(['Jo%'], $result->bindings);
    }

    public function testEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('email', '.com')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `email` LIKE ?', $result->query);
        $this->assertEquals(['%.com'], $result->bindings);
    }

    public function testNotEndsWith(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEndsWith('email', '.com')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `email` NOT LIKE ?', $result->query);
        $this->assertEquals(['%.com'], $result->bindings);
    }

    public function testContainsSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result->query);
        $this->assertEquals(['%php%'], $result->bindings);
    }

    public function testContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php', 'js'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`bio` LIKE ? OR `bio` LIKE ?)', $result->query);
        $this->assertEquals(['%php%', '%js%'], $result->bindings);
    }

    public function testContainsAny(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAny('tags', ['a', 'b'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `tags` IN (?, ?)', $result->query);
        $this->assertEquals(['a', 'b'], $result->bindings);
    }

    public function testContainsAll(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('perms', ['read', 'write'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`perms` LIKE ? AND `perms` LIKE ?)', $result->query);
        $this->assertEquals(['%read%', '%write%'], $result->bindings);
    }

    public function testNotContainsSingle(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['php'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` NOT LIKE ?', $result->query);
        $this->assertEquals(['%php%'], $result->bindings);
    }

    public function testNotContainsMultiple(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['php', 'js'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`bio` NOT LIKE ? AND `bio` NOT LIKE ?)', $result->query);
        $this->assertEquals(['%php%', '%js%'], $result->bindings);
    }

    public function testSearch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', 'hello')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?)', $result->query);
        $this->assertEquals(['hello'], $result->bindings);
    }

    public function testNotSearch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('content', 'hello')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?))', $result->query);
        $this->assertEquals(['hello'], $result->bindings);
    }

    public function testRegex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^[a-z]+$')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `slug` REGEXP ?', $result->query);
        $this->assertEquals(['^[a-z]+$'], $result->bindings);
    }

    public function testIsNull(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `deleted` IS NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testIsNotNull(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNotNull('verified')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `verified` IS NOT NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testExists(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name', 'email'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`name` IS NOT NULL AND `email` IS NOT NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testNotExists(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['legacy'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`legacy` IS NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`age` > ? AND `status` IN (?))', $result->query);
        $this->assertEquals([18, 'active'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`role` IN (?) OR `role` IN (?))', $result->query);
        $this->assertEquals(['admin', 'mod'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`age` > ? AND (`role` IN (?) OR `role` IN (?)))',
            $result->query
        );
        $this->assertEquals([18, 'admin', 'mod'], $result->bindings);
    }

    public function testSortAsc(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY `name` ASC', $result->query);
    }

    public function testSortDesc(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortDesc('score')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY `score` DESC', $result->query);
    }

    public function testSortRandom(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND()', $result->query);
    }

    public function testMultipleSorts(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY `name` ASC, `age` DESC', $result->query);
    }

    public function testLimitOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testOffsetOnly(): void
    {
        // OFFSET without LIMIT is invalid in MySQL/ClickHouse, so offset is suppressed
        $result = (new Builder())
            ->from('t')
            ->offset(50)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testCursorAfter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc123')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `_cursor` > ?', $result->query);
        $this->assertEquals(['abc123'], $result->bindings);
    }

    public function testCursorBefore(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorBefore('xyz789')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `_cursor` < ?', $result->query);
        $this->assertEquals(['xyz789'], $result->bindings);
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `id`, `name` FROM `users` WHERE `status` IN (?) AND `age` > ? ORDER BY `name` ASC, `age` DESC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['active', 18, 25, 10], $result->bindings);
    }

    public function testMultipleFilterCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->filter([Query::equal('b', [2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?) AND `b` IN (?)', $result->query);
        $this->assertEquals([1, 2], $result->bindings);
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `orders` WHERE `total` > ?', $result->query);
        $this->assertEquals([100], $result->bindings);
    }

    public function testAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('users')
            ->addHook(new AttributeMap([
                '$id' => '_uid',
                '$createdAt' => '_createdAt',
            ]))
            ->filter([Query::equal('$id', ['abc'])])
            ->sortAsc('$createdAt')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `_uid` IN (?) ORDER BY `_createdAt` ASC',
            $result->query
        );
        $this->assertEquals(['abc'], $result->bindings);
    }

    public function testMultipleAttributeHooksChain(): void
    {
        $prefixHook = new class () implements Attribute {
            public function resolve(string $attribute): string
            {
                return 'col_' . $attribute;
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap(['name' => 'full_name']))
            ->addHook($prefixHook)
            ->filter([Query::equal('name', ['Alice'])])
            ->build();
        $this->assertBindingCount($result);

        // First hook maps name→full_name, second prepends col_
        $this->assertEquals(
            'SELECT * FROM `t` WHERE `col_full_name` IN (?)',
            $result->query
        );
    }

    public function testDualInterfaceHook(): void
    {
        $hook = new class () implements Filter, Attribute {
            public function filter(string $table): Condition
            {
                return new Condition('_tenant = ?', ['t1']);
            }

            public function resolve(string $attribute): string
            {
                return match ($attribute) {
                    '$id' => '_uid',
                    default => $attribute,
                };
            }
        };

        $result = (new Builder())
            ->from('users')
            ->addHook($hook)
            ->filter([Query::equal('$id', ['abc'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `_uid` IN (?) AND _tenant = ?',
            $result->query
        );
        $this->assertEquals(['abc', 't1'], $result->bindings);
    }

    public function testConditionProvider(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition(
                    "_uid IN (SELECT _document FROM {$table}_perms WHERE _type = 'read')",
                );
            }
        };

        $result = (new Builder())
            ->from('users')
            ->addHook($hook)
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            "SELECT * FROM `users` WHERE `status` IN (?) AND _uid IN (SELECT _document FROM users_perms WHERE _type = 'read')",
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testConditionProviderWithBindings(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('_tenant = ?', ['tenant_abc']);
            }
        };

        $result = (new Builder())
            ->from('docs')
            ->addHook($hook)
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `docs` WHERE `status` IN (?) AND _tenant = ?',
            $result->query
        );
        // filter bindings first, then hook bindings
        $this->assertEquals(['active', 'tenant_abc'], $result->bindings);
    }

    public function testBindingOrderingWithProviderAndCursor(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('_tenant = ?', ['t1']);
            }
        };

        $result = (new Builder())
            ->from('docs')
            ->addHook($hook)
            ->filter([Query::equal('status', ['active'])])
            ->cursorAfter('cursor_val')
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        // binding order: filter, hook, cursor, limit, offset
        $this->assertEquals(['active', 't1', 'cursor_val', 10, 5], $result->bindings);
    }

    public function testDefaultSelectStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testCountStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testCountWithAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) AS `total` FROM `t`', $result->query);
    }

    public function testSumColumn(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sum('price', 'total_price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`price`) AS `total_price` FROM `orders`', $result->query);
    }

    public function testAvgColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->avg('score')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT AVG(`score`) FROM `t`', $result->query);
    }

    public function testMinColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->min('price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT MIN(`price`) FROM `t`', $result->query);
    }

    public function testMaxColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->max('price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT MAX(`price`) FROM `t`', $result->query);
    }

    public function testAggregationWithSelection(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->select(['status'])
            ->groupBy(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, `status` FROM `orders` GROUP BY `status`',
            $result->query
        );
    }

    public function testGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status`',
            $result->query
        );
    }

    public function testGroupByMultiple(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status', 'country'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status`, `country`',
            $result->query
        );
    }

    public function testHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([Query::greaterThan('total', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` GROUP BY `status` HAVING `total` > ?',
            $result->query
        );
        $this->assertEquals([5], $result->bindings);
    }

    public function testDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT `status` FROM `t`', $result->query);
    }

    public function testDistinctStar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result->query);
    }

    public function testJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result->query
        );
    }

    public function testLeftJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id`',
            $result->query
        );
    }

    public function testRightJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->rightJoin('orders', 'users.id', 'orders.user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` RIGHT JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result->query
        );
    }

    public function testCrossJoin(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `sizes` CROSS JOIN `colors`',
            $result->query
        );
    }

    public function testJoinWithFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->filter([Query::greaterThan('orders.total', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ?',
            $result->query
        );
        $this->assertEquals([100], $result->bindings);
    }

    public function testRawFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('score > ? AND score < ?', [10, 100])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE score > ? AND score < ?', $result->query);
        $this->assertEquals([10, 100], $result->bindings);
    }

    public function testRawFilterNoBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('1 = 1')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 1', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testUnion(): void
    {
        $admins = (new Builder())->from('admins')->filter([Query::equal('role', ['admin'])]);
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->union($admins)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users` WHERE `status` IN (?)) UNION (SELECT * FROM `admins` WHERE `role` IN (?))',
            $result->query
        );
        $this->assertEquals(['active', 'admin'], $result->bindings);
    }

    public function testUnionAll(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('current')
            ->unionAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `current`) UNION ALL (SELECT * FROM `archive`)',
            $result->query
        );
    }

    public function testWhenTrue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?)', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testWhenFalse(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('status', ['active'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(3, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([10, 20], $result->bindings);
    }

    public function testPageDefaultPerPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([25, 0], $result->bindings);
    }

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `order_count`, SUM(`total`) AS `total_amount`, `users`.`name` FROM `orders` JOIN `users` ON `orders`.`user_id` = `users`.`id` GROUP BY `users`.`name` HAVING `order_count` > ? ORDER BY `total_amount` DESC LIMIT ?',
            $result->query
        );
        $this->assertEquals([5, 10], $result->bindings);
    }

    public function testResetClearsUnions(): void
    {
        $other = (new Builder())->from('archive');
        $builder = (new Builder())
            ->from('current')
            ->union($other);

        $builder->build();
        $builder->reset();

        $result = $builder->from('fresh')->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `fresh`', $result->query);
    }
    //  EDGE CASES & COMBINATIONS


    public function testCountWithNamedColumn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(`id`) FROM `t`', $result->query);
    }

    public function testCountWithEmptyStringAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `cnt`, SUM(`price`) AS `total`, AVG(`score`) AS `avg_score`, MIN(`age`) AS `youngest`, MAX(`age`) AS `oldest` FROM `t`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testAggregationWithoutGroupBy(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sum('total', 'grand_total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`total`) AS `grand_total` FROM `orders`', $result->query);
    }

    public function testAggregationWithFilter(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->filter([Query::equal('status', ['completed'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `orders` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['completed'], $result->bindings);
    }

    public function testAggregationWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->sum('price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*), SUM(`price`) FROM `t`', $result->query);
    }

    public function testGroupByEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->groupBy([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testMultipleGroupByCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->groupBy(['country'])
            ->build();
        $this->assertBindingCount($result);

        // Both groupBy calls should merge since groupByType merges values
        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('`status`', $result->query);
        $this->assertStringContainsString('`country`', $result->query);
    }

    public function testHavingEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->having([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('HAVING', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total`, SUM(`price`) AS `sum_price` FROM `t` GROUP BY `status` HAVING `total` > ? AND `sum_price` < ?',
            $result->query
        );
        $this->assertEquals([5, 1000], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING (`total` > ? OR `total` < ?)', $result->query);
        $this->assertEquals([10, 2], $result->bindings);
    }

    public function testHavingWithoutGroupBy(): void
    {
        // SQL allows HAVING without GROUP BY in some engines
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->having([Query::greaterThan('total', 0)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING', $result->query);
        $this->assertStringNotContainsString('GROUP BY', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING `total` > ? AND `total` < ?', $result->query);
        $this->assertEquals([1, 100], $result->bindings);
    }

    public function testDistinctWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->count('*', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT COUNT(*) AS `total` FROM `t`', $result->query);
    }

    public function testDistinctMultipleCalls(): void
    {
        // Multiple distinct() calls should still produce single DISTINCT keyword
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result->query);
    }

    public function testDistinctWithJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->distinct()
            ->select(['users.name'])
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT DISTINCT `users`.`name` FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT DISTINCT `status` FROM `t` WHERE `status` IS NOT NULL ORDER BY `status` ASC',
            $result->query
        );
    }

    public function testMultipleJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->rightJoin('departments', 'users.dept_id', 'departments.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` LEFT JOIN `profiles` ON `users`.`id` = `profiles`.`user_id` RIGHT JOIN `departments` ON `users`.`dept_id` = `departments`.`id`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `order_count` FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` GROUP BY `users`.`name`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ? ORDER BY `orders`.`total` DESC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals([50, 10, 20], $result->bindings);
    }

    public function testJoinWithCustomOperator(): void
    {
        $result = (new Builder())
            ->from('a')
            ->join('b', 'a.val', 'b.val', '!=')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `a` JOIN `b` ON `a`.`val` != `b`.`val`',
            $result->query
        );
    }

    public function testCrossJoinWithOtherJoins(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->leftJoin('inventory', 'sizes.id', 'inventory.size_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `sizes` CROSS JOIN `colors` LEFT JOIN `inventory` ON `sizes`.`id` = `inventory`.`size_id`',
            $result->query
        );
    }

    public function testRawWithMixedBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('a = ? AND b = ? AND c = ?', ['str', 42, 3.14])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE a = ? AND b = ? AND c = ?', $result->query);
        $this->assertEquals(['str', 42, 3.14], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) AND custom_func(col) > ?',
            $result->query
        );
        $this->assertEquals(['active', 10], $result->bindings);
    }

    public function testRawWithEmptySql(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('')])
            ->build();
        $this->assertBindingCount($result);

        // Empty raw SQL still appears as a WHERE clause
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testMultipleUnions(): void
    {
        $q1 = (new Builder())->from('admins');
        $q2 = (new Builder())->from('mods');

        $result = (new Builder())
            ->from('users')
            ->union($q1)
            ->union($q2)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) UNION (SELECT * FROM `admins`) UNION (SELECT * FROM `mods`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) UNION (SELECT * FROM `admins`) UNION ALL (SELECT * FROM `mods`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users` WHERE `status` IN (?)) UNION (SELECT * FROM `admins` WHERE `level` IN (?)) UNION ALL (SELECT * FROM `mods` WHERE `score` > ?)',
            $result->query
        );
        $this->assertEquals(['active', 1, 50], $result->bindings);
    }

    public function testUnionWithAggregation(): void
    {
        $q1 = (new Builder())->from('orders_2023')->count('*', 'total');

        $result = (new Builder())
            ->from('orders_2024')
            ->count('*', 'total')
            ->unionAll($q1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT COUNT(*) AS `total` FROM `orders_2024`) UNION ALL (SELECT COUNT(*) AS `total` FROM `orders_2023`)',
            $result->query
        );
    }

    public function testWhenNested(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, function (Builder $b) {
                $b->when(true, fn (Builder $b2) => $b2->filter([Query::equal('a', [1])]));
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $result->query);
    }

    public function testWhenMultipleCalls(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('a', [1])]))
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('b', [2])]))
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('c', [3])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?) AND `c` IN (?)', $result->query);
        $this->assertEquals([1, 3], $result->bindings);
    }

    public function testPageZero(): void
    {
        $this->expectException(ValidationException::class);
        (new Builder())
            ->from('t')
            ->page(0, 10)
            ->build();
    }

    public function testPageOnePerPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(5, 1)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([1, 4], $result->bindings);
    }

    public function testPageLargeValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(1000, 100)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([100, 99900], $result->bindings);
    }

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

    public function testCompileFilterUnsupportedType(): void
    {
        $this->expectException(\ValueError::class);
        new Query('totallyInvalid', 'x', [1]);
    }

    public function testCompileOrderUnsupportedType(): void
    {
        $builder = new Builder();
        $query = new Query('equal', 'x', [1]);

        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Unsupported order type: equal');
        $builder->compileOrder($query);
    }

    public function testCompileJoinUnsupportedType(): void
    {
        $builder = new Builder();
        $query = new Query('equal', 't', ['a', '=', 'b']);

        $this->expectException(UnsupportedException::class);
        $this->expectExceptionMessage('Unsupported join type: equal');
        $builder->compileJoin($query);
    }

    public function testBindingOrderFilterProviderCursorLimitOffset(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('_tenant = ?', ['tenant1']);
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook)
            ->filter([
                Query::equal('a', ['x']),
                Query::greaterThan('b', 5),
            ])
            ->cursorAfter('cursor_abc')
            ->limit(10)
            ->offset(20)
            ->build();
        $this->assertBindingCount($result);

        // Order: filter bindings, hook bindings, cursor, limit, offset
        $this->assertEquals(['x', 5, 'tenant1', 'cursor_abc', 10, 20], $result->bindings);
    }

    public function testBindingOrderMultipleProviders(): void
    {
        $hook1 = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('p1 = ?', ['v1']);
            }
        };
        $hook2 = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('p2 = ?', ['v2']);
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook1)
            ->addHook($hook2)
            ->filter([Query::equal('a', ['x'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['x', 'v1', 'v2'], $result->bindings);
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
        $this->assertBindingCount($result);

        // Filter bindings, then having bindings, then limit
        $this->assertEquals(['active', 5, 10], $result->bindings);
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
        $this->assertBindingCount($result);

        // Main filter, main limit, then union bindings
        $this->assertEquals(['b', 5, 'y'], $result->bindings);
    }

    public function testBindingOrderComplexMixed(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('year', [2023])]);

        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('_org = ?', ['org1']);
            }
        };

        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->addHook($hook)
            ->filter([Query::equal('status', ['paid'])])
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->cursorAfter('cur1')
            ->limit(10)
            ->offset(5)
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);

        // filter, hook, cursor, having, limit, offset, union
        $this->assertEquals(['paid', 'org1', 'cur1', 1, 10, 5, 2023], $result->bindings);
    }

    public function testAttributeResolverWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap(['$price' => '_price']))
            ->sum('$price', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`_price`) AS `total` FROM `t`', $result->query);
    }

    public function testAttributeResolverWithGroupBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap(['$status' => '_status']))
            ->count('*', 'total')
            ->groupBy(['$status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS `total` FROM `t` GROUP BY `_status`',
            $result->query
        );
    }

    public function testAttributeResolverWithJoin(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap([
                '$id' => '_uid',
                '$ref' => '_ref',
            ]))
            ->join('other', '$id', '$ref')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` JOIN `other` ON `_uid` = `_ref`',
            $result->query
        );
    }

    public function testAttributeResolverWithHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap(['$total' => '_total']))
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('$total', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING `_total` > ?', $result->query);
    }

    public function testConditionProviderWithJoins(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('users.org_id = ?', ['org1']);
            }
        };

        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->addHook($hook)
            ->filter([Query::greaterThan('orders.total', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ? AND users.org_id = ?',
            $result->query
        );
        $this->assertEquals([100, 'org1'], $result->bindings);
    }

    public function testConditionProviderWithAggregation(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('org_id = ?', ['org1']);
            }
        };

        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->addHook($hook)
            ->groupBy(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE org_id = ?', $result->query);
        $this->assertEquals(['org1'], $result->bindings);
    }

    public function testMultipleBuildsConsistentOutput(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->limit(10);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1->query, $result2->query);
        $this->assertEquals($result1->bindings, $result2->bindings);
    }


    public function testEmptyBuilderNoFrom(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())->from('')->build();
    }

    public function testCursorWithLimitAndOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc')
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `_cursor` > ? LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['abc', 10, 5], $result->bindings);
    }

    public function testCursorWithPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->cursorAfter('abc')
            ->page(2, 10)
            ->build();
        $this->assertBindingCount($result);

        // Cursor + limit from page + offset from page; first limit/offset wins
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

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
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['o1']);
                }
            })
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 1)])
            ->sortDesc('sum_total')
            ->limit(25)
            ->offset(50)
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);

        // Verify structural elements
        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('SUM(`total`) AS `sum_total`', $result->query);
        $this->assertStringContainsString('`status`', $result->query);
        $this->assertStringContainsString('FROM `orders`', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `coupons`', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('GROUP BY `status`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
        $this->assertStringContainsString('ORDER BY `sum_total` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertStringContainsString('UNION', $result->query);

        // Verify SQL clause ordering
        $query = $result->query;
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

    public function testFilterEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testSelectEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select([])
            ->build();
        $this->assertBindingCount($result);

        // Empty select produces empty column list
        $this->assertEquals('SELECT  FROM `t`', $result->query);
    }

    public function testLimitZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([0], $result->bindings);
    }

    public function testOffsetZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->offset(0)
            ->build();
        $this->assertBindingCount($result);

        // OFFSET without LIMIT is suppressed
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

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
    //  1. SQL-Specific: REGEXP

    public function testRegexWithEmptyPattern(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `slug` REGEXP ?', $result->query);
        $this->assertEquals([''], $result->bindings);
    }

    public function testRegexWithDotChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a.b')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` REGEXP ?', $result->query);
        $this->assertEquals(['a.b'], $result->bindings);
    }

    public function testRegexWithStarChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a*b')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['a*b'], $result->bindings);
    }

    public function testRegexWithPlusChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'a+')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['a+'], $result->bindings);
    }

    public function testRegexWithQuestionMarkChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('name', 'colou?r')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['colou?r'], $result->bindings);
    }

    public function testRegexWithCaretAndDollar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('code', '^[A-Z]+$')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['^[A-Z]+$'], $result->bindings);
    }

    public function testRegexWithPipeChar(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('color', 'red|blue|green')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['red|blue|green'], $result->bindings);
    }

    public function testRegexWithBackslash(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('path', '\\\\server\\\\share')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['\\\\server\\\\share'], $result->bindings);
    }

    public function testRegexWithBracketsAndBraces(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('zip', '[0-9]{5}')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('[0-9]{5}', $result->bindings[0]);
    }

    public function testRegexWithParentheses(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('phone', '(\\+1)?[0-9]{10}')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['(\\+1)?[0-9]{10}'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) AND `slug` REGEXP ? AND `age` > ?',
            $result->query
        );
        $this->assertEquals(['active', '^[a-z-]+$', 18], $result->bindings);
    }

    public function testRegexWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap([
                '$slug' => '_slug',
            ]))
            ->filter([Query::regex('$slug', '^test')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `_slug` REGEXP ?', $result->query);
        $this->assertEquals(['^test'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertSame($pattern, $result->bindings[0]);
    }

    public function testRegexWithVeryLongPattern(): void
    {
        $pattern = str_repeat('[a-z]', 500);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('col', $pattern)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals($pattern, $result->bindings[0]);
        $this->assertStringContainsString('REGEXP ?', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `name` REGEXP ? AND `email` REGEXP ?',
            $result->query
        );
        $this->assertEquals(['^A', '@test\\.com$'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`slug` REGEXP ? AND `status` IN (?))',
            $result->query
        );
        $this->assertEquals(['^[a-z]+$', 'active'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`name` REGEXP ? OR `name` REGEXP ?)',
            $result->query
        );
        $this->assertEquals(['^Admin', '^Mod'], $result->bindings);
    }
    //  2. SQL-Specific: MATCH AGAINST / Search

    public function testSearchWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', '')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?)', $result->query);
        $this->assertEquals([''], $result->bindings);
    }

    public function testSearchWithSpecialCharacters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('body', 'hello "world" +required -excluded')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['hello "world" +required -excluded'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?) AND `status` IN (?) AND `views` > ?',
            $result->query
        );
        $this->assertEquals(['hello', 'published', 100], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?)) AND `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['spam', 'published'], $result->bindings);
    }

    public function testSearchWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap([
                '$body' => '_body',
            ]))
            ->filter([Query::search('$body', 'hello')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`_body`) AGAINST(?)', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertSame($searchTerm, $result->bindings[0]);
    }

    public function testSearchWithVeryLongText(): void
    {
        $longText = str_repeat('keyword ', 1000);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', $longText)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals($longText, $result->bindings[0]);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`title`) AGAINST(?) AND MATCH(`body`) AGAINST(?)',
            $result->query
        );
        $this->assertEquals(['hello', 'world'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (MATCH(`content`) AGAINST(?) AND `status` IN (?))',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (MATCH(`title`) AGAINST(?) OR MATCH(`body`) AGAINST(?))',
            $result->query
        );
        $this->assertEquals(['hello', 'hello'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(?) AND `slug` REGEXP ?',
            $result->query
        );
        $this->assertEquals(['hello world', '^[a-z-]+$'], $result->bindings);
    }

    public function testNotSearchStandalone(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('content', 'spam')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE NOT (MATCH(`content`) AGAINST(?))', $result->query);
        $this->assertEquals(['spam'], $result->bindings);
    }
    //  3. SQL-Specific: RAND()

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` ORDER BY `name` ASC, RAND(), `age` DESC',
            $result->query
        );
    }

    public function testRandomSortWithFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `status` IN (?) ORDER BY RAND()',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testRandomSortWithLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->limit(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ?', $result->query);
        $this->assertEquals([5], $result->bindings);
    }

    public function testRandomSortWithAggregation(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['category'])
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY RAND()', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
    }

    public function testRandomSortWithJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.user_id')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
        $this->assertStringContainsString('ORDER BY RAND()', $result->query);
    }

    public function testRandomSortWithDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT DISTINCT `status` FROM `t` ORDER BY RAND()',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testRandomSortWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Attribute {
                public function resolve(string $attribute): string
                {
                    return '_' . $attribute;
                }
            })
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY RAND()', $result->query);
    }

    public function testMultipleRandomSorts(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND(), RAND()', $result->query);
    }

    public function testRandomSortWithOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->limit(10)
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` ORDER BY RAND() LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([10, 5], $result->bindings);
    }
    //  5. Standalone Compiler method calls

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
    //  6. Filter edge cases

    public function testEqualWithSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `status` IN (?)', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testEqualWithManyValues(): void
    {
        $values = range(1, 10);
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('id', $values)])
            ->build();
        $this->assertBindingCount($result);

        $placeholders = implode(', ', array_fill(0, 10, '?'));
        $this->assertEquals("SELECT * FROM `t` WHERE `id` IN ({$placeholders})", $result->query);
        $this->assertEquals($values, $result->bindings);
    }

    public function testEqualWithEmptyArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('id', [])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 0', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testNotEqualWithExactlyTwoValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('role', ['guest', 'banned'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `role` NOT IN (?, ?)', $result->query);
        $this->assertEquals(['guest', 'banned'], $result->bindings);
    }

    public function testBetweenWithSameMinAndMax(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 25, 25)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([25, 25], $result->bindings);
    }

    public function testStartsWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', '')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result->query);
        $this->assertEquals(['%'], $result->bindings);
    }

    public function testEndsWithEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('name', '')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result->query);
        $this->assertEquals(['%'], $result->bindings);
    }

    public function testContainsWithSingleEmptyString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', [''])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result->query);
        $this->assertEquals(['%%'], $result->bindings);
    }

    public function testContainsWithManyValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['a', 'b', 'c', 'd', 'e'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ? OR `bio` LIKE ?)', $result->query);
        $this->assertEquals(['%a%', '%b%', '%c%', '%d%', '%e%'], $result->bindings);
    }

    public function testContainsAllWithSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('perms', ['read'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`perms` LIKE ?)', $result->query);
        $this->assertEquals(['%read%'], $result->bindings);
    }

    public function testNotContainsWithEmptyStringValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', [''])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `bio` NOT LIKE ?', $result->query);
        $this->assertEquals(['%%'], $result->bindings);
    }

    public function testComparisonWithFloatValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('price', 9.99)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `price` > ?', $result->query);
        $this->assertEquals([9.99], $result->bindings);
    }

    public function testComparisonWithNegativeValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('balance', -100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `balance` < ?', $result->query);
        $this->assertEquals([-100], $result->bindings);
    }

    public function testComparisonWithZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 0)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `score` >= ?', $result->query);
        $this->assertEquals([0], $result->bindings);
    }

    public function testComparisonWithVeryLargeInteger(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('id', 9999999999999)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([9999999999999], $result->bindings);
    }

    public function testComparisonWithStringValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('name', 'M')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `name` > ?', $result->query);
        $this->assertEquals(['M'], $result->bindings);
    }

    public function testBetweenWithStringValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('created_at', '2024-01-01', '2024-12-31')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `created_at` BETWEEN ? AND ?', $result->query);
        $this->assertEquals(['2024-01-01', '2024-12-31'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `deleted_at` IS NULL AND `verified_at` IS NOT NULL',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `a` IS NULL AND `b` IS NULL AND `c` IS NULL',
            $result->query
        );
    }

    public function testExistsWithSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`name` IS NOT NULL)', $result->query);
    }

    public function testExistsWithManyAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['a', 'b', 'c', 'd'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IS NOT NULL AND `b` IS NOT NULL AND `c` IS NOT NULL AND `d` IS NOT NULL)',
            $result->query
        );
    }

    public function testNotExistsWithManyAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['a', 'b', 'c'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IS NULL AND `b` IS NULL AND `c` IS NULL)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?))', $result->query);
        $this->assertEquals([1], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?))', $result->query);
        $this->assertEquals([1], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IN (?) AND `b` IN (?) AND `c` IN (?) AND `d` IN (?) AND `e` IN (?))',
            $result->query
        );
        $this->assertEquals([1, 2, 3, 4, 5], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (`a` IN (?) OR `b` IN (?) OR `c` IN (?) OR `d` IN (?) OR `e` IN (?))',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE (((`a` IN (?) AND `b` IN (?)) OR `c` IN (?)) AND `d` IN (?))',
            $result->query
        );
        $this->assertEquals([1, 2, 3, 4], $result->bindings);
    }

    public function testRawWithManyBindings(): void
    {
        $bindings = range(1, 10);
        $placeholders = implode(' AND ', array_map(fn ($i) => "col{$i} = ?", range(1, 10)));
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw($placeholders, $bindings)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals("SELECT * FROM `t` WHERE {$placeholders}", $result->query);
        $this->assertEquals($bindings, $result->bindings);
    }

    public function testFilterWithDotsInAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('table.column', ['value'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `table`.`column` IN (?)', $result->query);
    }

    public function testFilterWithUnderscoresInAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('my_column_name', ['value'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `my_column_name` IN (?)', $result->query);
    }

    public function testFilterWithNumericAttributeName(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('123', ['value'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `123` IN (?)', $result->query);
    }
    //  7. Aggregation edge cases

    public function testCountWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->count()->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testSumWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->sum('price')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT SUM(`price`) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testAvgWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->avg('score')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT AVG(`score`) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testMinWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->min('price')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT MIN(`price`) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testMaxWithoutAliasNoAsClause(): void
    {
        $result = (new Builder())->from('t')->max('price')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT MAX(`price`) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testCountWithAlias2(): void
    {
        $result = (new Builder())->from('t')->count('*', 'cnt')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('AS `cnt`', $result->query);
    }

    public function testSumWithAlias(): void
    {
        $result = (new Builder())->from('t')->sum('price', 'total')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('AS `total`', $result->query);
    }

    public function testAvgWithAlias(): void
    {
        $result = (new Builder())->from('t')->avg('score', 'avg_s')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('AS `avg_s`', $result->query);
    }

    public function testMinWithAlias(): void
    {
        $result = (new Builder())->from('t')->min('price', 'lowest')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('AS `lowest`', $result->query);
    }

    public function testMaxWithAlias(): void
    {
        $result = (new Builder())->from('t')->max('price', 'highest')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('AS `highest`', $result->query);
    }

    public function testMultipleSameAggregationType(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('id', 'count_id')
            ->count('*', 'count_all')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(`id`) AS `count_id`, COUNT(*) AS `count_all` FROM `t`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
        $this->assertStringContainsString('SUM(`price`) AS `price_sum`', $result->query);
        $this->assertStringContainsString('`category`', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
        $this->assertStringContainsString('GROUP BY `category`', $result->query);
        $this->assertStringContainsString('ORDER BY `cnt` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertEquals(['paid', 5], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('SUM(`total`) AS `revenue`', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('WHERE `orders`.`total` > ?', $result->query);
        $this->assertStringContainsString('GROUP BY `users`.`name`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
        $this->assertStringContainsString('ORDER BY `revenue` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals([0, 2, 20, 10], $result->bindings);
    }

    public function testAggregationWithAttributeResolver(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap([
                '$amount' => '_amount',
            ]))
            ->sum('$amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`_amount`) AS `total` FROM `t`', $result->query);
    }

    public function testMinMaxWithStringColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->min('name', 'first_name')
            ->max('name', 'last_name')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT MIN(`name`) AS `first_name`, MAX(`name`) AS `last_name` FROM `t`',
            $result->query
        );
    }
    //  8. Join edge cases

    public function testSelfJoin(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->join('employees', 'employees.manager_id', 'employees.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `employees` JOIN `employees` ON `employees`.`manager_id` = `employees`.`id`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString("JOIN `{$longTable}`", $result->query);
        $this->assertStringContainsString("ON `{$longLeft}` = `{$longRight}`", $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
        $this->assertStringContainsString('WHERE `orders`.`status` IN (?) AND `orders`.`total` > ?', $result->query);
        $this->assertStringContainsString('ORDER BY `orders`.`total` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals(['paid', 100, 25, 50], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('GROUP BY `users`.`name`', $result->query);
        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
        $this->assertEquals([3], $result->bindings);
    }

    public function testJoinWithDistinct(): void
    {
        $result = (new Builder())
            ->from('users')
            ->distinct()
            ->select(['users.name'])
            ->join('orders', 'users.id', 'orders.user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT `users`.`name`', $result->query);
        $this->assertStringContainsString('JOIN `orders`', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
        $this->assertStringContainsString('JOIN `archived_orders`', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `users`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `products`', $result->query);
        $this->assertStringContainsString('RIGHT JOIN `categories`', $result->query);
        $this->assertStringContainsString('CROSS JOIN `promotions`', $result->query);
    }

    public function testJoinWithAttributeResolverOnJoinColumns(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new AttributeMap([
                '$id' => '_uid',
                '$ref' => '_ref_id',
            ]))
            ->join('other', '$id', '$ref')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` JOIN `other` ON `_uid` = `_ref_id`',
            $result->query
        );
    }

    public function testCrossJoinCombinedWithFilter(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->crossJoin('colors')
            ->filter([Query::equal('sizes.active', [true])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `colors`', $result->query);
        $this->assertStringContainsString('WHERE `sizes`.`active` IN (?)', $result->query);
    }

    public function testCrossJoinFollowedByRegularJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->join('c', 'a.id', 'c.a_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `a` CROSS JOIN `b` JOIN `c` ON `a`.`id` = `c`.`a_id`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `profiles`', $result->query);
        $this->assertStringContainsString('`orders`.`total` > ?', $result->query);
        $this->assertStringContainsString('`profiles`.`avatar` IS NOT NULL', $result->query);
    }

    public function testJoinWithCustomOperatorLessThan(): void
    {
        $result = (new Builder())
            ->from('a')
            ->join('b', 'a.start', 'b.end', '<')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `a` JOIN `b` ON `a`.`start` < `b`.`end`',
            $result->query
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
        $this->assertBindingCount($result);

        $query = $result->query;
        $this->assertEquals(5, substr_count($query, 'JOIN'));
    }
    //  9. Union edge cases

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `main`) UNION (SELECT * FROM `a`) UNION (SELECT * FROM `b`) UNION (SELECT * FROM `c`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `main`) UNION ALL (SELECT * FROM `a`) UNION ALL (SELECT * FROM `b`) UNION ALL (SELECT * FROM `c`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `main`) UNION (SELECT * FROM `a`) UNION ALL (SELECT * FROM `b`) UNION (SELECT * FROM `c`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString(
            'UNION (SELECT * FROM `archived_users` JOIN `archived_orders`',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION (SELECT COUNT(*) AS `cnt` FROM `orders_2023` GROUP BY `status`)', $result->query);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION (SELECT * FROM `archive` ORDER BY `created_at` DESC LIMIT ?)', $result->query);
    }

    public function testUnionWithConditionProviders(): void
    {
        $sub = (new Builder())
            ->from('other')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org2']);
                }
            });

        $result = (new Builder())
            ->from('main')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE org = ?', $result->query);
        $this->assertStringContainsString('UNION (SELECT * FROM `other` WHERE org = ?)', $result->query);
        $this->assertEquals(['org1', 'org2'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 10, 2023, 5], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT `name` FROM `current`', $result->query);
        $this->assertStringContainsString('UNION (SELECT DISTINCT `name` FROM `archive`)', $result->query);
    }

    public function testUnionAfterReset(): void
    {
        $builder = (new Builder())->from('old');
        $builder->build();
        $builder->reset();

        $sub = (new Builder())->from('other');
        $result = $builder->from('fresh')->union($sub)->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `fresh`) UNION (SELECT * FROM `other`)',
            $result->query
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
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 1, 2, 10, 20], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(4, substr_count($result->query, 'UNION'));
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
        $this->assertBindingCount($result);

        $this->assertEquals(['paid', 'paid', 'paid', 'paid'], $result->bindings);
        $this->assertEquals(3, substr_count($result->query, 'UNION ALL'));
    }
    //  10. toRawSql edge cases

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
    //  11. when() edge cases

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
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE `status` IN (?)', $result->query);
        $this->assertStringContainsString('ORDER BY `name` ASC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertEquals(['active', 10], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE `a` IN (?) AND `b` IN (?) AND `d` IN (?) AND `e` IN (?)',
            $result->query
        );
        $this->assertEquals([1, 2, 4, 5], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `deep` IN (?)', $result->query);
        $this->assertEquals([1], $result->bindings);
    }

    public function testWhenThatAddsJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->when(true, fn (Builder $b) => $b->join('orders', 'users.id', 'orders.uid'))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
    }

    public function testWhenThatAddsAggregations(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->count('*', 'total')->groupBy(['status']))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
        $this->assertStringContainsString('GROUP BY `status`', $result->query);
    }

    public function testWhenThatAddsUnions(): void
    {
        $sub = (new Builder())->from('archive');

        $result = (new Builder())
            ->from('current')
            ->when(true, fn (Builder $b) => $b->union($sub))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testWhenFalseDoesNotAffectFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('status', ['banned'])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testWhenFalseDoesNotAffectJoins(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->join('other', 'a', 'b'))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('JOIN', $result->query);
    }

    public function testWhenFalseDoesNotAffectAggregations(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->count('*', 'total'))
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testWhenFalseDoesNotAffectSort(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->sortAsc('name'))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('ORDER BY', $result->query);
    }
    //  12. Condition provider edge cases

    public function testThreeConditionProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p1 = ?', ['v1']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p2 = ?', ['v2']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p3 = ?', ['v3']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE p1 = ? AND p2 = ? AND p3 = ?',
            $result->query
        );
        $this->assertEquals(['v1', 'v2', 'v3'], $result->bindings);
    }

    public function testProviderReturningEmptyConditionString(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('', []);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        // Empty string still appears as a WHERE clause element
        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testProviderWithManyBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('a IN (?, ?, ?, ?, ?)', [1, 2, 3, 4, 5]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE a IN (?, ?, ?, ?, ?)',
            $result->query
        );
        $this->assertEquals([1, 2, 3, 4, 5], $result->bindings);
    }

    public function testProviderCombinedWithCursorFilterHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->filter([Query::equal('status', ['active'])])
            ->cursorAfter('cur1')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('HAVING', $result->query);
        // filter, provider, cursor, having
        $this->assertEquals(['active', 'org1', 'cur1', 5], $result->bindings);
    }

    public function testProviderCombinedWithJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders`', $result->query);
        $this->assertStringContainsString('WHERE tenant = ?', $result->query);
        $this->assertEquals(['t1'], $result->bindings);
    }

    public function testProviderCombinedWithUnions(): void
    {
        $sub = (new Builder())->from('archive');

        $result = (new Builder())
            ->from('current')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE org = ?', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
        $this->assertEquals(['org1'], $result->bindings);
    }

    public function testProviderCombinedWithAggregations(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->groupBy(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS `total`', $result->query);
        $this->assertStringContainsString('WHERE org = ?', $result->query);
    }

    public function testProviderReferencesTableName(): void
    {
        $result = (new Builder())
            ->from('users')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition("EXISTS (SELECT 1 FROM {$table}_perms WHERE type = ?)", ['read']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('users_perms', $result->query);
        $this->assertEquals(['read'], $result->bindings);
    }

    public function testProviderBindingOrderWithComplexQuery(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p1 = ?', ['pv1']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p2 = ?', ['pv2']);
                }
            })
            ->filter([
                Query::equal('a', ['va']),
                Query::greaterThan('b', 10),
            ])
            ->cursorAfter('cur')
            ->limit(5)
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        // filter, provider1, provider2, cursor, limit, offset
        $this->assertEquals(['va', 10, 'pv1', 'pv2', 'cur', 5, 10], $result->bindings);
    }

    public function testProviderPreservedAcrossReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            });

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('WHERE org = ?', $result->query);
        $this->assertEquals(['org1'], $result->bindings);
    }

    public function testFourConditionProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('a = ?', [1]);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('b = ?', [2]);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('c = ?', [3]);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('d = ?', [4]);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `t` WHERE a = ? AND b = ? AND c = ? AND d = ?',
            $result->query
        );
        $this->assertEquals([1, 2, 3, 4], $result->bindings);
    }

    public function testProviderWithNoBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('1 = 1', []);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE 1 = 1', $result->query);
        $this->assertEquals([], $result->bindings);
    }
    //  13. Reset edge cases

    public function testResetPreservesAttributeResolver(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addHook(new class () implements Attribute {
                public function resolve(string $attribute): string
                {
                    return '_' . $attribute;
                }
            })
            ->filter([Query::equal('x', [1])]);

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->filter([Query::equal('y', [2])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`_y`', $result->query);
    }

    public function testResetPreservesConditionProviders(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            });

        $builder->build();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('org = ?', $result->query);
        $this->assertEquals(['org1'], $result->bindings);
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
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t2`', $result->query);
        $this->assertEquals([], $result->bindings);
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
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testResetClearsTable(): void
    {
        $builder = (new Builder())->from('old_table');
        $builder->build();
        $builder->reset();

        $result = $builder->from('new_table')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`new_table`', $result->query);
        $this->assertStringNotContainsString('`old_table`', $result->query);
    }

    public function testResetClearsUnionsAfterBuild(): void
    {
        $sub = (new Builder())->from('other');
        $builder = (new Builder())->from('main')->union($sub);
        $builder->build();
        $builder->reset();

        $result = $builder->from('fresh')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('UNION', $result->query);
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
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testMultipleResetCalls(): void
    {
        $builder = (new Builder())->from('t')->filter([Query::equal('a', [1])]);
        $builder->build();
        $builder->reset();
        $builder->reset();
        $builder->reset();

        $result = $builder->from('t2')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t2`', $result->query);
    }

    public function testResetBetweenDifferentQueryTypes(): void
    {
        $builder = new Builder();

        // First: aggregation query
        $builder->from('orders')->count('*', 'total')->groupBy(['status']);
        $result1 = $builder->build();
        $this->assertStringContainsString('COUNT(*)', $result1->query);

        $builder->reset();

        // Second: simple select query
        $builder->from('users')->select(['name'])->filter([Query::equal('active', [true])]);
        $result2 = $builder->build();
        $this->assertStringNotContainsString('COUNT', $result2->query);
        $this->assertStringContainsString('`name`', $result2->query);
    }

    public function testResetAfterUnion(): void
    {
        $sub = (new Builder())->from('other');
        $builder = (new Builder())->from('main')->union($sub);
        $builder->build();
        $builder->reset();

        $result = $builder->from('new')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `new`', $result->query);
        $this->assertEquals([], $result->bindings);
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
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `simple`', $result->query);
        $this->assertEquals([], $result->bindings);
    }
    //  14. Multiple build() calls

    public function testBuildTwiceModifyInBetween(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])]);

        $result1 = $builder->build();

        $builder->filter([Query::equal('b', [2])]);
        $result2 = $builder->build();

        $this->assertStringNotContainsString('`b`', $result1->query);
        $this->assertStringContainsString('`b`', $result2->query);
    }

    public function testBuildDoesNotMutatePendingQueries(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->filter([Query::equal('a', [1])])
            ->limit(10);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1->query, $result2->query);
        $this->assertEquals($result1->bindings, $result2->bindings);
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
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->filter([Query::equal('status', ['active'])]);

        $result1 = $builder->build();
        $result2 = $builder->build();
        $result3 = $builder->build();

        $this->assertEquals($result1->bindings, $result2->bindings);
        $this->assertEquals($result2->bindings, $result3->bindings);
    }

    public function testBuildAfterAddingMoreQueries(): void
    {
        $builder = (new Builder())->from('t');

        $result1 = $builder->build();
        $this->assertEquals('SELECT * FROM `t`', $result1->query);

        $builder->filter([Query::equal('a', [1])]);
        $result2 = $builder->build();
        $this->assertStringContainsString('WHERE', $result2->query);

        $builder->sortAsc('a');
        $result3 = $builder->build();
        $this->assertStringContainsString('ORDER BY', $result3->query);
    }

    public function testBuildWithUnionProducesConsistentResults(): void
    {
        $sub = (new Builder())->from('other')->filter([Query::equal('x', [1])]);
        $builder = (new Builder())->from('main')->union($sub);

        $result1 = $builder->build();
        $result2 = $builder->build();

        $this->assertEquals($result1->query, $result2->query);
        $this->assertEquals($result1->bindings, $result2->bindings);
    }

    public function testBuildThreeTimesWithIncreasingComplexity(): void
    {
        $builder = (new Builder())->from('t');

        $r1 = $builder->build();
        $this->assertEquals('SELECT * FROM `t`', $r1->query);

        $builder->filter([Query::equal('a', [1])]);
        $r2 = $builder->build();
        $this->assertEquals('SELECT * FROM `t` WHERE `a` IN (?)', $r2->query);

        $builder->limit(10)->offset(5);
        $r3 = $builder->build();
        $this->assertStringContainsString('LIMIT ?', $r3->query);
        $this->assertStringContainsString('OFFSET ?', $r3->query);
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

        $this->assertEquals([5], $r1->bindings);
        $this->assertEquals([5], $r2->bindings);
    }
    //  15. Binding ordering comprehensive

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
        $this->assertBindingCount($result);

        $this->assertEquals(['v1', 10, 1, 100], $result->bindings);
    }

    public function testBindingOrderThreeProviders(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p1 = ?', ['pv1']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p2 = ?', ['pv2']);
                }
            })
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('p3 = ?', ['pv3']);
                }
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['pv1', 'pv2', 'pv3'], $result->bindings);
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
        $this->assertBindingCount($result);

        // main filter, main limit, union1 bindings, union2 bindings
        $this->assertEquals([3, 5, 1, 2], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals([1, 2, 3], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals([1, 2, 3], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals([1, 2, 3], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(['v1', 10, 20], $result->bindings);
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
        $this->assertBindingCount($result);

        // filter, having1, having2, limit
        $this->assertEquals(['active', 5, 10000, 10], $result->bindings);
    }

    public function testBindingOrderFullPipelineWithEverything(): void
    {
        $sub = (new Builder())->from('archive')->filter([Query::equal('archived', [true])]);

        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('tenant = ?', ['t1']);
                }
            })
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
        $this->assertBindingCount($result);

        // filter(paid, 0), provider(t1), cursor(cursor_val), having(1), limit(25), offset(50), union(true)
        $this->assertEquals(['paid', 0, 't1', 'cursor_val', 1, 25, 50, true], $result->bindings);
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
        $this->assertBindingCount($result);

        // contains produces three LIKE bindings, then equal
        $this->assertEquals(['%php%', '%js%', '%go%', 'active'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals([18, 65, 50, 100], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(['A%', '%.com'], $result->bindings);
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
        $this->assertBindingCount($result);

        $this->assertEquals(['hello', '^test'], $result->bindings);
    }

    public function testBindingOrderWithCursorBeforeFilterAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('org = ?', ['org1']);
                }
            })
            ->filter([Query::equal('a', ['x'])])
            ->cursorBefore('my_cursor')
            ->limit(10)
            ->offset(0)
            ->build();
        $this->assertBindingCount($result);

        // filter, provider, cursor, limit, offset
        $this->assertEquals(['x', 'org1', 'my_cursor', 10, 0], $result->bindings);
    }
    //  16. Empty/minimal queries

    public function testBuildWithNoFromNoFilters(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())->from('')->build();
    }

    public function testBuildWithOnlyLimit(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->limit(10)
            ->build();
    }

    public function testBuildWithOnlyOffset(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->offset(50)
            ->build();
    }

    public function testBuildWithOnlySort(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->sortAsc('name')
            ->build();
    }

    public function testBuildWithOnlySelect(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->select(['a', 'b'])
            ->build();
    }

    public function testBuildWithOnlyAggregationNoFrom(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())
            ->from('')
            ->count('*', 'total')
            ->build();
    }

    public function testBuildWithEmptyFilterArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testBuildWithEmptySelectArray(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select([])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT  FROM `t`', $result->query);
    }

    public function testBuildWithOnlyHavingNoGroupBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->having([Query::greaterThan('cnt', 0)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING `cnt` > ?', $result->query);
        $this->assertStringNotContainsString('GROUP BY', $result->query);
    }

    public function testBuildWithOnlyDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT * FROM `t`', $result->query);
    }
    //  Spatial/Vector/ElemMatch Exception Tests


    public function testSpatialCrosses(): void
    {
        $result = (new Builder())->from('t')->filter([Query::crosses('attr', [1.0, 2.0])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Crosses', $result->query);
    }

    public function testSpatialDistanceLessThan(): void
    {
        $result = (new Builder())->from('t')->filter([Query::distanceLessThan('attr', [0, 0], 1000, true)])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('metre', $result->query);
    }

    public function testSpatialIntersects(): void
    {
        $result = (new Builder())->from('t')->filter([Query::intersects('attr', [1.0, 2.0])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Intersects', $result->query);
    }

    public function testSpatialOverlaps(): void
    {
        $result = (new Builder())->from('t')->filter([Query::overlaps('attr', [[0, 0], [1, 1]])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Overlaps', $result->query);
    }

    public function testSpatialTouches(): void
    {
        $result = (new Builder())->from('t')->filter([Query::touches('attr', [1.0, 2.0])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Touches', $result->query);
    }

    public function testSpatialNotIntersects(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notIntersects('attr', [1.0, 2.0])])->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('NOT ST_Intersects', $result->query);
    }

    public function testUnsupportedFilterTypeVectorDot(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorDot('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeVectorCosine(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorCosine('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeVectorEuclidean(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::vectorEuclidean('attr', [1.0, 2.0])])->build();
    }

    public function testUnsupportedFilterTypeElemMatch(): void
    {
        $this->expectException(UnsupportedException::class);
        (new Builder())->from('t')->filter([Query::elemMatch('attr', [Query::equal('x', [1])])])->build();
    }
    //  toRawSql Edge Cases

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
    //  Kitchen Sink Exact SQL

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
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT DISTINCT COUNT(*) AS `total`, `status` FROM `orders` JOIN `users` ON `orders`.`uid` = `users`.`id` WHERE `amount` > ? GROUP BY `status` HAVING `total` > ? ORDER BY `status` ASC LIMIT ? OFFSET ?) UNION (SELECT * FROM `archive` WHERE `status` IN (?))',
            $result->query
        );
        $this->assertEquals([100, 5, 10, 20, 'closed'], $result->bindings);
    }
    //  Feature Combination Tests

    public function testDistinctWithUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())->from('a')->distinct()->union($other)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('(SELECT DISTINCT * FROM `a`) UNION (SELECT * FROM `b`)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testRawInsideLogicalAnd(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::and([
                Query::greaterThan('x', 1),
                Query::raw('custom_func(y) > ?', [5]),
            ])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`x` > ? AND custom_func(y) > ?)', $result->query);
        $this->assertEquals([1, 5], $result->bindings);
    }

    public function testRawInsideLogicalOr(): void
    {
        $result = (new Builder())->from('t')
            ->filter([Query::or([
                Query::equal('a', [1]),
                Query::raw('b IS NOT NULL', []),
            ])])
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`a` IN (?) OR b IS NOT NULL)', $result->query);
        $this->assertEquals([1], $result->bindings);
    }

    public function testAggregationWithCursor(): void
    {
        $result = (new Builder())->from('t')
            ->count('*', 'total')
            ->cursorAfter('abc')
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('COUNT(*)', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertContains('abc', $result->bindings);
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
        $this->assertBindingCount($result);
        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('ORDER BY', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }

    public function testConditionProviderWithNoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE _tenant = ?', $result->query);
        $this->assertEquals(['t1'], $result->bindings);
    }

    public function testConditionProviderWithCursorNoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->cursorAfter('abc')
            ->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('_tenant = ?', $result->query);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        // Provider bindings come before cursor bindings
        $this->assertEquals(['t1', 'abc'], $result->bindings);
    }

    public function testConditionProviderWithDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT DISTINCT * FROM `t` WHERE _tenant = ?', $result->query);
        $this->assertEquals(['t1'], $result->bindings);
    }

    public function testConditionProviderPersistsAfterReset(): void
    {
        $builder = (new Builder())
            ->from('t')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            });
        $builder->build();
        $builder->reset()->from('other');
        $result = $builder->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('FROM `other`', $result->query);
        $this->assertStringContainsString('_tenant = ?', $result->query);
        $this->assertEquals(['t1'], $result->bindings);
    }

    public function testConditionProviderWithHaving(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['status'])
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_tenant = ?', ['t1']);
                }
            })
            ->having([Query::greaterThan('total', 5)])
            ->build();
        $this->assertBindingCount($result);
        // Provider should be in WHERE, not HAVING
        $this->assertStringContainsString('WHERE _tenant = ?', $result->query);
        $this->assertStringContainsString('HAVING `total` > ?', $result->query);
        // Provider bindings before having bindings
        $this->assertEquals(['t1', 5], $result->bindings);
    }

    public function testUnionWithConditionProvider(): void
    {
        $sub = (new Builder())
            ->from('b')
            ->addHook(new class () implements Filter {
                public function filter(string $table): Condition
                {
                    return new Condition('_deleted = ?', [0]);
                }
            });
        $result = (new Builder())
            ->from('a')
            ->union($sub)
            ->build();
        $this->assertBindingCount($result);
        // Sub-query should include the condition provider
        $this->assertStringContainsString('UNION (SELECT * FROM `b` WHERE _deleted = ?)', $result->query);
        $this->assertEquals([0], $result->bindings);
    }
    //  Boundary Value Tests

    public function testNegativeLimit(): void
    {
        $result = (new Builder())->from('t')->limit(-1)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([-1], $result->bindings);
    }

    public function testNegativeOffset(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(-5)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', [null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NULL', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::equal('col', ['a', null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` IN (?) OR `col` IS NULL)', $result->query);
        $this->assertSame(['a'], $result->bindings);
    }

    public function testNotEqualWithNullOnly(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', [null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `col` IS NOT NULL', $result->query);
        $this->assertSame([], $result->bindings);
    }

    public function testNotEqualWithNullAndNonNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` != ? AND `col` IS NOT NULL)', $result->query);
        $this->assertSame(['a'], $result->bindings);
    }

    public function testNotEqualWithMultipleNonNullAndNull(): void
    {
        $result = (new Builder())->from('t')->filter([Query::notEqual('col', ['a', 'b', null])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE (`col` NOT IN (?, ?) AND `col` IS NOT NULL)', $result->query);
        $this->assertSame(['a', 'b'], $result->bindings);
    }

    public function testBetweenReversedMinMax(): void
    {
        $result = (new Builder())->from('t')->filter([Query::between('age', 65, 18)])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([65, 18], $result->bindings);
    }

    public function testContainsWithSqlWildcard(): void
    {
        $result = (new Builder())->from('t')->filter([Query::contains('bio', ['100%'])])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `bio` LIKE ?', $result->query);
        $this->assertEquals(['%100\%%'], $result->bindings);
    }

    public function testStartsWithWithWildcard(): void
    {
        $result = (new Builder())->from('t')->filter([Query::startsWith('name', '%admin')])->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` WHERE `name` LIKE ?', $result->query);
        $this->assertEquals(['\%admin%'], $result->bindings);
    }

    public function testCursorWithNullValue(): void
    {
        // Null cursor value is ignored by groupByType since cursor stays null
        $result = (new Builder())->from('t')->cursorAfter(null)->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('_cursor', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testCursorWithIntegerValue(): void
    {
        $result = (new Builder())->from('t')->cursorAfter(42)->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertSame([42], $result->bindings);
    }

    public function testCursorWithFloatValue(): void
    {
        $result = (new Builder())->from('t')->cursorAfter(3.14)->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertSame([3.14], $result->bindings);
    }

    public function testMultipleLimitsFirstWins(): void
    {
        $result = (new Builder())->from('t')->limit(10)->limit(20)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t` LIMIT ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testMultipleOffsetsFirstWins(): void
    {
        // OFFSET without LIMIT is suppressed
        $result = (new Builder())->from('t')->offset(5)->offset(50)->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testCursorAfterAndBeforeFirstWins(): void
    {
        $result = (new Builder())->from('t')->cursorAfter('a')->cursorBefore('b')->build();
        $this->assertBindingCount($result);
        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertStringNotContainsString('`_cursor` < ?', $result->query);
    }

    public function testEmptyTableWithJoin(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())->from('')->join('other', 'a', 'b')->build();
    }

    public function testBuildWithoutFromCall(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');
        (new Builder())->filter([Query::equal('x', [1])])->build();
    }
    //  Standalone Compiler Method Tests

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
        $this->expectException(UnsupportedException::class);
        $builder->compileOrder(Query::limit(10));
    }

    public function testCompileJoinException(): void
    {
        $builder = new Builder();
        $this->expectException(UnsupportedException::class);
        $builder->compileJoin(Query::equal('x', [1]));
    }
    //  Query::compile() Integration Tests

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
    //  Reset Behavior

    public function testResetFollowedByUnion(): void
    {
        $builder = (new Builder())
            ->from('a')
            ->union((new Builder())->from('old'));
        $builder->reset()->from('b');
        $result = $builder->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `b`', $result->query);
        $this->assertStringNotContainsString('UNION', $result->query);
    }

    public function testResetClearsBindingsAfterBuild(): void
    {
        $builder = (new Builder())->from('t')->filter([Query::equal('x', [1])]);
        $builder->build();
        $this->assertNotEmpty($builder->getBindings());
        $builder->reset()->from('t');
        $result = $builder->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }
    //  Missing Binding Assertions

    public function testSortAscBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortAsc('name')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testSortDescBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortDesc('name')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testSortRandomBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->sortRandom()->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testDistinctBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->distinct()->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testJoinBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->join('other', 'a', 'b')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testCrossJoinBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->crossJoin('other')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testGroupByBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->groupBy(['status'])->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }

    public function testCountWithAliasBindingsEmpty(): void
    {
        $result = (new Builder())->from('t')->count('*', 'total')->build();
        $this->assertBindingCount($result);
        $this->assertEquals([], $result->bindings);
    }
    // DML: INSERT

    public function testInsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`name`, `email`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'a@b.com'], $result->bindings);
    }

    public function testInsertBatch(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->set(['name' => 'Bob', 'email' => 'b@b.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'a@b.com', 'Bob', 'b@b.com'], $result->bindings);
    }

    public function testInsertNoRowsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('users')
            ->insert();
    }

    public function testIntoAliasesFrom(): void
    {
        $builder = new Builder();
        $builder->into('users')->set(['name' => 'Alice'])->insert();
        $this->assertStringContainsString('users', $builder->insert()->query);
    }
    // DML: UPSERT

    public function testUpsertSingleRow(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice', 'email' => 'a@b.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `email` = VALUES(`email`)',
            $result->query
        );
        $this->assertEquals([1, 'Alice', 'a@b.com'], $result->bindings);
    }

    public function testUpsertMultipleConflictColumns(): void
    {
        $result = (new Builder())
            ->into('user_roles')
            ->set(['user_id' => 1, 'role_id' => 2, 'granted_at' => '2024-01-01'])
            ->onConflict(['user_id', 'role_id'], ['granted_at'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO `user_roles` (`user_id`, `role_id`, `granted_at`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `granted_at` = VALUES(`granted_at`)',
            $result->query
        );
        $this->assertEquals([1, 2, '2024-01-01'], $result->bindings);
    }
    // DML: UPDATE

    public function testUpdateWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('status', ['inactive'])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `users` SET `status` = ? WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['archived', 'inactive'], $result->bindings);
    }

    public function testUpdateWithSetRaw(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'Alice'])
            ->setRaw('login_count', 'login_count + 1')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `users` SET `name` = ?, `login_count` = login_count + 1 WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals(['Alice', 1], $result->bindings);
    }

    public function testUpdateWithFilterHook(): void
    {
        $hook = new class () implements Filter, Hook {
            public function filter(string $table): Condition
            {
                return new Condition('`_tenant` = ?', ['tenant_123']);
            }
        };

        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'active'])
            ->filter([Query::equal('id', [1])])
            ->addHook($hook)
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `users` SET `status` = ? WHERE `id` IN (?) AND `_tenant` = ?',
            $result->query
        );
        $this->assertEquals(['active', 1, 'tenant_123'], $result->bindings);
    }

    public function testUpdateWithoutWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'active'])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals('UPDATE `users` SET `status` = ?', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testUpdateWithOrderByAndLimit(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('active', [false])])
            ->sortAsc('created_at')
            ->limit(100)
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `users` SET `status` = ? WHERE `active` IN (?) ORDER BY `created_at` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['archived', false, 100], $result->bindings);
    }

    public function testUpdateNoAssignmentsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('users')
            ->update();
    }
    // DML: DELETE

    public function testDeleteWithWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::lessThan('last_login', '2024-01-01')])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'DELETE FROM `users` WHERE `last_login` < ?',
            $result->query
        );
        $this->assertEquals(['2024-01-01'], $result->bindings);
    }

    public function testDeleteWithFilterHook(): void
    {
        $hook = new class () implements Filter, Hook {
            public function filter(string $table): Condition
            {
                return new Condition('`_tenant` = ?', ['tenant_123']);
            }
        };

        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['deleted'])])
            ->addHook($hook)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'DELETE FROM `users` WHERE `status` IN (?) AND `_tenant` = ?',
            $result->query
        );
        $this->assertEquals(['deleted', 'tenant_123'], $result->bindings);
    }

    public function testDeleteWithoutWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals('DELETE FROM `users`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testDeleteWithOrderByAndLimit(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::lessThan('created_at', '2023-01-01')])
            ->sortAsc('created_at')
            ->limit(1000)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'DELETE FROM `logs` WHERE `created_at` < ? ORDER BY `created_at` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['2023-01-01', 1000], $result->bindings);
    }
    // DML: Reset clears new state

    public function testResetClearsDmlState(): void
    {
        $builder = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice'])
            ->setRaw('count', 'count + 1')
            ->onConflict(['id'], ['name']);

        $builder->reset();

        $this->expectException(ValidationException::class);
        $builder->into('users')->insert();
    }
    // Validation: Missing table

    public function testInsertWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');

        (new Builder())->set(['name' => 'Alice'])->insert();
    }

    public function testUpdateWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');

        (new Builder())->set(['name' => 'Alice'])->update();
    }

    public function testDeleteWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');

        (new Builder())->delete();
    }

    public function testSelectWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No table specified');

        (new Builder())->build();
    }
    // Validation: Empty rows

    public function testInsertEmptyRowThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('empty row');

        (new Builder())->into('users')->set([])->insert();
    }
    // Validation: Inconsistent batch columns

    public function testInsertInconsistentBatchThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('different columns');

        (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->set(['name' => 'Bob', 'phone' => '555-1234'])
            ->insert();
    }
    // Validation: Upsert without onConflict

    public function testUpsertWithoutConflictKeysThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No conflict keys');

        (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->upsert();
    }

    public function testUpsertWithoutConflictUpdateColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No conflict update columns');

        (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], [])
            ->upsert();
    }

    public function testUpsertConflictColumnNotInRowThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage("not present in the row data");

        (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], ['email'])
            ->upsert();
    }
    //  INTERSECT / EXCEPT

    public function testIntersect(): void
    {
        $other = (new Builder())->from('admins');
        $result = (new Builder())
            ->from('users')
            ->intersect($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) INTERSECT (SELECT * FROM `admins`)',
            $result->query
        );
    }

    public function testIntersectAll(): void
    {
        $other = (new Builder())->from('admins');
        $result = (new Builder())
            ->from('users')
            ->intersectAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) INTERSECT ALL (SELECT * FROM `admins`)',
            $result->query
        );
    }

    public function testExcept(): void
    {
        $other = (new Builder())->from('banned');
        $result = (new Builder())
            ->from('users')
            ->except($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) EXCEPT (SELECT * FROM `banned`)',
            $result->query
        );
    }

    public function testExceptAll(): void
    {
        $other = (new Builder())->from('banned');
        $result = (new Builder())
            ->from('users')
            ->exceptAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users`) EXCEPT ALL (SELECT * FROM `banned`)',
            $result->query
        );
    }

    public function testIntersectWithBindings(): void
    {
        $other = (new Builder())->from('admins')->filter([Query::equal('role', ['admin'])]);
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->intersect($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            '(SELECT * FROM `users` WHERE `status` IN (?)) INTERSECT (SELECT * FROM `admins` WHERE `role` IN (?))',
            $result->query
        );
        $this->assertEquals(['active', 'admin'], $result->bindings);
    }

    public function testExceptWithBindings(): void
    {
        $other = (new Builder())->from('banned')->filter([Query::equal('reason', ['spam'])]);
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->except($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 'spam'], $result->bindings);
    }

    public function testMixedSetOperations(): void
    {
        $q1 = (new Builder())->from('a');
        $q2 = (new Builder())->from('b');
        $q3 = (new Builder())->from('c');

        $result = (new Builder())
            ->from('main')
            ->union($q1)
            ->intersect($q2)
            ->except($q3)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION', $result->query);
        $this->assertStringContainsString('INTERSECT', $result->query);
        $this->assertStringContainsString('EXCEPT', $result->query);
    }

    public function testIntersectFluentReturnsSameInstance(): void
    {
        $builder = new Builder();
        $other = (new Builder())->from('t');
        $this->assertSame($builder, $builder->from('t')->intersect($other));
    }

    public function testExceptFluentReturnsSameInstance(): void
    {
        $builder = new Builder();
        $other = (new Builder())->from('t');
        $this->assertSame($builder, $builder->from('t')->except($other));
    }
    //  Row Locking

    public function testForUpdate(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->filter([Query::equal('id', [1])])
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `accounts` WHERE `id` IN (?) FOR UPDATE',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testForShare(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->filter([Query::equal('id', [1])])
            ->forShare()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `accounts` WHERE `id` IN (?) FOR SHARE',
            $result->query
        );
    }

    public function testForUpdateWithLimitAndOffset(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->limit(10)
            ->offset(5)
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `accounts` LIMIT ? OFFSET ? FOR UPDATE',
            $result->query
        );
        $this->assertEquals([10, 5], $result->bindings);
    }

    public function testLockModeResetClears(): void
    {
        $builder = (new Builder())->from('t')->forUpdate();
        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }
    //  Transaction Statements

    public function testBegin(): void
    {
        $result = (new Builder())->begin();
        $this->assertEquals('BEGIN', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testCommit(): void
    {
        $result = (new Builder())->commit();
        $this->assertEquals('COMMIT', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testRollback(): void
    {
        $result = (new Builder())->rollback();
        $this->assertEquals('ROLLBACK', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testSavepoint(): void
    {
        $result = (new Builder())->savepoint('sp1');
        $this->assertEquals('SAVEPOINT `sp1`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testReleaseSavepoint(): void
    {
        $result = (new Builder())->releaseSavepoint('sp1');
        $this->assertEquals('RELEASE SAVEPOINT `sp1`', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testRollbackToSavepoint(): void
    {
        $result = (new Builder())->rollbackToSavepoint('sp1');
        $this->assertEquals('ROLLBACK TO SAVEPOINT `sp1`', $result->query);
        $this->assertEquals([], $result->bindings);
    }
    //  INSERT...SELECT

    public function testInsertSelect(): void
    {
        $source = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('status', ['active'])]);

        $result = (new Builder())
            ->into('archive')
            ->fromSelect(['name', 'email'], $source)
            ->insertSelect();

        $this->assertEquals(
            'INSERT INTO `archive` (`name`, `email`) SELECT `name`, `email` FROM `users` WHERE `status` IN (?)',
            $result->query
        );
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testInsertSelectWithoutSourceThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('No SELECT source specified');

        (new Builder())
            ->into('archive')
            ->insertSelect();
    }

    public function testInsertSelectWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);

        $source = (new Builder())->from('users');

        (new Builder())
            ->fromSelect(['name'], $source)
            ->insertSelect();
    }

    public function testInsertSelectWithAggregation(): void
    {
        $source = (new Builder())
            ->from('orders')
            ->select(['customer_id'])
            ->count('*', 'order_count')
            ->groupBy(['customer_id']);

        $result = (new Builder())
            ->into('customer_stats')
            ->fromSelect(['customer_id', 'order_count'], $source)
            ->insertSelect();

        $this->assertStringContainsString('INSERT INTO `customer_stats`', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `order_count`', $result->query);
    }

    public function testInsertSelectResetClears(): void
    {
        $source = (new Builder())->from('users');
        $builder = (new Builder())
            ->into('archive')
            ->fromSelect(['name'], $source);

        $builder->reset();

        $this->expectException(ValidationException::class);
        $builder->into('archive')->insertSelect();
    }
    //  CTEs (WITH)

    public function testCteWith(): void
    {
        $cte = (new Builder())
            ->from('orders')
            ->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->with('paid_orders', $cte)
            ->from('paid_orders')
            ->select(['customer_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'WITH `paid_orders` AS (SELECT * FROM `orders` WHERE `status` IN (?)) SELECT `customer_id` FROM `paid_orders`',
            $result->query
        );
        $this->assertEquals(['paid'], $result->bindings);
    }

    public function testCteWithRecursive(): void
    {
        $cte = (new Builder())->from('categories');

        $result = (new Builder())
            ->withRecursive('tree', $cte)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'WITH RECURSIVE `tree` AS (SELECT * FROM `categories`) SELECT * FROM `tree`',
            $result->query
        );
    }

    public function testMultipleCtes(): void
    {
        $cte1 = (new Builder())->from('orders')->filter([Query::equal('status', ['paid'])]);
        $cte2 = (new Builder())->from('returns')->filter([Query::equal('status', ['approved'])]);

        $result = (new Builder())
            ->with('paid', $cte1)
            ->with('approved_returns', $cte2)
            ->from('paid')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('WITH `paid` AS', $result->query);
        $this->assertStringContainsString('`approved_returns` AS', $result->query);
        $this->assertEquals(['paid', 'approved'], $result->bindings);
    }

    public function testCteBindingsComeBefore(): void
    {
        $cte = (new Builder())->from('orders')->filter([Query::equal('year', [2024])]);

        $result = (new Builder())
            ->with('recent', $cte)
            ->from('recent')
            ->filter([Query::greaterThan('amount', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([2024, 100], $result->bindings);
    }

    public function testCteResetClears(): void
    {
        $cte = (new Builder())->from('orders');
        $builder = (new Builder())->with('o', $cte)->from('o');
        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testMixedRecursiveAndNonRecursiveCte(): void
    {
        $cte1 = (new Builder())->from('categories');
        $cte2 = (new Builder())->from('products');

        $result = (new Builder())
            ->with('prods', $cte2)
            ->withRecursive('tree', $cte1)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('WITH RECURSIVE', $result->query);
        $this->assertStringContainsString('`prods` AS', $result->query);
        $this->assertStringContainsString('`tree` AS', $result->query);
    }
    //  CASE/WHEN + selectRaw()

    public function testCaseBuilder(): void
    {
        $case = (new CaseBuilder())
            ->when('status = ?', '?', ['active'], ['Active'])
            ->when('status = ?', '?', ['inactive'], ['Inactive'])
            ->elseResult('?', ['Unknown'])
            ->alias('label')
            ->build();

        $this->assertEquals(
            'CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END AS label',
            $case->sql
        );
        $this->assertEquals(['active', 'Active', 'inactive', 'Inactive', 'Unknown'], $case->bindings);
    }

    public function testCaseBuilderWithoutElse(): void
    {
        $case = (new CaseBuilder())
            ->when('x > ?', '1', [10])
            ->build();

        $this->assertEquals('CASE WHEN x > ? THEN 1 END', $case->sql);
        $this->assertEquals([10], $case->bindings);
    }

    public function testCaseBuilderWithoutAlias(): void
    {
        $case = (new CaseBuilder())
            ->when('x = 1', "'yes'")
            ->elseResult("'no'")
            ->build();

        $this->assertEquals("CASE WHEN x = 1 THEN 'yes' ELSE 'no' END", $case->sql);
    }

    public function testCaseBuilderNoWhensThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('at least one WHEN');

        (new CaseBuilder())->build();
    }

    public function testCaseExpressionToSql(): void
    {
        $case = (new CaseBuilder())
            ->when('a = ?', '1', [1])
            ->build();

        $this->assertEquals('CASE WHEN a = ? THEN 1 END', $case->sql);
        $this->assertEquals([1], $case->bindings);
    }

    public function testSelectRaw(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectRaw('SUM(amount) AS total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(amount) AS total FROM `orders`', $result->query);
    }

    public function testSelectRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectRaw('IF(amount > ?, 1, 0) AS big_order', [1000])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT IF(amount > ?, 1, 0) AS big_order FROM `orders`', $result->query);
        $this->assertEquals([1000], $result->bindings);
    }

    public function testSelectRawCombinedWithSelect(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'customer_id'])
            ->selectRaw('SUM(amount) AS total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `id`, `customer_id`, SUM(amount) AS total FROM `orders`', $result->query);
    }

    public function testSelectRawWithCaseExpression(): void
    {
        $case = (new CaseBuilder())
            ->when('status = ?', '?', ['active'], ['Active'])
            ->elseResult('?', ['Other'])
            ->alias('label')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->select(['id'])
            ->selectRaw($case->sql, $case->bindings)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN status = ? THEN ? ELSE ? END AS label', $result->query);
        $this->assertEquals(['active', 'Active', 'Other'], $result->bindings);
    }

    public function testSelectRawResetClears(): void
    {
        $builder = (new Builder())->from('t')->selectRaw('1 AS one');
        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testSetRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->set(['name' => 'Alice'])
            ->setRaw('balance', 'balance + ?', [100])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE `accounts` SET `name` = ?, `balance` = balance + ? WHERE `id` IN (?)',
            $result->query
        );
        $this->assertEquals(['Alice', 100, 1], $result->bindings);
    }

    public function testSetRawWithBindingsResetClears(): void
    {
        $builder = (new Builder())->from('t')->setRaw('x', 'x + ?', [1]);
        $builder->reset();

        $this->expectException(ValidationException::class);
        $builder->from('t')->update();
    }

    public function testMultipleSelectRaw(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectRaw('COUNT(*) AS cnt')
            ->selectRaw('MAX(price) AS max_price')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) AS cnt, MAX(price) AS max_price FROM `t`', $result->query);
    }

    public function testForUpdateNotInUnion(): void
    {
        $other = (new Builder())->from('b');
        $result = (new Builder())
            ->from('a')
            ->forUpdate()
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE', $result->query);
    }

    public function testCteWithUnion(): void
    {
        $cte = (new Builder())->from('orders');
        $other = (new Builder())->from('archive_orders');

        $result = (new Builder())
            ->with('o', $cte)
            ->from('o')
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('WITH `o` AS', $result->query);
        $this->assertStringContainsString('UNION', $result->query);
    }
    //  Spatial feature interface

    public function testImplementsSpatial(): void
    {
        $this->assertInstanceOf(Spatial::class, new Builder());
    }

    public function testFilterDistanceMeters(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [40.7128, -74.0060], '<', 5000.0, true)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance(ST_SRID(`coords`, 4326), ST_GeomFromText(?, 4326), \'metre\') < ?', $result->query);
        $this->assertEquals('POINT(40.7128 -74.006)', $result->bindings[0]);
        $this->assertEquals(5000.0, $result->bindings[1]);
    }

    public function testFilterDistanceNoMeters(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [1.0, 2.0], '>', 100.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance(`coords`, ST_GeomFromText(?)) > ?', $result->query);
    }

    public function testFilterIntersectsPoint(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Intersects(`area`, ST_GeomFromText(?, 4326))', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
    }

    public function testFilterNotIntersects(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterNotIntersects('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Intersects', $result->query);
    }

    public function testFilterCovers(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterCovers('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Contains(`area`, ST_GeomFromText(?, 4326))', $result->query);
    }

    public function testFilterSpatialEquals(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterSpatialEquals('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Equals', $result->query);
    }

    public function testSpatialWithLinestring(): void
    {
        $result = (new Builder())
            ->from('roads')
            ->filterIntersects('path', [[0, 0], [1, 1], [2, 2]])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('LINESTRING(0 0, 1 1, 2 2)', $result->bindings[0]);
    }

    public function testSpatialWithPolygon(): void
    {
        $result = (new Builder())
            ->from('areas')
            ->filterIntersects('zone', [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]])
            ->build();
        $this->assertBindingCount($result);

        /** @var string $wkt */
        $wkt = $result->bindings[0];
        $this->assertStringContainsString('POLYGON', $wkt);
    }
    //  JSON feature interface

    public function testImplementsJson(): void
    {
        $this->assertInstanceOf(Json::class, new Builder());
    }

    public function testFilterJsonContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonContains('tags', 'php')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_CONTAINS(`tags`, ?)', $result->query);
        $this->assertEquals('"php"', $result->bindings[0]);
    }

    public function testFilterJsonNotContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('tags', 'old')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT JSON_CONTAINS(`tags`, ?)', $result->query);
    }

    public function testFilterJsonOverlaps(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'go'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_OVERLAPS(`tags`, ?)', $result->query);
        $this->assertEquals('["php","go"]', $result->bindings[0]);
    }

    public function testFilterJsonPath(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('metadata', 'level', '>', 5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("JSON_EXTRACT(`metadata`, '$.level') > ?", $result->query);
        $this->assertEquals(5, $result->bindings[0]);
    }

    public function testSetJsonAppend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonAppend('tags', ['new_tag'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_MERGE_PRESERVE(IFNULL(`tags`, JSON_ARRAY()), ?)', $result->query);
    }

    public function testSetJsonPrepend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonPrepend('tags', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_MERGE_PRESERVE(?, IFNULL(`tags`, JSON_ARRAY()))', $result->query);
    }

    public function testSetJsonInsert(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonInsert('tags', 0, 'inserted')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_ARRAY_INSERT', $result->query);
    }

    public function testSetJsonRemove(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonRemove('tags', 'old_tag')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_REMOVE', $result->query);
    }
    //  Hints feature interface

    public function testImplementsHints(): void
    {
        $this->assertInstanceOf(Hints::class, new Builder());
    }

    public function testHintInSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->hint('NO_INDEX_MERGE(users)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('/*+ NO_INDEX_MERGE(users) */', $result->query);
    }

    public function testMaxExecutionTime(): void
    {
        $result = (new Builder())
            ->from('users')
            ->maxExecutionTime(5000)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('/*+ MAX_EXECUTION_TIME(5000) */', $result->query);
    }

    public function testMultipleHints(): void
    {
        $result = (new Builder())
            ->from('users')
            ->hint('NO_INDEX_MERGE(users)')
            ->hint('BKA(users)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('/*+ NO_INDEX_MERGE(users) BKA(users) */', $result->query);
    }
    //  Window functions

    public function testImplementsWindows(): void
    {
        $this->assertInstanceOf(Windows::class, new Builder());
    }

    public function testSelectWindowRowNumber(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('ROW_NUMBER()', 'rn', ['customer_id'], ['created_at'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `created_at` ASC) AS `rn`', $result->query);
    }

    public function testSelectWindowRank(): void
    {
        $result = (new Builder())
            ->from('scores')
            ->selectWindow('RANK()', 'rank', null, ['-score'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RANK() OVER (ORDER BY `score` DESC) AS `rank`', $result->query);
    }

    public function testSelectWindowPartitionOnly(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('SUM(amount)', 'total', ['dept'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(amount) OVER (PARTITION BY `dept`) AS `total`', $result->query);
    }

    public function testSelectWindowNoPartitionNoOrder(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->selectWindow('COUNT(*)', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) OVER () AS `total`', $result->query);
    }
    //  CASE integration

    public function testSelectCaseExpression(): void
    {
        $case = (new CaseBuilder())
            ->when('status = ?', '?', ['active'], ['Active'])
            ->elseResult('?', ['Other'])
            ->alias('label')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->select(['id'])
            ->selectCase($case)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN status = ? THEN ? ELSE ? END AS label', $result->query);
        $this->assertEquals(['active', 'Active', 'Other'], $result->bindings);
    }

    public function testSetCaseExpression(): void
    {
        $case = (new CaseBuilder())
            ->when('age >= ?', '?', [18], ['adult'])
            ->elseResult('?', ['minor'])
            ->build();

        $result = (new Builder())
            ->from('users')
            ->setCase('category', $case)
            ->filter([Query::greaterThan('id', 0)])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`category` = CASE WHEN age >= ? THEN ? ELSE ? END', $result->query);
        $this->assertEquals([18, 'adult', 'minor', 0], $result->bindings);
    }
    //  Query factory methods for JSON

    public function testQueryJsonContainsFactory(): void
    {
        $q = Query::jsonContains('tags', 'php');
        $this->assertEquals(Method::JsonContains, $q->getMethod());
        $this->assertEquals('tags', $q->getAttribute());
    }

    public function testQueryJsonOverlapsFactory(): void
    {
        $q = Query::jsonOverlaps('tags', ['php', 'go']);
        $this->assertEquals(Method::JsonOverlaps, $q->getMethod());
    }

    public function testQueryJsonPathFactory(): void
    {
        $q = Query::jsonPath('meta', 'level', '>', 5);
        $this->assertEquals(Method::JsonPath, $q->getMethod());
        $this->assertEquals(['level', '>', 5], $q->getValues());
    }
    //  Does NOT implement VectorSearch

    public function testDoesNotImplementVectorSearch(): void
    {
        $builder = new Builder();
        $this->assertNotInstanceOf(VectorSearch::class, $builder); // @phpstan-ignore method.alreadyNarrowedType
    }
    //  Reset clears new state

    public function testResetClearsHintsAndJsonSets(): void
    {
        $builder = (new Builder())
            ->from('users')
            ->hint('test')
            ->setJsonAppend('tags', ['a']);

        $builder->reset();

        $result = $builder->from('users')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('/*+', $result->query);
    }

    public function testFilterNotIntersectsPoint(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterNotIntersects('zone', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Intersects', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
    }

    public function testFilterNotCrossesLinestring(): void
    {
        $result = (new Builder())
            ->from('roads')
            ->filterNotCrosses('path', [[0, 0], [1, 1]])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Crosses', $result->query);
        /** @var string $binding */
        $binding = $result->bindings[0];
        $this->assertStringContainsString('LINESTRING', $binding);
    }

    public function testFilterOverlapsPolygon(): void
    {
        $result = (new Builder())
            ->from('regions')
            ->filterOverlaps('area', [[[0, 0], [1, 0], [1, 1], [0, 0]]])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Overlaps', $result->query);
        /** @var string $binding */
        $binding = $result->bindings[0];
        $this->assertStringContainsString('POLYGON', $binding);
    }

    public function testFilterNotOverlaps(): void
    {
        $result = (new Builder())
            ->from('regions')
            ->filterNotOverlaps('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Overlaps', $result->query);
    }

    public function testFilterTouches(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterTouches('zone', [5.0, 10.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Touches', $result->query);
    }

    public function testFilterNotTouches(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterNotTouches('zone', [5.0, 10.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Touches', $result->query);
    }

    public function testFilterNotCovers(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterNotCovers('region', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Contains', $result->query);
    }

    public function testFilterNotSpatialEquals(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterNotSpatialEquals('geom', [3.0, 4.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Equals', $result->query);
    }

    public function testFilterDistanceGreaterThan(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('loc', [1.0, 2.0], '>', 500.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('> ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(500.0, $result->bindings[1]);
    }

    public function testFilterDistanceEqual(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('loc', [1.0, 2.0], '=', 0.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('= ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(0.0, $result->bindings[1]);
    }

    public function testFilterDistanceNotEqual(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('loc', [1.0, 2.0], '!=', 100.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('!= ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(100.0, $result->bindings[1]);
    }

    public function testFilterDistanceWithoutMeters(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('loc', [1.0, 2.0], '<', 50.0, false)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance(`loc`, ST_GeomFromText(?)) < ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(50.0, $result->bindings[1]);
    }

    public function testFilterIntersectsLinestring(): void
    {
        $result = (new Builder())
            ->from('roads')
            ->filterIntersects('path', [[0, 0], [1, 1], [2, 2]])
            ->build();
        $this->assertBindingCount($result);

        /** @var string $binding */
        $binding = $result->bindings[0];
        $this->assertStringContainsString('LINESTRING(0 0, 1 1, 2 2)', $binding);
    }

    public function testFilterSpatialEqualsPoint(): void
    {
        $result = (new Builder())
            ->from('places')
            ->filterSpatialEquals('pos', [42.5, -73.2])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Equals', $result->query);
        $this->assertEquals('POINT(42.5 -73.2)', $result->bindings[0]);
    }

    public function testSetJsonIntersect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonIntersect('tags', ['a', 'b'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_ARRAYAGG', $result->query);
        $this->assertStringContainsString('JSON_CONTAINS(?, val)', $result->query);
        $this->assertStringContainsString('UPDATE `t` SET', $result->query);
    }

    public function testSetJsonDiff(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonDiff('tags', ['x'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT JSON_CONTAINS(?, val)', $result->query);
        $this->assertContains(\json_encode(['x']), $result->bindings);
    }

    public function testSetJsonUnique(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonUnique('tags')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_ARRAYAGG', $result->query);
        $this->assertStringContainsString('DISTINCT', $result->query);
    }

    public function testSetJsonPrependMergeOrder(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonPrepend('items', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_MERGE_PRESERVE(?, IFNULL(', $result->query);
    }

    public function testSetJsonInsertWithIndex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->setJsonInsert('items', 2, 'value')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_ARRAY_INSERT', $result->query);
        $this->assertContains('$[2]', $result->bindings);
        $this->assertContains('value', $result->bindings);
    }

    public function testFilterJsonNotContainsCompiles(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('meta', 'admin')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT JSON_CONTAINS(`meta`, ?)', $result->query);
    }

    public function testFilterJsonOverlapsCompiles(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'js'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_OVERLAPS(`tags`, ?)', $result->query);
    }

    public function testFilterJsonPathCompiles(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age', '>=', 21)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("JSON_EXTRACT(`data`, '$.age') >= ?", $result->query);
        $this->assertEquals(21, $result->bindings[0]);
    }

    public function testMultipleHintsNoIcpAndBka(): void
    {
        $result = (new Builder())
            ->from('t')
            ->hint('NO_ICP(t)')
            ->hint('BKA(t)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('/*+ NO_ICP(t) BKA(t) */', $result->query);
    }

    public function testHintWithDistinct(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->hint('SET_VAR(sort_buffer_size=16M)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT /*+', $result->query);
    }

    public function testHintPreservesBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->hint('NO_ICP(t)')
            ->filter([Query::equal('status', ['active'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active'], $result->bindings);
    }

    public function testMaxExecutionTimeValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->maxExecutionTime(5000)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('/*+ MAX_EXECUTION_TIME(5000) */', $result->query);
    }

    public function testSelectWindowWithPartitionOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('SUM(amount)', 'total', ['dept'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(amount) OVER (PARTITION BY `dept`) AS `total`', $result->query);
    }

    public function testSelectWindowWithOrderOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('ROW_NUMBER()', 'rn', null, ['created_at'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER (ORDER BY `created_at` ASC) AS `rn`', $result->query);
    }

    public function testSelectWindowNoPartitionNoOrderEmpty(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('COUNT(*)', 'cnt')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) OVER () AS `cnt`', $result->query);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('ROW_NUMBER()', 'rn', null, ['id'])
            ->selectWindow('SUM(amount)', 'running_total', null, ['id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER()', $result->query);
        $this->assertStringContainsString('SUM(amount)', $result->query);
    }

    public function testSelectWindowWithDescOrder(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectWindow('RANK()', 'r', null, ['-score'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY `score` DESC', $result->query);
    }

    public function testCaseWithMultipleWhens(): void
    {
        $case = (new CaseBuilder())
            ->when('x = ?', '?', [1], ['one'])
            ->when('x = ?', '?', [2], ['two'])
            ->when('x = ?', '?', [3], ['three'])
            ->build();

        $this->assertStringContainsString('WHEN x = ? THEN ?', $case->sql);
        $this->assertEquals([1, 'one', 2, 'two', 3, 'three'], $case->bindings);
    }

    public function testCaseExpressionWithoutElseClause(): void
    {
        $case = (new CaseBuilder())
            ->when('x > ?', '1', [10])
            ->when('x < ?', '0', [0])
            ->build();

        $this->assertStringNotContainsString('ELSE', $case->sql);
    }

    public function testCaseExpressionWithoutAliasClause(): void
    {
        $case = (new CaseBuilder())
            ->when('x = 1', "'yes'")
            ->build();

        $this->assertStringNotContainsString(' AS ', $case->sql);
    }

    public function testSetCaseInUpdate(): void
    {
        $case = (new CaseBuilder())
            ->when('age >= ?', '?', [18], ['adult'])
            ->elseResult('?', ['minor'])
            ->build();

        $result = (new Builder())
            ->from('users')
            ->setCase('status', $case)
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UPDATE', $result->query);
        $this->assertStringContainsString('CASE WHEN', $result->query);
        $this->assertStringContainsString('END', $result->query);
    }

    public function testCaseBuilderThrowsWhenNoWhensAdded(): void
    {
        $this->expectException(ValidationException::class);

        (new CaseBuilder())->build();
    }

    public function testMultipleCTEsWithTwoSources(): void
    {
        $cte1 = (new Builder())->from('orders');
        $cte2 = (new Builder())->from('returns');

        $result = (new Builder())
            ->with('a', $cte1)
            ->with('b', $cte2)
            ->from('a')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH `a` AS', $result->query);
        $this->assertStringContainsString('`b` AS', $result->query);
    }

    public function testCTEWithBindings(): void
    {
        $cte = (new Builder())->from('orders')->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->with('paid_orders', $cte)
            ->from('paid_orders')
            ->filter([Query::greaterThan('amount', 100)])
            ->build();
        $this->assertBindingCount($result);

        // CTE bindings come BEFORE main query bindings
        $this->assertEquals('paid', $result->bindings[0]);
        $this->assertEquals(100, $result->bindings[1]);
    }

    public function testCTEWithRecursiveMixed(): void
    {
        $cte1 = (new Builder())->from('products');
        $cte2 = (new Builder())->from('categories');

        $result = (new Builder())
            ->with('prods', $cte1)
            ->withRecursive('tree', $cte2)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('WITH RECURSIVE', $result->query);
        $this->assertStringContainsString('`prods` AS', $result->query);
        $this->assertStringContainsString('`tree` AS', $result->query);
    }

    public function testCTEResetClearedAfterBuild(): void
    {
        $cte = (new Builder())->from('orders');
        $builder = (new Builder())
            ->with('o', $cte)
            ->from('o');

        $builder->reset();

        $result = $builder->from('users')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('WITH', $result->query);
    }

    public function testInsertSelectWithFilter(): void
    {
        $source = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->filter([Query::equal('status', ['active'])]);

        $result = (new Builder())
            ->into('archive')
            ->fromSelect(['name', 'email'], $source)
            ->insertSelect();

        $this->assertStringContainsString('INSERT INTO `archive`', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testInsertSelectThrowsWithoutSource(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('archive')
            ->insertSelect();
    }

    public function testInsertSelectThrowsWithoutColumns(): void
    {
        $this->expectException(ValidationException::class);

        $source = (new Builder())->from('users');

        (new Builder())
            ->into('archive')
            ->fromSelect([], $source)
            ->insertSelect();
    }

    public function testInsertSelectMultipleColumns(): void
    {
        $source = (new Builder())
            ->from('users')
            ->select(['name', 'email', 'age']);

        $result = (new Builder())
            ->into('archive')
            ->fromSelect(['name', 'email', 'age'], $source)
            ->insertSelect();

        $this->assertStringContainsString('`name`', $result->query);
        $this->assertStringContainsString('`email`', $result->query);
        $this->assertStringContainsString('`age`', $result->query);
    }

    public function testUnionAllCompiles(): void
    {
        $other = (new Builder())->from('archive');
        $result = (new Builder())
            ->from('current')
            ->unionAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION ALL', $result->query);
    }

    public function testIntersectCompiles(): void
    {
        $other = (new Builder())->from('admins');
        $result = (new Builder())
            ->from('users')
            ->intersect($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INTERSECT', $result->query);
    }

    public function testIntersectAllCompiles(): void
    {
        $other = (new Builder())->from('admins');
        $result = (new Builder())
            ->from('users')
            ->intersectAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INTERSECT ALL', $result->query);
    }

    public function testExceptCompiles(): void
    {
        $other = (new Builder())->from('banned');
        $result = (new Builder())
            ->from('users')
            ->except($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXCEPT', $result->query);
    }

    public function testExceptAllCompiles(): void
    {
        $other = (new Builder())->from('banned');
        $result = (new Builder())
            ->from('users')
            ->exceptAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXCEPT ALL', $result->query);
    }

    public function testUnionWithBindings(): void
    {
        $other = (new Builder())->from('admins')->filter([Query::equal('role', ['admin'])]);
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['active', 'admin'], $result->bindings);
    }

    public function testPageThreeWithTen(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(3, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([10, 20], $result->bindings);
    }

    public function testPageFirstPage(): void
    {
        $result = (new Builder())
            ->from('t')
            ->page(1, 25)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([25, 0], $result->bindings);
    }

    public function testCursorAfterWithSort(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('id')
            ->cursorAfter(5)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`_cursor` > ?', $result->query);
        $this->assertContains(5, $result->bindings);
        $this->assertContains(10, $result->bindings);
    }

    public function testCursorBeforeWithSort(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('id')
            ->cursorBefore(5)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`_cursor` < ?', $result->query);
        $this->assertContains(5, $result->bindings);
        $this->assertContains(10, $result->bindings);
    }

    public function testToRawSqlWithStrings(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::equal('name', ['Alice'])])
            ->toRawSql();

        $this->assertStringContainsString("'Alice'", $sql);
    }

    public function testToRawSqlWithIntegers(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('age', 30)])
            ->toRawSql();

        $this->assertStringContainsString('30', $sql);
        $this->assertStringNotContainsString("'30'", $sql);
    }

    public function testToRawSqlWithNullValue(): void
    {
        $sql = (new Builder())
            ->from('t')
            ->filter([Query::raw('deleted_at = ?', [null])])
            ->toRawSql();

        $this->assertStringContainsString('NULL', $sql);
    }

    public function testToRawSqlWithBooleans(): void
    {
        $sqlTrue = (new Builder())
            ->from('t')
            ->filter([Query::raw('active = ?', [true])])
            ->toRawSql();

        $sqlFalse = (new Builder())
            ->from('t')
            ->filter([Query::raw('active = ?', [false])])
            ->toRawSql();

        $this->assertStringContainsString('= 1', $sqlTrue);
        $this->assertStringContainsString('= 0', $sqlFalse);
    }

    public function testWhenTrueAppliesLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(true, fn (Builder $b) => $b->limit(5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT', $result->query);
    }

    public function testWhenFalseSkipsLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->when(false, fn (Builder $b) => $b->limit(5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('LIMIT', $result->query);
    }

    public function testBuildWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->build();
    }

    public function testInsertWithoutRowsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->into('t')->insert();
    }

    public function testInsertWithEmptyRowThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->into('t')->set([])->insert();
    }

    public function testUpdateWithoutAssignmentsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->from('t')->update();
    }

    public function testUpsertWithoutConflictKeysThrowsValidation(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('t')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->upsert();
    }

    public function testBatchInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('t')
            ->set(['a' => 1, 'b' => 2])
            ->set(['a' => 3, 'b' => 4])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('VALUES (?, ?), (?, ?)', $result->query);
        $this->assertEquals([1, 2, 3, 4], $result->bindings);
    }

    public function testBatchInsertMismatchedColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('t')
            ->set(['a' => 1, 'b' => 2])
            ->set(['a' => 3, 'c' => 4])
            ->insert();
    }

    public function testEmptyColumnNameThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('t')
            ->set(['' => 'val'])
            ->insert();
    }

    public function testSearchNotCompiles(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('body', 'spam')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT (MATCH(`body`) AGAINST(?))', $result->query);
    }

    public function testRegexpCompiles(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^test')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`slug` REGEXP ?', $result->query);
    }

    public function testUpsertUsesOnDuplicateKey(): void
    {
        $result = (new Builder())
            ->into('t')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], ['name'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON DUPLICATE KEY UPDATE', $result->query);
    }

    public function testForUpdateCompiles(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringEndsWith('FOR UPDATE', $result->query);
    }

    public function testForShareCompiles(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->forShare()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringEndsWith('FOR SHARE', $result->query);
    }

    public function testForUpdateWithFilters(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->filter([Query::equal('id', [1])])
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringEndsWith('FOR UPDATE', $result->query);
    }

    public function testBeginTransaction(): void
    {
        $result = (new Builder())->begin();
        $this->assertEquals('BEGIN', $result->query);
    }

    public function testCommitTransaction(): void
    {
        $result = (new Builder())->commit();
        $this->assertEquals('COMMIT', $result->query);
    }

    public function testRollbackTransaction(): void
    {
        $result = (new Builder())->rollback();
        $this->assertEquals('ROLLBACK', $result->query);
    }

    public function testReleaseSavepointCompiles(): void
    {
        $result = (new Builder())->releaseSavepoint('sp1');
        $this->assertEquals('RELEASE SAVEPOINT `sp1`', $result->query);
    }

    public function testResetClearsCTEs(): void
    {
        $cte = (new Builder())->from('orders');
        $builder = (new Builder())
            ->with('o', $cte)
            ->from('o');

        $builder->reset();

        $result = $builder->from('items')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('WITH', $result->query);
    }

    public function testResetClearsUnionsComprehensive(): void
    {
        $other = (new Builder())->from('archive');
        $builder = (new Builder())
            ->from('current')
            ->union($other);

        $builder->reset();

        $result = $builder->from('items')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('UNION', $result->query);
    }

    public function testGroupByWithHavingCount(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->count('*', 'cnt')
            ->groupBy(['dept'])
            ->having([Query::and([Query::greaterThan('COUNT(*)', 5)])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY', $result->query);
        $this->assertStringContainsString('HAVING', $result->query);
    }

    public function testGroupByMultipleColumnsAB(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'total')
            ->groupBy(['a', 'b'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY `a`, `b`', $result->query);
    }

    public function testEqualEmptyArrayReturnsFalse(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 0', $result->query);
    }

    public function testEqualWithNullOnlyCompileIn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` IS NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [1, null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`x` IN (?) OR `x` IS NULL)', $result->query);
        $this->assertEquals([1], $result->bindings);
    }

    public function testEqualMultipleValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [1, 2, 3])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` IN (?, ?, ?)', $result->query);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testNotEqualEmptyArrayReturnsTrue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', [])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 1', $result->query);
    }

    public function testNotEqualSingleValue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` != ?', $result->query);
        $this->assertEquals([5], $result->bindings);
    }

    public function testNotEqualWithNullOnlyCompileNotIn(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', [null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` IS NOT NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testNotEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', [1, null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`x` != ? AND `x` IS NOT NULL)', $result->query);
        $this->assertEquals([1], $result->bindings);
    }

    public function testNotEqualMultipleValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', [1, 2, 3])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` NOT IN (?, ?, ?)', $result->query);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testNotEqualSingleNonNull(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', 42)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` != ?', $result->query);
        $this->assertEquals([42], $result->bindings);
    }

    public function testBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testNotBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notBetween('score', 0, 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`score` NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([0, 50], $result->bindings);
    }

    public function testBetweenWithStrings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('date', '2024-01-01', '2024-12-31')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`date` BETWEEN ? AND ?', $result->query);
        $this->assertEquals(['2024-01-01', '2024-12-31'], $result->bindings);
    }

    public function testAndWithTwoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::and([Query::greaterThan('age', 18), Query::lessThan('age', 65)])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`age` > ? AND `age` < ?)', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testOrWithTwoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::or([Query::equal('role', ['admin']), Query::equal('role', ['mod'])])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`role` IN (?) OR `role` IN (?))', $result->query);
        $this->assertEquals(['admin', 'mod'], $result->bindings);
    }

    public function testNestedAndInsideOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::or([
                    Query::and([Query::greaterThan('a', 1), Query::lessThan('b', 2)]),
                    Query::equal('c', [3]),
                ]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('((`a` > ? AND `b` < ?) OR `c` IN (?))', $result->query);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testEmptyAndReturnsTrue(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::and([])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 1', $result->query);
    }

    public function testEmptyOrReturnsFalse(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::or([])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 0', $result->query);
    }

    public function testExistsSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`name` IS NOT NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testExistsMultipleAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name', 'email'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`name` IS NOT NULL AND `email` IS NOT NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testNotExistsSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists('name')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`name` IS NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testNotExistsMultipleAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['a', 'b'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`a` IS NULL AND `b` IS NULL)', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testRawFilterWithSql(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('score > ?', [10])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('score > ?', $result->query);
        $this->assertContains(10, $result->bindings);
    }

    public function testRawFilterWithoutBindings(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('active = 1')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('active = 1', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testRawFilterEmpty(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('1 = 1', $result->query);
    }

    public function testStartsWithEscapesPercent(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', '100%')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['100\\%%'], $result->bindings);
    }

    public function testStartsWithEscapesUnderscore(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', 'a_b')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['a\\_b%'], $result->bindings);
    }

    public function testStartsWithEscapesBackslash(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::startsWith('name', 'path\\')])
            ->build();
        $this->assertBindingCount($result);

        /** @var string $binding */
        $binding = $result->bindings[0];
        $this->assertStringContainsString('\\\\', $binding);
    }

    public function testEndsWithEscapesSpecialChars(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('name', '%test_')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['%\\%test\\_'], $result->bindings);
    }

    public function testContainsMultipleValuesUsesOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php', 'js'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`bio` LIKE ? OR `bio` LIKE ?)', $result->query);
        $this->assertEquals(['%php%', '%js%'], $result->bindings);
    }

    public function testContainsAllUsesAnd(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('bio', ['php', 'js'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`bio` LIKE ? AND `bio` LIKE ?)', $result->query);
        $this->assertEquals(['%php%', '%js%'], $result->bindings);
    }

    public function testNotContainsMultipleValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['x', 'y'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`bio` NOT LIKE ? AND `bio` NOT LIKE ?)', $result->query);
        $this->assertEquals(['%x%', '%y%'], $result->bindings);
    }

    public function testContainsSingleValueNoParentheses(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['php'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`bio` LIKE ?', $result->query);
        $this->assertStringNotContainsString('(', $result->query);
    }

    public function testDottedIdentifierInSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select(['users.name', 'users.email'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`users`.`name`, `users`.`email`', $result->query);
    }

    public function testDottedIdentifierInFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('users.id', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`users`.`id` IN (?)', $result->query);
    }

    public function testMultipleOrderBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY `name` ASC, `age` DESC', $result->query);
    }

    public function testOrderByWithRandomAndRegular(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY', $result->query);
        $this->assertStringContainsString('`name` ASC', $result->query);
        $this->assertStringContainsString('RAND()', $result->query);
    }

    public function testDistinctWithSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT `name` FROM `t`', $result->query);
    }

    public function testDistinctWithAggregate(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->count()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT COUNT(*) FROM `t`', $result->query);
    }

    public function testSumWithAlias2(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM(`amount`) AS `total` FROM `t`', $result->query);
    }

    public function testAvgWithAlias2(): void
    {
        $result = (new Builder())
            ->from('t')
            ->avg('score', 'avg_score')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT AVG(`score`) AS `avg_score` FROM `t`', $result->query);
    }

    public function testMinWithAlias2(): void
    {
        $result = (new Builder())
            ->from('t')
            ->min('price', 'cheapest')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT MIN(`price`) AS `cheapest` FROM `t`', $result->query);
    }

    public function testMaxWithAlias2(): void
    {
        $result = (new Builder())
            ->from('t')
            ->max('price', 'priciest')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT MAX(`price`) AS `priciest` FROM `t`', $result->query);
    }

    public function testCountWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) FROM `t`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testMultipleAggregates(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT COUNT(*) AS `cnt`, SUM(`amount`) AS `total` FROM `t`', $result->query);
    }

    public function testSelectRawWithRegularSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select(['id'])
            ->selectRaw('NOW() as current_time')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `id`, NOW() as current_time FROM `t`', $result->query);
    }

    public function testSelectRawWithBindings2(): void
    {
        $result = (new Builder())
            ->from('t')
            ->selectRaw('COALESCE(?, ?) as result', ['a', 'b'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(['a', 'b'], $result->bindings);
    }

    public function testRightJoin2(): void
    {
        $result = (new Builder())
            ->from('a')
            ->rightJoin('b', 'a.id', 'b.a_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN `b` ON `a`.`id` = `b`.`a_id`', $result->query);
    }

    public function testCrossJoin2(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `b`', $result->query);
        $this->assertStringNotContainsString(' ON ', $result->query);
    }

    public function testJoinWithNonEqualOperator(): void
    {
        $result = (new Builder())
            ->from('a')
            ->join('b', 'a.id', 'b.a_id', '!=')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON `a`.`id` != `b`.`a_id`', $result->query);
    }

    public function testJoinInvalidOperatorThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('a')
            ->join('b', 'a.id', 'b.a_id', 'INVALID')
            ->build();
    }

    public function testMultipleFiltersJoinedWithAnd(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('a', [1]),
                Query::greaterThan('b', 2),
                Query::lessThan('c', 3),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE `a` IN (?) AND `b` > ? AND `c` < ?', $result->query);
        $this->assertEquals([1, 2, 3], $result->bindings);
    }

    public function testFilterWithRawCombined(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([
                Query::equal('x', [1]),
                Query::raw('y > 5'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`x` IN (?)', $result->query);
        $this->assertStringContainsString('y > 5', $result->query);
        $this->assertStringContainsString('AND', $result->query);
    }

    public function testResetClearsRawSelects2(): void
    {
        $builder = (new Builder())->from('t')->selectRaw('1 AS one');
        $builder->build();
        $builder->reset();

        $result = $builder->from('t')->build();
        $this->assertBindingCount($result);
        $this->assertEquals('SELECT * FROM `t`', $result->query);
        $this->assertStringNotContainsString('one', $result->query);
    }

    public function testAttributeHookResolvesColumn(): void
    {
        $hook = new class () implements Attribute {
            public function resolve(string $attribute): string
            {
                return match ($attribute) {
                    'alias' => 'real_column',
                    default => $attribute,
                };
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook)
            ->filter([Query::equal('alias', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`real_column`', $result->query);
        $this->assertStringNotContainsString('`alias`', $result->query);
    }

    public function testAttributeHookWithSelect(): void
    {
        $hook = new class () implements Attribute {
            public function resolve(string $attribute): string
            {
                return match ($attribute) {
                    'alias' => 'real_column',
                    default => $attribute,
                };
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook)
            ->select(['alias'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT `real_column`', $result->query);
    }

    public function testMultipleFilterHooks(): void
    {
        $hook1 = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('`tenant` = ?', ['t1']);
            }
        };

        $hook2 = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('`org` = ?', ['o1']);
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook1)
            ->addHook($hook2)
            ->filter([Query::equal('x', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`tenant` = ?', $result->query);
        $this->assertStringContainsString('`org` = ?', $result->query);
        $this->assertStringContainsString('AND', $result->query);
        $this->assertContains('t1', $result->bindings);
        $this->assertContains('o1', $result->bindings);
    }

    public function testSearchFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('body', 'hello world')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('MATCH(`body`) AGAINST(?)', $result->query);
        $this->assertContains('hello world', $result->bindings);
    }

    public function testNotSearchFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('body', 'spam')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT (MATCH(`body`) AGAINST(?))', $result->query);
        $this->assertContains('spam', $result->bindings);
    }

    public function testIsNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted_at')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`deleted_at` IS NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testIsNotNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNotNull('name')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name` IS NOT NULL', $result->query);
        $this->assertEquals([], $result->bindings);
    }

    public function testLessThanFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` < ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testLessThanEqualFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThanEqual('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` <= ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testGreaterThanFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('age', 18)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` > ?', $result->query);
        $this->assertEquals([18], $result->bindings);
    }

    public function testGreaterThanEqualFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('age', 21)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`age` >= ?', $result->query);
        $this->assertEquals([21], $result->bindings);
    }

    public function testNotStartsWithFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notStartsWith('name', 'foo')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name` NOT LIKE ?', $result->query);
        $this->assertEquals(['foo%'], $result->bindings);
    }

    public function testNotEndsWithFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEndsWith('name', 'bar')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name` NOT LIKE ?', $result->query);
        $this->assertEquals(['%bar'], $result->bindings);
    }

    public function testDeleteWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('age', 18)])
            ->sortAsc('id')
            ->limit(10)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('DELETE FROM `t`', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('ORDER BY `id` ASC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testUpdateWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->set(['status' => 'archived'])
            ->filter([Query::lessThan('age', 18)])
            ->sortAsc('id')
            ->limit(10)
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UPDATE `t` SET', $result->query);
        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('ORDER BY `id` ASC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    // Feature 1: Table Aliases

    public function testTableAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->select(['u.name', 'u.email'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT `u`.`name`, `u`.`email` FROM `users` AS `u`', $result->query);
    }

    public function testJoinAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->join('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `users` AS `u`', $result->query);
        $this->assertStringContainsString('JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`', $result->query);
    }

    public function testLeftJoinAlias(): void
    {
        $result = (new Builder())
            ->from('users')
            ->leftJoin('orders', 'users.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `orders` AS `o` ON `users`.`id` = `o`.`user_id`', $result->query);
    }

    public function testRightJoinAlias(): void
    {
        $result = (new Builder())
            ->from('users')
            ->rightJoin('orders', 'users.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN `orders` AS `o` ON `users`.`id` = `o`.`user_id`', $result->query);
    }

    public function testCrossJoinAlias(): void
    {
        $result = (new Builder())
            ->from('users')
            ->crossJoin('colors', 'c')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `colors` AS `c`', $result->query);
    }

    // Feature 2: Subqueries

    public function testFilterWhereIn(): void
    {
        $sub = (new Builder())->from('orders')->select(['user_id'])->filter([Query::greaterThan('total', 100)]);
        $result = (new Builder())
            ->from('users')
            ->filterWhereIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `total` > ?)',
            $result->query
        );
        $this->assertEquals([100], $result->bindings);
    }

    public function testFilterWhereNotIn(): void
    {
        $sub = (new Builder())->from('blacklist')->select(['user_id']);
        $result = (new Builder())
            ->from('users')
            ->filterWhereNotIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`id` NOT IN (SELECT `user_id` FROM `blacklist`)', $result->query);
    }

    public function testSelectSub(): void
    {
        $sub = (new Builder())->from('orders')->count('*', 'cnt')->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);
        $result = (new Builder())
            ->from('users')
            ->select(['name'])
            ->selectSub($sub, 'order_count')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`name`', $result->query);
        $this->assertStringContainsString('(SELECT COUNT(*) AS `cnt` FROM `orders`', $result->query);
        $this->assertStringContainsString(') AS `order_count`', $result->query);
    }

    public function testFromSub(): void
    {
        $sub = (new Builder())->from('orders')->select(['user_id'])->groupBy(['user_id']);
        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT `user_id` FROM (SELECT `user_id` FROM `orders` GROUP BY `user_id`) AS `sub`',
            $result->query
        );
    }

    // Feature 3: Raw ORDER BY / GROUP BY / HAVING

    public function testOrderByRaw(): void
    {
        $result = (new Builder())
            ->from('users')
            ->orderByRaw('FIELD(`status`, ?, ?, ?)', ['active', 'pending', 'inactive'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY FIELD(`status`, ?, ?, ?)', $result->query);
        $this->assertEquals(['active', 'pending', 'inactive'], $result->bindings);
    }

    public function testGroupByRaw(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupByRaw('YEAR(`created_at`)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY YEAR(`created_at`)', $result->query);
    }

    public function testHavingRaw(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->havingRaw('COUNT(*) > ?', [5])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING COUNT(*) > ?', $result->query);
        $this->assertContains(5, $result->bindings);
    }

    // Feature 4: countDistinct

    public function testCountDistinct(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countDistinct('user_id', 'unique_users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(DISTINCT `user_id`) AS `unique_users` FROM `orders`',
            $result->query
        );
    }

    public function testCountDistinctNoAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countDistinct('user_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(DISTINCT `user_id`) FROM `orders`',
            $result->query
        );
    }

    // Feature 5: JoinBuilder (complex JOIN ON)

    public function testJoinWhere(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id')
                    ->where('orders.status', '=', 'active');
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders` ON `users`.`id` = `orders`.`user_id` AND orders.status = ?', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testJoinWhereMultipleOns(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id')
                    ->on('users.org_id', 'orders.org_id');
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders` ON `users`.`id` = `orders`.`user_id` AND `users`.`org_id` = `orders`.`org_id`', $result->query);
    }

    public function testJoinWhereLeftJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id');
            }, JoinType::Left)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `orders` ON `users`.`id` = `orders`.`user_id`', $result->query);
    }

    public function testJoinWhereWithAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('u.id', 'o.user_id');
            }, JoinType::Inner, 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `users` AS `u`', $result->query);
        $this->assertStringContainsString('JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`', $result->query);
    }

    // Feature 6: EXISTS Subquery

    public function testFilterExists(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->filterExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT `id` FROM `orders`', $result->query);
    }

    public function testFilterNotExists(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->filterNotExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT EXISTS (SELECT `id` FROM `orders`', $result->query);
    }

    // Feature 7: insertOrIgnore

    public function testInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John', 'email' => 'john@example.com'])
            ->insertOrIgnore();

        $this->assertEquals(
            'INSERT IGNORE INTO `users` (`name`, `email`) VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['John', 'john@example.com'], $result->bindings);
    }

    // Feature 9: EXPLAIN

    public function testExplain(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
        $this->assertStringContainsString('FROM `users`', $result->query);
    }

    public function testExplainAnalyze(): void
    {
        $result = (new Builder())
            ->from('users')
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
    }

    // Feature 10: Locking Variants

    public function testForUpdateSkipLocked(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forUpdateSkipLocked()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE SKIP LOCKED', $result->query);
    }

    public function testForUpdateNoWait(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forUpdateNoWait()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE NOWAIT', $result->query);
    }

    public function testForShareSkipLocked(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forShareSkipLocked()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE SKIP LOCKED', $result->query);
    }

    public function testForShareNoWait(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forShareNoWait()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE NOWAIT', $result->query);
    }

    // Reset clears new properties

    public function testResetClearsNewProperties(): void
    {
        $builder = new Builder();
        $sub = (new Builder())->from('t')->select(['id']);

        $builder->from('users', 'u')
            ->filterWhereIn('id', $sub)
            ->selectSub($sub, 'cnt')
            ->orderByRaw('RAND()')
            ->groupByRaw('YEAR(created_at)')
            ->havingRaw('COUNT(*) > 1')
            ->countDistinct('id')
            ->filterExists($sub)
            ->reset();

        // After reset, building without setting table should throw
        $this->expectException(ValidationException::class);
        $builder->build();
    }

    // Case Builder — unit-level tests

    public function testCaseBuilderEmptyWhenThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('at least one WHEN');

        $case = new CaseBuilder();
        $case->build();
    }

    public function testCaseBuilderMultipleWhens(): void
    {
        $case = (new CaseBuilder())
            ->when('`status` = ?', '?', ['active'], ['Active'])
            ->when('`status` = ?', '?', ['inactive'], ['Inactive'])
            ->elseResult('?', ['Unknown'])
            ->alias('`label`')
            ->build();

        $this->assertEquals(
            'CASE WHEN `status` = ? THEN ? WHEN `status` = ? THEN ? ELSE ? END AS `label`',
            $case->sql
        );
        $this->assertEquals(['active', 'Active', 'inactive', 'Inactive', 'Unknown'], $case->bindings);
    }

    public function testCaseBuilderWithoutElseClause(): void
    {
        $case = (new CaseBuilder())
            ->when('`x` > ?', '1', [10])
            ->build();

        $this->assertEquals('CASE WHEN `x` > ? THEN 1 END', $case->sql);
        $this->assertEquals([10], $case->bindings);
    }

    public function testCaseBuilderWithoutAliasClause(): void
    {
        $case = (new CaseBuilder())
            ->when('1=1', '?', [], ['yes'])
            ->build();

        $this->assertStringNotContainsString(' AS ', $case->sql);
    }

    public function testCaseExpressionToSqlOutput(): void
    {
        $expr = new Expression('CASE WHEN 1 THEN 2 END', []);
        $this->assertEquals('CASE WHEN 1 THEN 2 END', $expr->sql);
        $this->assertEquals([], $expr->bindings);
    }

    // JoinBuilder — unit-level tests

    public function testJoinBuilderOnReturnsConditions(): void
    {
        $jb = new JoinBuilder();
        $jb->on('a.id', 'b.a_id')
           ->on('a.tenant', 'b.tenant', '=');

        $ons = $jb->ons;
        $this->assertCount(2, $ons);
        $this->assertEquals('a.id', $ons[0]->left);
        $this->assertEquals('b.a_id', $ons[0]->right);
        $this->assertEquals('=', $ons[0]->operator);
    }

    public function testJoinBuilderWhereAddsCondition(): void
    {
        $jb = new JoinBuilder();
        $jb->where('status', '=', 'active');

        $wheres = $jb->wheres;
        $this->assertCount(1, $wheres);
        $this->assertEquals('status = ?', $wheres[0]->expression);
        $this->assertEquals(['active'], $wheres[0]->bindings);
    }

    public function testJoinBuilderOnRaw(): void
    {
        $jb = new JoinBuilder();
        $jb->onRaw('a.created_at > NOW() - INTERVAL ? DAY', [30]);

        $wheres = $jb->wheres;
        $this->assertCount(1, $wheres);
        $this->assertEquals([30], $wheres[0]->bindings);
    }

    public function testJoinBuilderWhereRaw(): void
    {
        $jb = new JoinBuilder();
        $jb->whereRaw('`deleted_at` IS NULL');

        $wheres = $jb->wheres;
        $this->assertCount(1, $wheres);
        $this->assertEquals('`deleted_at` IS NULL', $wheres[0]->expression);
        $this->assertEquals([], $wheres[0]->bindings);
    }

    public function testJoinBuilderCombinedOnAndWhere(): void
    {
        $jb = new JoinBuilder();
        $jb->on('a.id', 'b.a_id')
           ->where('b.active', '=', true)
           ->onRaw('b.score > ?', [50]);

        $this->assertCount(1, $jb->ons);
        $this->assertCount(2, $jb->wheres);
    }

    // Subquery binding order

    public function testSubqueryBindingOrderIsCorrect(): void
    {
        $sub = (new Builder())->from('orders')
            ->select(['user_id'])
            ->filter([Query::equal('status', ['completed'])]);

        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('role', ['admin'])])
            ->filterWhereIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        // Main filter bindings come before subquery bindings
        $this->assertEquals(['admin', 'completed'], $result->bindings);
    }

    public function testSelectSubBindingOrder(): void
    {
        $sub = (new Builder())->from('orders')
            ->selectRaw('COUNT(*)')
            ->filter([Query::equal('orders.user_id', ['matched'])]);

        $result = (new Builder())
            ->from('users')
            ->selectSub($sub, 'order_count')
            ->filter([Query::equal('active', [true])])
            ->build();
        $this->assertBindingCount($result);

        // Sub-select bindings come before main WHERE bindings
        $this->assertEquals(['matched', true], $result->bindings);
    }

    public function testFromSubBindingOrder(): void
    {
        $sub = (new Builder())->from('orders')
            ->filter([Query::greaterThan('amount', 100)]);

        $result = (new Builder())
            ->fromSub($sub, 'expensive')
            ->filter([Query::equal('status', ['shipped'])])
            ->build();
        $this->assertBindingCount($result);

        // FROM sub bindings come before main WHERE bindings
        $this->assertEquals([100, 'shipped'], $result->bindings);
    }

    // EXISTS with bindings

    public function testFilterExistsBindings(): void
    {
        $sub = (new Builder())->from('orders')
            ->select(['id'])
            ->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->filterExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT', $result->query);
        $this->assertEquals([true, 'paid'], $result->bindings);
    }

    public function testFilterNotExistsQuery(): void
    {
        $sub = (new Builder())->from('bans')->select(['id']);

        $result = (new Builder())
            ->from('users')
            ->filterNotExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT EXISTS (SELECT', $result->query);
    }

    // Combined features

    public function testExplainWithFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
        $this->assertEquals([true], $result->bindings);
    }

    public function testExplainAnalyzeWithFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('active', [true])])
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
        $this->assertEquals([true], $result->bindings);
    }

    public function testTableAliasClearsOnNewFrom(): void
    {
        $builder = (new Builder())
            ->from('users', 'u');

        // Reset with new from() should clear alias
        $result = $builder->from('orders')->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `orders`', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testFromSubClearsTable(): void
    {
        $sub = (new Builder())->from('orders')->select(['id']);

        $builder = (new Builder())
            ->from('users')
            ->fromSub($sub, 'sub');

        $result = $builder->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('`users`', $result->query);
        $this->assertStringContainsString('AS `sub`', $result->query);
    }

    public function testFromClearsFromSub(): void
    {
        $sub = (new Builder())->from('orders')->select(['id']);

        $builder = (new Builder())
            ->fromSub($sub, 'sub')
            ->from('users');

        $result = $builder->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `users`', $result->query);
        $this->assertStringNotContainsString('sub', $result->query);
    }

    // Raw clauses with bindings

    public function testOrderByRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('users')
            ->orderByRaw('FIELD(`status`, ?, ?, ?)', ['active', 'pending', 'inactive'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY FIELD(`status`, ?, ?, ?)', $result->query);
        $this->assertEquals(['active', 'pending', 'inactive'], $result->bindings);
    }

    public function testGroupByRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'cnt')
            ->groupByRaw('DATE_FORMAT(`created_at`, ?)', ['%Y-%m'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("GROUP BY DATE_FORMAT(`created_at`, ?)", $result->query);
        $this->assertEquals(['%Y-%m'], $result->bindings);
    }

    public function testHavingRawWithBindings(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->havingRaw('SUM(`amount`) > ?', [1000])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING SUM(`amount`) > ?', $result->query);
        $this->assertEquals([1000], $result->bindings);
    }

    public function testMultipleRawOrdersCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('name')
            ->orderByRaw('FIELD(`role`, ?)', ['admin'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY `name` ASC, FIELD(`role`, ?)', $result->query);
    }

    public function testMultipleRawGroupsCombined(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'cnt')
            ->groupBy(['type'])
            ->groupByRaw('YEAR(`created_at`)')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY `type`, YEAR(`created_at`)', $result->query);
    }

    // countDistinct with alias and without

    public function testCountDistinctWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('users')
            ->countDistinct('email')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(DISTINCT `email`)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    // Join alias with various join types

    public function testLeftJoinWithAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->leftJoin('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `orders` AS `o`', $result->query);
    }

    public function testRightJoinWithAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->rightJoin('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN `orders` AS `o`', $result->query);
    }

    public function testCrossJoinWithAlias(): void
    {
        $result = (new Builder())
            ->from('users')
            ->crossJoin('roles', 'r')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `roles` AS `r`', $result->query);
    }

    // JoinWhere with LEFT JOIN

    public function testJoinWhereWithLeftJoinType(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id')
                     ->where('orders.status', '=', 'active');
            }, JoinType::Left)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN `orders` ON', $result->query);
        $this->assertStringContainsString('orders.status = ?', $result->query);
        $this->assertEquals(['active'], $result->bindings);
    }

    public function testJoinWhereWithTableAlias(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('u.id', 'o.user_id');
            }, JoinType::Inner, 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `orders` AS `o`', $result->query);
    }

    public function testJoinWhereWithMultipleOnConditions(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id')
                     ->on('users.tenant_id', 'orders.tenant_id');
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString(
            'ON `users`.`id` = `orders`.`user_id` AND `users`.`tenant_id` = `orders`.`tenant_id`',
            $result->query
        );
    }

    // WHERE IN subquery combined with regular filters

    public function testWhereInSubqueryWithRegularFilters(): void
    {
        $sub = (new Builder())->from('vip_users')->select(['id']);

        $result = (new Builder())
            ->from('orders')
            ->filter([
                Query::greaterThan('amount', 100),
                Query::equal('status', ['paid']),
            ])
            ->filterWhereIn('user_id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`amount` > ?', $result->query);
        $this->assertStringContainsString('`status` IN (?)', $result->query);
        $this->assertStringContainsString('`user_id` IN (SELECT', $result->query);
    }

    // Multiple subqueries

    public function testMultipleWhereInSubqueries(): void
    {
        $sub1 = (new Builder())->from('admins')->select(['id']);
        $sub2 = (new Builder())->from('departments')->select(['id']);

        $result = (new Builder())
            ->from('users')
            ->filterWhereIn('id', $sub1)
            ->filterWhereNotIn('dept_id', $sub2)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`id` IN (SELECT', $result->query);
        $this->assertStringContainsString('`dept_id` NOT IN (SELECT', $result->query);
    }

    // insertOrIgnore

    public function testInsertOrIgnoreMySQL(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John', 'email' => 'john@example.com'])
            ->insertOrIgnore();

        $this->assertStringStartsWith('INSERT IGNORE INTO', $result->query);
        $this->assertEquals(['John', 'john@example.com'], $result->bindings);
    }

    // toRawSql with various types

    public function testToRawSqlWithMixedTypes(): void
    {
        $sql = (new Builder())
            ->from('users')
            ->filter([
                Query::equal('name', ['O\'Brien']),
                Query::equal('active', [true]),
                Query::equal('age', [25]),
            ])
            ->toRawSql();

        $this->assertStringContainsString("'O''Brien'", $sql);
        $this->assertStringContainsString('1', $sql);
        $this->assertStringContainsString('25', $sql);
    }

    // page() helper

    public function testPageFirstPageOffsetZero(): void
    {
        $result = (new Builder())
            ->from('users')
            ->page(1, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertContains(10, $result->bindings);
        $this->assertContains(0, $result->bindings);
    }

    public function testPageThirdPage(): void
    {
        $result = (new Builder())
            ->from('users')
            ->page(3, 25)
            ->build();
        $this->assertBindingCount($result);

        $this->assertContains(25, $result->bindings);
        $this->assertContains(50, $result->bindings);
    }

    // when() conditional

    public function testWhenTrueAppliesCallback(): void
    {
        $result = (new Builder())
            ->from('users')
            ->when(true, fn (Builder $b) => $b->filter([Query::equal('active', [true])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE', $result->query);
    }

    public function testWhenFalseSkipsCallback(): void
    {
        $result = (new Builder())
            ->from('users')
            ->when(false, fn (Builder $b) => $b->filter([Query::equal('active', [true])]))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('WHERE', $result->query);
    }

    // Locking combined with query

    public function testLockingAppearsAtEnd(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('id', [1])])
            ->limit(1)
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringEndsWith('FOR UPDATE', $result->query);
    }

    // CTE with main query bindings

    public function testCteBindingOrder(): void
    {
        $cte = (new Builder())->from('orders')
            ->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->with('paid_orders', $cte)
            ->from('paid_orders')
            ->filter([Query::greaterThan('amount', 100)])
            ->build();
        $this->assertBindingCount($result);

        // CTE bindings come first
        $this->assertEquals(['paid', 100], $result->bindings);
    }

    public function testExactSimpleSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name', 'email'])
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name`, `email` FROM `users` WHERE `status` IN (?) ORDER BY `name` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['active', 10], $result->bindings);
    }

    public function testExactSelectWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('products')
            ->select(['id', 'name', 'price'])
            ->filter([
                Query::greaterThan('price', 10),
                Query::lessThanEqual('price', 500),
                Query::equal('category', ['electronics']),
                Query::startsWith('name', 'Pro'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name`, `price` FROM `products` WHERE `price` > ? AND `price` <= ? AND `category` IN (?) AND `name` LIKE ?',
            $result->query
        );
        $this->assertEquals([10, 500, 'electronics', 'Pro%'], $result->bindings);
    }

    public function testExactMultipleJoins(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['orders.id', 'users.name', 'products.title'])
            ->join('users', 'orders.user_id', 'users.id')
            ->leftJoin('products', 'orders.product_id', 'products.id')
            ->rightJoin('categories', 'products.category_id', 'categories.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `orders`.`id`, `users`.`name`, `products`.`title` FROM `orders` JOIN `users` ON `orders`.`user_id` = `users`.`id` LEFT JOIN `products` ON `orders`.`product_id` = `products`.`id` RIGHT JOIN `categories` ON `products`.`category_id` = `categories`.`id`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactCrossJoin(): void
    {
        $result = (new Builder())
            ->from('sizes')
            ->select(['sizes.label', 'colors.name'])
            ->crossJoin('colors')
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `sizes`.`label`, `colors`.`name` FROM `sizes` CROSS JOIN `colors`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@test.com'])
            ->set(['name' => 'Bob', 'email' => 'bob@test.com'])
            ->set(['name' => 'Charlie', 'email' => 'charlie@test.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertSame(
            'INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'alice@test.com', 'Bob', 'bob@test.com', 'Charlie', 'charlie@test.com'], $result->bindings);
    }

    public function testExactUpdateWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([Query::lessThan('last_login', '2023-06-01')])
            ->sortAsc('last_login')
            ->limit(50)
            ->update();
        $this->assertBindingCount($result);

        $this->assertSame(
            'UPDATE `users` SET `status` = ? WHERE `last_login` < ? ORDER BY `last_login` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['archived', '2023-06-01', 50], $result->bindings);
    }

    public function testExactDeleteWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('logs')
            ->filter([Query::lessThan('created_at', '2023-01-01')])
            ->sortAsc('created_at')
            ->limit(500)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertSame(
            'DELETE FROM `logs` WHERE `created_at` < ? ORDER BY `created_at` ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['2023-01-01', 500], $result->bindings);
    }

    public function testExactUpsertOnDuplicateKey(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice', 'email' => 'alice@new.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertSame(
            'INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `email` = VALUES(`email`)',
            $result->query
        );
        $this->assertEquals([1, 'Alice', 'alice@new.com'], $result->bindings);
    }

    public function testExactSubqueryWhereIn(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::greaterThan('total', 1000)]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterWhereIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `total` > ?)',
            $result->query
        );
        $this->assertEquals([1000], $result->bindings);
    }

    public function testExactExistsSubquery(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE EXISTS (SELECT `id` FROM `orders` WHERE `orders`.`user_id` = `users`.`id`)',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactCte(): void
    {
        $cte = (new Builder())
            ->from('orders')
            ->select(['user_id', 'total'])
            ->filter([Query::equal('status', ['paid'])]);

        $result = (new Builder())
            ->with('paid_orders', $cte)
            ->from('paid_orders')
            ->select(['user_id'])
            ->sum('total', 'total_spent')
            ->groupBy(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'WITH `paid_orders` AS (SELECT `user_id`, `total` FROM `orders` WHERE `status` IN (?)) SELECT SUM(`total`) AS `total_spent`, `user_id` FROM `paid_orders` GROUP BY `user_id`',
            $result->query
        );
        $this->assertEquals(['paid'], $result->bindings);
    }

    public function testExactCaseInSelect(): void
    {
        $case = (new CaseBuilder())
            ->when('status = ?', '?', ['active'], ['Active'])
            ->when('status = ?', '?', ['inactive'], ['Inactive'])
            ->elseResult('?', ['Unknown'])
            ->alias('status_label')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->selectCase($case)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name`, CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END AS status_label FROM `users`',
            $result->query
        );
        $this->assertEquals(['active', 'Active', 'inactive', 'Inactive', 'Unknown'], $result->bindings);
    }

    public function testExactAggregationGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->count('*', 'order_count')
            ->sum('total', 'total_spent')
            ->groupBy(['user_id'])
            ->having([Query::greaterThan('order_count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT COUNT(*) AS `order_count`, SUM(`total`) AS `total_spent`, `user_id` FROM `orders` GROUP BY `user_id` HAVING `order_count` > ?',
            $result->query
        );
        $this->assertEquals([5], $result->bindings);
    }

    public function testExactUnion(): void
    {
        $admins = (new Builder())
            ->from('admins')
            ->select(['id', 'name'])
            ->filter([Query::equal('role', ['admin'])]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([Query::equal('status', ['active'])])
            ->union($admins)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            '(SELECT `id`, `name` FROM `users` WHERE `status` IN (?)) UNION (SELECT `id`, `name` FROM `admins` WHERE `role` IN (?))',
            $result->query
        );
        $this->assertEquals(['active', 'admin'], $result->bindings);
    }

    public function testExactUnionAll(): void
    {
        $archive = (new Builder())
            ->from('orders_archive')
            ->select(['id', 'total', 'created_at']);

        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'total', 'created_at'])
            ->unionAll($archive)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            '(SELECT `id`, `total`, `created_at` FROM `orders`) UNION ALL (SELECT `id`, `total`, `created_at` FROM `orders_archive`)',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactWindowFunction(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'customer_id', 'total'])
            ->selectWindow('ROW_NUMBER()', 'rn', ['customer_id'], ['total'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `customer_id`, `total`, ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `total` ASC) AS `rn` FROM `orders`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactForUpdate(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->select(['id', 'balance'])
            ->filter([Query::equal('id', [42])])
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `balance` FROM `accounts` WHERE `id` IN (?) FOR UPDATE',
            $result->query
        );
        $this->assertEquals([42], $result->bindings);
    }

    public function testExactForShareSkipLocked(): void
    {
        $result = (new Builder())
            ->from('inventory')
            ->select(['id', 'quantity'])
            ->filter([Query::greaterThan('quantity', 0)])
            ->limit(5)
            ->forShareSkipLocked()
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `quantity` FROM `inventory` WHERE `quantity` > ? LIMIT ? FOR SHARE SKIP LOCKED',
            $result->query
        );
        $this->assertEquals([0, 5], $result->bindings);
    }

    public function testExactHintMaxExecutionTime(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->maxExecutionTime(5000)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT /*+ MAX_EXECUTION_TIME(5000) */ `id`, `name` FROM `users`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }

    public function testExactRawExpressions(): void
    {
        $result = (new Builder())
            ->from('users')
            ->selectRaw('COUNT(*) AS `total`')
            ->selectRaw('MAX(`created_at`) AS `latest`')
            ->filter([Query::equal('active', [true])])
            ->orderByRaw('FIELD(`role`, ?, ?, ?)', ['admin', 'editor', 'viewer'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT COUNT(*) AS `total`, MAX(`created_at`) AS `latest` FROM `users` WHERE `active` IN (?) ORDER BY FIELD(`role`, ?, ?, ?)',
            $result->query
        );
        $this->assertEquals([true, 'admin', 'editor', 'viewer'], $result->bindings);
    }

    public function testExactNestedWhereGroups(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([
                Query::and([
                    Query::equal('active', [true]),
                    Query::or([
                        Query::equal('role', ['admin']),
                        Query::greaterThan('karma', 100),
                    ]),
                ]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `id`, `name` FROM `users` WHERE (`active` IN (?) AND (`role` IN (?) OR `karma` > ?))',
            $result->query
        );
        $this->assertEquals([true, 'admin', 100], $result->bindings);
    }

    public function testExactDistinctWithOffset(): void
    {
        $result = (new Builder())
            ->from('tags')
            ->distinct()
            ->select(['name'])
            ->limit(20)
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT DISTINCT `name` FROM `tags` LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals([20, 10], $result->bindings);
    }

    public function testExactInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('tags')
            ->set(['name' => 'php', 'slug' => 'php'])
            ->set(['name' => 'mysql', 'slug' => 'mysql'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertSame(
            'INSERT IGNORE INTO `tags` (`name`, `slug`) VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['php', 'php', 'mysql', 'mysql'], $result->bindings);
    }

    public function testExactFromSubquery(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->sum('total', 'user_total')
            ->groupBy(['user_id']);

        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id', 'user_total'])
            ->filter([Query::greaterThan('user_total', 500)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `user_id`, `user_total` FROM (SELECT SUM(`total`) AS `user_total`, `user_id` FROM `orders` GROUP BY `user_id`) AS `sub` WHERE `user_total` > ?',
            $result->query
        );
        $this->assertEquals([500], $result->bindings);
    }

    public function testExactSelectSubquery(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->selectRaw('COUNT(*)')
            ->filter([Query::raw('`orders`.`user_id` = `users`.`id`')]);

        $result = (new Builder())
            ->from('users')
            ->selectSub($sub, 'order_count')
            ->select(['name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT `name`, (SELECT COUNT(*) FROM `orders` WHERE `orders`.`user_id` = `users`.`id`) AS `order_count` FROM `users`',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
    }
}
