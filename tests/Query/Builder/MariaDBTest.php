<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\LateralJoins;
use Utopia\Query\Builder\MariaDB as Builder;
use Utopia\Query\Compiler;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Query;

class MariaDBTest extends TestCase
{
    use AssertsBindingCount;

    public function testImplementsCompiler(): void
    {
        $this->assertInstanceOf(Compiler::class, new Builder());
    }

    public function testImplementsJson(): void
    {
        $this->assertInstanceOf(Json::class, new Builder());
    }

    public function testImplementsConditionalAggregates(): void
    {
        $this->assertInstanceOf(ConditionalAggregates::class, new Builder());
    }

    public function testImplementsHints(): void
    {
        $this->assertInstanceOf(Hints::class, new Builder());
    }

    public function testImplementsLateralJoins(): void
    {
        $this->assertInstanceOf(LateralJoins::class, new Builder());
    }

    public function testBasicSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t`', $result->query);
    }

    public function testSelectWithFilters(): void
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

    public function testGeomFromTextWithoutAxisOrder(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_GeomFromText(?, 4326)', $result->query);
        $this->assertStringNotContainsString('axis-order', $result->query);
    }

    public function testFilterDistanceMetersUsesDistanceSphere(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [40.7128, -74.0060], '<', 5000.0, true)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE(`coords`, ST_GeomFromText(?, 4326)) < ?', $result->query);
        $this->assertEquals('POINT(40.7128 -74.006)', $result->bindings[0]);
        $this->assertEquals(5000.0, $result->bindings[1]);
    }

    public function testFilterDistanceNoMetersUsesStDistance(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [1.0, 2.0], '>', 100.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance(`coords`, ST_GeomFromText(?, 4326)) > ?', $result->query);
        $this->assertStringNotContainsString('ST_DISTANCE_SPHERE', $result->query);
    }

    public function testSpatialDistanceLessThanMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceLessThan('attr', [0, 0], 1000, true)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
        $this->assertStringContainsString('< ?', $result->query);
    }

    public function testSpatialDistanceGreaterThanNoMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceGreaterThan('attr', [0, 0], 500, false)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('> ?', $result->query);
        $this->assertStringNotContainsString('ST_DISTANCE_SPHERE', $result->query);
    }

    public function testSpatialDistanceEqualMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceEqual('attr', [10, 20], 100, true)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
        $this->assertStringContainsString('= ?', $result->query);
    }

    public function testSpatialDistanceNotEqualNoMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceNotEqual('attr', [10, 20], 50, false)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringContainsString('!= ?', $result->query);
    }

    public function testSpatialDistanceMetersNonPointTypeThrowsValidation(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Distance in meters is not supported between');

        $query = Query::distanceLessThan('attr', [[0, 0], [1, 1], [2, 2]], 1000, true);
        $query->setAttributeType('linestring');

        (new Builder())
            ->from('t')
            ->filter([$query])
            ->build();
    }

    public function testSpatialDistanceMetersPointTypeWithPointAttribute(): void
    {
        $query = Query::distanceLessThan('attr', [10, 20], 1000, true);
        $query->setAttributeType('point');

        $result = (new Builder())
            ->from('t')
            ->filter([$query])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
    }

    public function testSpatialDistanceMetersWithEmptyAttributeTypePassesThrough(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceLessThan('attr', [0, 0], 1000, true)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
    }

    public function testSpatialDistanceMetersPolygonAttributeThrows(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Distance in meters is not supported between polygon and point');

        $query = Query::distanceLessThan('attr', [10, 20], 1000, true);
        $query->setAttributeType('polygon');

        (new Builder())
            ->from('t')
            ->filter([$query])
            ->build();
    }

    public function testSpatialDistanceNoMetersDoesNotValidateType(): void
    {
        $query = Query::distanceLessThan('attr', [[0, 0], [1, 1], [2, 2]], 1000, false);
        $query->setAttributeType('linestring');

        $result = (new Builder())
            ->from('t')
            ->filter([$query])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
    }

    public function testFilterIntersectsUsesMariaDbGeomFromText(): void
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

    public function testSpatialCrosses(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::crosses('attr', [1.0, 2.0])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Crosses', $result->query);
    }

    public function testSpatialTouches(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::touches('attr', [1.0, 2.0])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Touches', $result->query);
    }

    public function testSpatialOverlaps(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::overlaps('attr', [[0, 0], [1, 1]])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Overlaps', $result->query);
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
    }

    public function testUpsertUsesOnDuplicateKey(): void
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
    }

    public function testInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John', 'email' => 'john@example.com'])
            ->insertOrIgnore();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT IGNORE INTO `users` (`name`, `email`) VALUES (?, ?)',
            $result->query
        );
    }

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
    }

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

    public function testRegex(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^[a-z]+$')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE `slug` REGEXP ?', $result->query);
    }

    public function testSearch(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('content', 'hello')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` WHERE MATCH(`content`) AGAINST(? IN BOOLEAN MODE)', $result->query);
        $this->assertEquals(['hello*'], $result->bindings);
    }

    public function testExplain(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
    }

    public function testExplainAnalyze(): void
    {
        $result = (new Builder())
            ->from('users')
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
    }

    public function testTransactionStatements(): void
    {
        $builder = new Builder();

        $this->assertEquals('BEGIN', $builder->begin()->query);
        $this->assertEquals('COMMIT', $builder->commit()->query);
        $this->assertEquals('ROLLBACK', $builder->rollback()->query);
    }

    public function testForUpdate(): void
    {
        $result = (new Builder())
            ->from('t')
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE', $result->query);
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

    public function testFilterJsonContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonContains('meta', 'admin')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_CONTAINS(`meta`, ?)', $result->query);
    }

    public function testFilterJsonNotContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('meta', 'admin')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT JSON_CONTAINS(`meta`, ?)', $result->query);
    }

    public function testFilterJsonOverlaps(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'js'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JSON_OVERLAPS(`tags`, ?)', $result->query);
    }

    public function testFilterJsonPath(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age', '>=', 21)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("JSON_EXTRACT(`data`, '$.age') >= ?", $result->query);
        $this->assertEquals(21, $result->bindings[0]);
    }

    public function testCountWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ?', 'active_count', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `active_count`', $result->query);
    }

    public function testSumWhenWithAlias(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->sumWhen('amount', 'status = ?', 'total_active', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM(CASE WHEN status = ? THEN `amount` END) AS `total_active`', $result->query);
    }

    public function testExactSpatialDistanceMetersQuery(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [40.7128, -74.0060], '<', 5000.0, true)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT * FROM `locations` WHERE ST_DISTANCE_SPHERE(`coords`, ST_GeomFromText(?, 4326)) < ?',
            $result->query
        );
        $this->assertEquals(['POINT(40.7128 -74.006)', 5000.0], $result->bindings);
    }

    public function testExactSpatialDistanceNoMetersQuery(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('coords', [1.0, 2.0], '>', 100.0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT * FROM `locations` WHERE ST_Distance(`coords`, ST_GeomFromText(?, 4326)) > ?',
            $result->query
        );
        $this->assertEquals(['POINT(1 2)', 100.0], $result->bindings);
    }

    public function testExactIntersectsQuery(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertSame(
            'SELECT * FROM `zones` WHERE ST_Intersects(`area`, ST_GeomFromText(?, 4326))',
            $result->query
        );
        $this->assertEquals(['POINT(1 2)'], $result->bindings);
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

    public function testSpatialDistanceGreaterThanMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceGreaterThan('attr', [5, 10], 2000, true)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
        $this->assertStringContainsString('> ?', $result->query);
    }

    public function testSpatialDistanceNotEqualMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceNotEqual('attr', [5, 10], 500, true)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_DISTANCE_SPHERE', $result->query);
        $this->assertStringContainsString('!= ?', $result->query);
    }

    public function testSpatialDistanceEqualNoMeters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::distanceEqual('attr', [5, 10], 500, false)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertStringNotContainsString('ST_DISTANCE_SPHERE', $result->query);
        $this->assertStringContainsString('= ?', $result->query);
    }

    public function testSpatialDistanceWktString(): void
    {
        $query = new Query(\Utopia\Query\Method::DistanceLessThan, 'coords', [['POINT(10 20)', 500.0, false]]);

        $result = (new Builder())
            ->from('t')
            ->filter([$query])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance', $result->query);
        $this->assertContains('POINT(10 20)', $result->bindings);
    }

    public function testCteJoinWhereGroupByHavingOrderLimit(): void
    {
        $cte = (new Builder())
            ->from('raw_orders')
            ->select(['customer_id', 'amount'])
            ->filter([Query::greaterThan('amount', 0)]);

        $result = (new Builder())
            ->with('filtered_orders', $cte)
            ->from('filtered_orders')
            ->join('customers', 'filtered_orders.customer_id', 'customers.id')
            ->filter([Query::equal('customers.active', [1])])
            ->sum('filtered_orders.amount', 'total')
            ->groupBy(['customers.country'])
            ->having([Query::greaterThan('total', 100)])
            ->sortDesc('total')
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH `filtered_orders` AS', $result->query);
        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('WHERE `customers`.`active` IN (?)', $result->query);
        $this->assertStringContainsString('GROUP BY `customers`.`country`', $result->query);
        $this->assertStringContainsString('HAVING `total` > ?', $result->query);
        $this->assertStringContainsString('ORDER BY `total` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testWindowFunctionWithJoin(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->join('products', 'sales.product_id', 'products.id')
            ->selectWindow('ROW_NUMBER()', 'rn', ['products.category'], ['sales.amount'])
            ->select(['products.name', 'sales.amount'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER', $result->query);
        $this->assertStringContainsString('JOIN `products`', $result->query);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->selectWindow('ROW_NUMBER()', 'rn', ['department'], ['salary'])
            ->selectWindow('RANK()', 'rnk', ['department'], ['-salary'])
            ->select(['name', 'department', 'salary'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER() OVER', $result->query);
        $this->assertStringContainsString('RANK() OVER', $result->query);
    }

    public function testJoinAggregateHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('customers', 'orders.customer_id', 'customers.id')
            ->count('*', 'order_count')
            ->sum('orders.total', 'revenue')
            ->groupBy(['customers.country'])
            ->having([Query::greaterThan('order_count', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `order_count`', $result->query);
        $this->assertStringContainsString('HAVING `order_count` > ?', $result->query);
    }

    public function testUnionAllWithOrderLimit(): void
    {
        $archive = (new Builder())
            ->from('orders_archive')
            ->select(['id', 'total', 'created_at'])
            ->filter([Query::greaterThan('created_at', '2023-01-01')]);

        $result = (new Builder())
            ->from('orders')
            ->select(['id', 'total', 'created_at'])
            ->unionAll($archive)
            ->sortDesc('created_at')
            ->limit(50)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION ALL', $result->query);
        $this->assertStringContainsString('ORDER BY `created_at` DESC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testSubSelectWithFilter(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->select(['customer_id'])
            ->sum('total', 'total_spent')
            ->groupBy(['customer_id']);

        $result = (new Builder())
            ->from('customers')
            ->selectSub($sub, 'spending')
            ->filter([Query::equal('active', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE `active` IN (?)', $result->query);
    }

    public function testFilterWhereInSubquery(): void
    {
        $sub = (new Builder())
            ->from('premium_users')
            ->select(['id'])
            ->filter([Query::equal('tier', ['gold'])]);

        $result = (new Builder())
            ->from('orders')
            ->filterWhereIn('user_id', $sub)
            ->filter([Query::greaterThan('total', 100)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`user_id` IN (SELECT', $result->query);
        $this->assertStringContainsString('WHERE `total` > ?', $result->query);
    }

    public function testExistsSubqueryWithFilter(): void
    {
        $sub = (new Builder())
            ->from('orders')
            ->filter([Query::raw('orders.customer_id = customers.id')])
            ->filter([Query::greaterThan('total', 1000)]);

        $result = (new Builder())
            ->from('customers')
            ->filterExists($sub)
            ->filter([Query::equal('active', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT', $result->query);
        $this->assertStringContainsString('`active` IN (?)', $result->query);
    }

    public function testUpsertOnDuplicateKeyUpdate(): void
    {
        $result = (new Builder())
            ->into('counters')
            ->set(['id' => 1, 'name' => 'visits', 'count' => 1])
            ->onConflict(['id'], ['count'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON DUPLICATE KEY UPDATE `count` = VALUES(`count`)', $result->query);
    }

    public function testInsertSelectQuery(): void
    {
        $source = (new Builder())
            ->from('staging')
            ->select(['name', 'email'])
            ->filter([Query::equal('imported', [0])]);

        $result = (new Builder())
            ->into('users')
            ->fromSelect(['name', 'email'], $source)
            ->insertSelect();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INSERT INTO `users`', $result->query);
        $this->assertStringContainsString('SELECT `name`, `email` FROM `staging`', $result->query);
    }

    public function testCaseExpressionWithAggregate(): void
    {
        $case = (new \Utopia\Query\Builder\Case\Builder())
            ->when('status = ?', "'active'", ['active'])
            ->when('status = ?', "'inactive'", ['inactive'])
            ->elseResult("'other'")
            ->alias('`label`')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->selectCase($case)
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN', $result->query);
        $this->assertStringContainsString('COUNT(*) AS `cnt`', $result->query);
    }

    public function testBeforeBuildCallback(): void
    {
        $callbackCalled = false;
        $result = (new Builder())
            ->from('users')
            ->beforeBuild(function (Builder $b) use (&$callbackCalled) {
                $callbackCalled = true;
                $b->filter([Query::equal('injected', ['yes'])]);
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertTrue($callbackCalled);
        $this->assertStringContainsString('`injected` IN (?)', $result->query);
    }

    public function testAfterBuildCallback(): void
    {
        $capturedQuery = '';
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])])
            ->afterBuild(function (BuildResult $r) use (&$capturedQuery) {
                $capturedQuery = 'executed';
                return $r;
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('executed', $capturedQuery);
    }

    public function testNestedLogicalFilters(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::or([
                    Query::and([
                        Query::equal('status', ['active']),
                        Query::greaterThan('age', 18),
                    ]),
                    Query::and([
                        Query::lessThan('score', 50),
                        Query::notEqual('role', 'admin'),
                    ]),
                ]),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('(`status` IN (?) AND `age` > ?)', $result->query);
        $this->assertStringContainsString('(`score` < ? AND `role` != ?)', $result->query);
        $this->assertStringContainsString(' OR ', $result->query);
    }

    public function testTripleJoin(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->join('customers', 'orders.customer_id', 'customers.id')
            ->join('products', 'orders.product_id', 'products.id')
            ->leftJoin('categories', 'products.category_id', 'categories.id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN `customers`', $result->query);
        $this->assertStringContainsString('JOIN `products`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `categories`', $result->query);
    }

    public function testSelfJoinWithAlias(): void
    {
        $result = (new Builder())
            ->from('employees', 'e')
            ->leftJoin('employees', 'e.manager_id', 'm.id', '=', 'm')
            ->select(['e.name', 'm.name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM `employees` AS `e`', $result->query);
        $this->assertStringContainsString('LEFT JOIN `employees` AS `m`', $result->query);
    }

    public function testDistinctWithCount(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->distinct()
            ->countDistinct('customer_id', 'unique_customers')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT', $result->query);
        $this->assertStringContainsString('COUNT(DISTINCT `customer_id`)', $result->query);
    }

    public function testBindingOrderVerification(): void
    {
        $cte = (new Builder())
            ->from('raw')
            ->filter([Query::greaterThan('val', 0)]);

        $result = (new Builder())
            ->with('filtered', $cte)
            ->from('filtered')
            ->filter([Query::equal('status', ['active'])])
            ->count('*', 'cnt')
            ->groupBy(['region'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(0, $result->bindings[0]);
        $this->assertEquals('active', $result->bindings[1]);
        $this->assertEquals(5, $result->bindings[2]);
    }

    public function testCloneAndModify(): void
    {
        $original = (new Builder())
            ->from('users')
            ->filter([Query::equal('status', ['active'])]);

        $cloned = $original->clone();
        $cloned->filter([Query::greaterThan('age', 18)]);

        $origResult = $original->build();
        $clonedResult = $cloned->build();
        $this->assertBindingCount($origResult);
        $this->assertBindingCount($clonedResult);

        $this->assertStringNotContainsString('`age`', $origResult->query);
        $this->assertStringContainsString('`age` > ?', $clonedResult->query);
    }

    public function testReadOnlyFlagOnSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertTrue($result->readOnly);
    }

    public function testReadOnlyFlagOnUpdate(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertFalse($result->readOnly);
    }

    public function testMultipleSortDirections(): void
    {
        $result = (new Builder())
            ->from('users')
            ->sortAsc('last_name')
            ->sortDesc('created_at')
            ->sortAsc('first_name')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM `users` ORDER BY `last_name` ASC, `created_at` DESC, `first_name` ASC',
            $result->query
        );
    }

    public function testBooleanAndNullFilterValues(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::equal('active', [true]),
                Query::equal('deleted', [false]),
                Query::isNull('suspended_at'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals([true, false], $result->bindings);
        $this->assertStringContainsString('`suspended_at` IS NULL', $result->query);
    }

    public function testGroupByMultipleColumns(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'total')
            ->groupBy(['region', 'category', 'year'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY `region`, `category`, `year`', $result->query);
    }

    public function testWindowWithNamedDefinition(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->window('w', ['category'], ['date'])
            ->selectWindow('SUM(amount)', 'running', null, null, 'w')
            ->select(['category', 'date', 'amount'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WINDOW `w` AS', $result->query);
        $this->assertStringContainsString('OVER `w`', $result->query);
    }

    public function testInsertBatchMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'a@b.com'])
            ->set(['name' => 'Bob', 'email' => 'b@b.com'])
            ->set(['name' => 'Charlie', 'email' => 'c@b.com'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('VALUES (?, ?), (?, ?), (?, ?)', $result->query);
        $this->assertEquals(['Alice', 'a@b.com', 'Bob', 'b@b.com', 'Charlie', 'c@b.com'], $result->bindings);
    }

    public function testDeleteWithComplexFilter(): void
    {
        $result = (new Builder())
            ->from('sessions')
            ->filter([
                Query::or([
                    Query::lessThan('expires_at', '2024-01-01'),
                    Query::equal('revoked', [1]),
                ]),
            ])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringStartsWith('DELETE FROM `sessions`', $result->query);
        $this->assertStringContainsString('`expires_at` < ?', $result->query);
        $this->assertStringContainsString('`revoked` IN (?)', $result->query);
    }

    public function testCountWhenWithGroupByAndHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countWhen('status = ?', 'completed', 'completed')
            ->countWhen('status = ?', 'pending', 'pending')
            ->groupBy(['region'])
            ->having([Query::greaterThan('completed', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `completed`', $result->query);
        $this->assertStringContainsString('COUNT(CASE WHEN status = ? THEN 1 END) AS `pending`', $result->query);
        $this->assertStringContainsString('HAVING `completed` > ?', $result->query);
    }

    public function testFilterWhereNotInSubquery(): void
    {
        $sub = (new Builder())
            ->from('blocked')
            ->select(['user_id']);

        $result = (new Builder())
            ->from('users')
            ->filterWhereNotIn('id', $sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`id` NOT IN (SELECT', $result->query);
    }

    public function testFromSubqueryWithFilter(): void
    {
        $sub = (new Builder())
            ->from('events')
            ->select(['user_id'])
            ->count('*', 'event_count')
            ->groupBy(['user_id']);

        $result = (new Builder())
            ->fromSub($sub, 'user_events')
            ->filter([Query::greaterThan('event_count', 10)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM (SELECT', $result->query);
        $this->assertStringContainsString(') AS `user_events`', $result->query);
    }

    public function testLimitOneOffsetZero(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(1)
            ->offset(0)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM `t` LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([1, 0], $result->bindings);
    }

    public function testBetweenWithNotEqual(): void
    {
        $result = (new Builder())
            ->from('products')
            ->filter([
                Query::between('price', 10, 100),
                Query::notEqual('status', 'discontinued'),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`price` BETWEEN ? AND ?', $result->query);
        $this->assertStringContainsString('`status` != ?', $result->query);
    }

    public function testIsNullIsNotNullCombined(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([
                Query::isNull('deleted_at'),
                Query::isNotNull('email'),
                Query::equal('status', ['active']),
            ])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('`deleted_at` IS NULL', $result->query);
        $this->assertStringContainsString('`email` IS NOT NULL', $result->query);
        $this->assertStringContainsString('`status` IN (?)', $result->query);
    }

    public function testCrossJoin(): void
    {
        $result = (new Builder())
            ->from('users')
            ->crossJoin('config')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN `config`', $result->query);
    }

    public function testRecursiveCte(): void
    {
        $seed = (new Builder())
            ->from('categories')
            ->select(['id', 'name', 'parent_id'])
            ->filter([Query::isNull('parent_id')]);

        $step = (new Builder())
            ->from('categories')
            ->select(['categories.id', 'categories.name', 'categories.parent_id'])
            ->join('tree', 'categories.parent_id', 'tree.id');

        $result = (new Builder())
            ->withRecursiveSeedStep('tree', $seed, $step)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH RECURSIVE `tree` AS', $result->query);
        $this->assertStringContainsString('UNION ALL', $result->query);
    }
}
