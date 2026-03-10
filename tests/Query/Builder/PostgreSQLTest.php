<?php

namespace Tests\Query\Builder;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\Case\Builder as CaseBuilder;
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
use Utopia\Query\Builder\PostgreSQL as Builder;
use Utopia\Query\Builder\VectorMetric;
use Utopia\Query\Compiler;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Query;

class PostgreSQLTest extends TestCase
{
    use AssertsBindingCount;
    public function testImplementsCompiler(): void
    {
        $this->assertInstanceOf(Compiler::class, new Builder());
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

    public function testSelectWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select(['a', 'b', 'c'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT "a", "b", "c" FROM "t"', $result->query);
    }

    public function testFromWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('my_table')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "my_table"', $result->query);
    }

    public function testFilterWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('col', [1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" WHERE "col" IN (?)', $result->query);
    }

    public function testSortWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" ORDER BY "name" ASC, "age" DESC', $result->query);
    }

    public function testJoinWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('users')
            ->join('orders', 'users.id', 'orders.uid')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM "users" JOIN "orders" ON "users"."id" = "orders"."uid"',
            $result->query
        );
    }

    public function testLeftJoinWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('users')
            ->leftJoin('profiles', 'users.id', 'profiles.uid')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM "users" LEFT JOIN "profiles" ON "users"."id" = "profiles"."uid"',
            $result->query
        );
    }

    public function testRightJoinWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('users')
            ->rightJoin('orders', 'users.id', 'orders.uid')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT * FROM "users" RIGHT JOIN "orders" ON "users"."id" = "orders"."uid"',
            $result->query
        );
    }

    public function testCrossJoinWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "a" CROSS JOIN "b"', $result->query);
    }

    public function testAggregationWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sum('price', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT SUM("price") AS "total" FROM "t"', $result->query);
    }

    public function testGroupByWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status', 'country'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(*) AS "cnt" FROM "t" GROUP BY "status", "country"',
            $result->query
        );
    }

    public function testHavingWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->groupBy(['status'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING "cnt" > ?', $result->query);
    }

    public function testDistinctWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['status'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT DISTINCT "status" FROM "t"', $result->query);
    }

    public function testIsNullWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" WHERE "deleted" IS NULL', $result->query);
    }

    public function testRandomUsesRandomFunction(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortRandom()
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" ORDER BY RANDOM()', $result->query);
    }

    public function testRegexUsesTildeOperator(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::regex('slug', '^test')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" WHERE "slug" ~ ?', $result->query);
        $this->assertEquals(['^test'], $result->bindings);
    }

    public function testSearchUsesToTsvector(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::search('body', 'hello')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" WHERE to_tsvector("body") @@ plainto_tsquery(?)', $result->query);
        $this->assertEquals(['hello'], $result->bindings);
    }

    public function testNotSearchUsesToTsvector(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notSearch('body', 'spam')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" WHERE NOT (to_tsvector("body") @@ plainto_tsquery(?))', $result->query);
        $this->assertEquals(['spam'], $result->bindings);
    }

    public function testUpsertUsesOnConflict(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO "users" ("id", "name", "email") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "email" = EXCLUDED."email"',
            $result->query
        );
        $this->assertEquals([1, 'Alice', 'alice@example.com'], $result->bindings);
    }

    public function testOffsetWithoutLimitEmitsOffset(): void
    {
        $result = (new Builder())
            ->from('t')
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" OFFSET ?', $result->query);
        $this->assertEquals([10], $result->bindings);
    }

    public function testOffsetWithLimitEmitsBoth(): void
    {
        $result = (new Builder())
            ->from('t')
            ->limit(25)
            ->offset(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('SELECT * FROM "t" LIMIT ? OFFSET ?', $result->query);
        $this->assertEquals([25, 10], $result->bindings);
    }

    public function testConditionProviderWithDoubleQuotes(): void
    {
        $hook = new class () implements Filter {
            public function filter(string $table): Condition
            {
                return new Condition('raw_condition = 1', []);
            }
        };

        $result = (new Builder())
            ->from('t')
            ->addHook($hook)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE raw_condition = 1', $result->query);
        $this->assertStringContainsString('FROM "t"', $result->query);
    }

    public function testInsertWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'INSERT INTO "users" ("name", "age") VALUES (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 30], $result->bindings);
    }

    public function testUpdateWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'Bob'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'UPDATE "users" SET "name" = ? WHERE "id" IN (?)',
            $result->query
        );
        $this->assertEquals(['Bob', 1], $result->bindings);
    }

    public function testDeleteWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('id', [1])])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'DELETE FROM "users" WHERE "id" IN (?)',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
    }

    public function testSavepointWrapsWithDoubleQuotes(): void
    {
        $result = (new Builder())->savepoint('sp1');

        $this->assertEquals('SAVEPOINT "sp1"', $result->query);
    }

    public function testForUpdateWithDoubleQuotes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE', $result->query);
        $this->assertStringContainsString('FROM "t"', $result->query);
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

        $this->assertStringContainsString('ST_Distance(("coords"::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) < ?', $result->query);
        $this->assertEquals('POINT(40.7128 -74.006)', $result->bindings[0]);
        $this->assertEquals(5000.0, $result->bindings[1]);
    }

    public function testFilterIntersectsPoint(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Intersects("area", ST_GeomFromText(?, 4326))', $result->query);
    }

    public function testFilterCovers(): void
    {
        $result = (new Builder())
            ->from('zones')
            ->filterCovers('area', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Covers("area", ST_GeomFromText(?, 4326))', $result->query);
    }

    public function testFilterCrosses(): void
    {
        $result = (new Builder())
            ->from('roads')
            ->filterCrosses('path', [[0, 0], [1, 1]])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Crosses', $result->query);
    }
    //  VectorSearch feature interface

    public function testImplementsVectorSearch(): void
    {
        $this->assertInstanceOf(VectorSearch::class, new Builder());
    }

    public function testOrderByVectorDistanceCosine(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->orderByVectorDistance('embedding', [0.1, 0.2, 0.3], VectorMetric::Cosine)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <=> ?::vector) ASC', $result->query);
        $this->assertEquals('[0.1,0.2,0.3]', $result->bindings[0]);
    }

    public function testOrderByVectorDistanceEuclidean(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->orderByVectorDistance('embedding', [1.0, 2.0], VectorMetric::Euclidean)
            ->limit(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <-> ?::vector) ASC', $result->query);
    }

    public function testOrderByVectorDistanceDot(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->orderByVectorDistance('embedding', [1.0, 2.0], VectorMetric::Dot)
            ->limit(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <#> ?::vector) ASC', $result->query);
    }

    public function testVectorFilterCosine(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->filter([Query::vectorCosine('embedding', [0.1, 0.2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <=> ?::vector)', $result->query);
    }

    public function testVectorFilterEuclidean(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->filter([Query::vectorEuclidean('embedding', [0.1, 0.2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <-> ?::vector)', $result->query);
    }

    public function testVectorFilterDot(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->filter([Query::vectorDot('embedding', [0.1, 0.2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <#> ?::vector)', $result->query);
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

        $this->assertStringContainsString('"tags" @> ?::jsonb', $result->query);
    }

    public function testFilterJsonNotContains(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('tags', 'old')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ("tags" @> ?::jsonb)', $result->query);
    }

    public function testFilterJsonOverlaps(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'go'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("\"tags\" ?| ARRAY", $result->query);
    }

    public function testFilterJsonPath(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('metadata', 'level', '>', 5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("\"metadata\"->>'level' > ?", $result->query);
        $this->assertEquals(5, $result->bindings[0]);
    }

    public function testSetJsonAppend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonAppend('tags', ['new'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('|| ?::jsonb', $result->query);
    }

    public function testSetJsonPrepend(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonPrepend('tags', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('?::jsonb ||', $result->query);
    }

    public function testSetJsonInsert(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonInsert('tags', 0, 'inserted')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('jsonb_insert', $result->query);
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

        $this->assertStringContainsString('ROW_NUMBER() OVER (PARTITION BY "customer_id" ORDER BY "created_at" ASC) AS "rn"', $result->query);
    }

    public function testSelectWindowRankDesc(): void
    {
        $result = (new Builder())
            ->from('scores')
            ->selectWindow('RANK()', 'rank', null, ['-score'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RANK() OVER (ORDER BY "score" DESC) AS "rank"', $result->query);
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
    //  Does NOT implement Hints

    public function testDoesNotImplementHints(): void
    {
        $builder = new Builder();
        $this->assertNotInstanceOf(Hints::class, $builder); // @phpstan-ignore method.alreadyNarrowedType
    }
    //  Reset clears new state

    public function testResetClearsVectorOrder(): void
    {
        $builder = (new Builder())
            ->from('embeddings')
            ->orderByVectorDistance('embedding', [0.1], VectorMetric::Cosine);

        $builder->reset();

        $result = $builder->from('embeddings')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('<=>', $result->query);
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
            ->from('maps')
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
            ->from('maps')
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

    public function testFilterCoversUsesSTCovers(): void
    {
        $result = (new Builder())
            ->from('regions')
            ->filterCovers('region', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Covers', $result->query);
        $this->assertStringNotContainsString('ST_Contains', $result->query);
    }

    public function testFilterNotCovers(): void
    {
        $result = (new Builder())
            ->from('regions')
            ->filterNotCovers('region', [1.0, 2.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ST_Covers', $result->query);
    }

    public function testFilterSpatialEquals(): void
    {
        $result = (new Builder())
            ->from('geoms')
            ->filterSpatialEquals('geom', [3.0, 4.0])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Equals', $result->query);
    }

    public function testFilterNotSpatialEquals(): void
    {
        $result = (new Builder())
            ->from('geoms')
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

        $this->assertStringContainsString('> ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(500.0, $result->bindings[1]);
    }

    public function testFilterDistanceWithoutMeters(): void
    {
        $result = (new Builder())
            ->from('locations')
            ->filterDistance('loc', [1.0, 2.0], '<', 50.0, false)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ST_Distance("loc", ST_GeomFromText(?)) < ?', $result->query);
        $this->assertEquals('POINT(1 2)', $result->bindings[0]);
        $this->assertEquals(50.0, $result->bindings[1]);
    }

    public function testVectorOrderWithExistingOrderBy(): void
    {
        $result = (new Builder())
            ->from('items')
            ->sortAsc('name')
            ->orderByVectorDistance('embedding', [0.1], VectorMetric::Cosine)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY', $result->query);
        $pos_vector = strpos($result->query, '<=>');
        $pos_name = strpos($result->query, '"name"');
        $this->assertNotFalse($pos_vector);
        $this->assertNotFalse($pos_name);
        $this->assertLessThan($pos_name, $pos_vector);
    }

    public function testVectorOrderWithLimit(): void
    {
        $result = (new Builder())
            ->from('items')
            ->orderByVectorDistance('emb', [0.1, 0.2], VectorMetric::Cosine)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY', $result->query);
        $pos_order = strpos($result->query, 'ORDER BY');
        $pos_limit = strpos($result->query, 'LIMIT');
        $this->assertNotFalse($pos_order);
        $this->assertNotFalse($pos_limit);
        $this->assertLessThan($pos_limit, $pos_order);

        // Vector JSON binding comes before limit value binding
        $vectorIdx = array_search('[0.1,0.2]', $result->bindings, true);
        $limitIdx = array_search(10, $result->bindings, true);
        $this->assertNotFalse($vectorIdx);
        $this->assertNotFalse($limitIdx);
        $this->assertLessThan($limitIdx, $vectorIdx);
    }

    public function testVectorOrderDefaultMetric(): void
    {
        $result = (new Builder())
            ->from('items')
            ->orderByVectorDistance('emb', [0.5])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('<=>', $result->query);
    }

    public function testVectorFilterCosineBindings(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->filter([Query::vectorCosine('embedding', [0.1, 0.2])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <=> ?::vector)', $result->query);
        $this->assertEquals(json_encode([0.1, 0.2]), $result->bindings[0]);
    }

    public function testVectorFilterEuclideanBindings(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->filter([Query::vectorEuclidean('embedding', [0.1])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("embedding" <-> ?::vector)', $result->query);
        $this->assertEquals(json_encode([0.1]), $result->bindings[0]);
    }

    public function testFilterJsonNotContainsAdmin(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonNotContains('meta', 'admin')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT ("meta" @> ?::jsonb)', $result->query);
    }

    public function testFilterJsonOverlapsArray(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->filterJsonOverlaps('tags', ['php', 'js'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"tags" ?| ARRAY(SELECT jsonb_array_elements_text(?::jsonb))', $result->query);
    }

    public function testFilterJsonPathComparison(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('data', 'age', '>=', 21)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("\"data\"->>'age' >= ?", $result->query);
        $this->assertEquals(21, $result->bindings[0]);
    }

    public function testFilterJsonPathEquality(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filterJsonPath('meta', 'status', '=', 'active')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString("\"meta\"->>'status' = ?", $result->query);
        $this->assertEquals('active', $result->bindings[0]);
    }

    public function testSetJsonRemove(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonRemove('tags', 'old')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"tags" - ?', $result->query);
        $this->assertContains(json_encode('old'), $result->bindings);
    }

    public function testSetJsonIntersect(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonIntersect('tags', ['a', 'b'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('jsonb_agg(elem)', $result->query);
        $this->assertStringContainsString('elem <@ ?::jsonb', $result->query);
    }

    public function testSetJsonDiff(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonDiff('tags', ['x'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT elem <@ ?::jsonb', $result->query);
    }

    public function testSetJsonUnique(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonUnique('tags')
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('jsonb_agg(DISTINCT elem)', $result->query);
    }

    public function testSetJsonAppendBindings(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonAppend('tags', ['new'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('|| ?::jsonb', $result->query);
        $this->assertContains(json_encode(['new']), $result->bindings);
    }

    public function testSetJsonPrependPutsNewArrayFirst(): void
    {
        $result = (new Builder())
            ->from('docs')
            ->setJsonPrepend('items', ['first'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('?::jsonb || COALESCE(', $result->query);
    }

    public function testMultipleCTEs(): void
    {
        $a = (new Builder())->from('x')->filter([Query::equal('status', ['active'])]);
        $b = (new Builder())->from('y')->filter([Query::equal('type', ['premium'])]);

        $result = (new Builder())
            ->with('a', $a)
            ->with('b', $b)
            ->from('a')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH "a" AS (', $result->query);
        $this->assertStringContainsString('), "b" AS (', $result->query);
    }

    public function testCTEWithRecursive(): void
    {
        $sub = (new Builder())->from('categories');

        $result = (new Builder())
            ->withRecursive('tree', $sub)
            ->from('tree')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WITH RECURSIVE', $result->query);
    }

    public function testCTEBindingOrder(): void
    {
        $cteQuery = (new Builder())->from('orders')->filter([Query::equal('status', ['shipped'])]);

        $result = (new Builder())
            ->with('shipped', $cteQuery)
            ->from('shipped')
            ->filter([Query::equal('total', [100])])
            ->build();
        $this->assertBindingCount($result);

        // CTE bindings come first
        $this->assertEquals('shipped', $result->bindings[0]);
        $this->assertEquals(100, $result->bindings[1]);
    }

    public function testInsertSelectWithFilter(): void
    {
        $source = (new Builder())
            ->from('orders')
            ->select(['customer_id', 'total'])
            ->filter([Query::greaterThan('total', 100)]);

        $result = (new Builder())
            ->into('big_orders')
            ->fromSelect(['customer_id', 'total'], $source)
            ->insertSelect();

        $this->assertStringContainsString('INSERT INTO "big_orders"', $result->query);
        $this->assertStringContainsString('SELECT', $result->query);
        $this->assertContains(100, $result->bindings);
    }

    public function testInsertSelectThrowsWithoutSource(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('target')
            ->insertSelect();
    }

    public function testUnionAll(): void
    {
        $other = (new Builder())->from('b');

        $result = (new Builder())
            ->from('a')
            ->unionAll($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UNION ALL', $result->query);
    }

    public function testIntersect(): void
    {
        $other = (new Builder())->from('b');

        $result = (new Builder())
            ->from('a')
            ->intersect($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('INTERSECT', $result->query);
    }

    public function testExcept(): void
    {
        $other = (new Builder())->from('b');

        $result = (new Builder())
            ->from('a')
            ->except($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXCEPT', $result->query);
    }

    public function testUnionWithBindingsOrder(): void
    {
        $other = (new Builder())->from('b')->filter([Query::equal('type', ['beta'])]);

        $result = (new Builder())
            ->from('a')
            ->filter([Query::equal('type', ['alpha'])])
            ->union($other)
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals('alpha', $result->bindings[0]);
        $this->assertEquals('beta', $result->bindings[1]);
    }

    public function testPage(): void
    {
        $result = (new Builder())
            ->from('items')
            ->page(3, 10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals(10, $result->bindings[0]);
        $this->assertEquals(20, $result->bindings[1]);
    }

    public function testOffsetWithoutLimitEmitsOffsetPostgres(): void
    {
        $result = (new Builder())
            ->from('items')
            ->offset(5)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('OFFSET ?', $result->query);
        $this->assertEquals([5], $result->bindings);
    }

    public function testCursorAfter(): void
    {
        $result = (new Builder())
            ->from('items')
            ->sortAsc('id')
            ->cursorAfter(5)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('> ?', $result->query);
        $this->assertContains(5, $result->bindings);
        $this->assertContains(10, $result->bindings);
    }

    public function testCursorBefore(): void
    {
        $result = (new Builder())
            ->from('items')
            ->sortAsc('id')
            ->cursorBefore(5)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('< ?', $result->query);
        $this->assertContains(5, $result->bindings);
    }

    public function testSelectWindowWithPartitionOnly(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->selectWindow('SUM("salary")', 'dept_total', ['dept'], null)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('OVER (PARTITION BY "dept")', $result->query);
    }

    public function testSelectWindowNoPartitionNoOrder(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->selectWindow('COUNT(*)', 'total', null, null)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('OVER ()', $result->query);
    }

    public function testMultipleWindowFunctions(): void
    {
        $result = (new Builder())
            ->from('scores')
            ->selectWindow('ROW_NUMBER()', 'rn', null, ['id'])
            ->selectWindow('RANK()', 'rnk', null, ['-score'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ROW_NUMBER()', $result->query);
        $this->assertStringContainsString('RANK()', $result->query);
    }

    public function testWindowFunctionWithDescOrder(): void
    {
        $result = (new Builder())
            ->from('scores')
            ->selectWindow('RANK()', 'rnk', null, ['-score'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY "score" DESC', $result->query);
    }

    public function testCaseMultipleWhens(): void
    {
        $case = (new CaseBuilder())
            ->when('status = ?', '?', ['active'], ['Active'])
            ->when('status = ?', '?', ['pending'], ['Pending'])
            ->when('status = ?', '?', ['closed'], ['Closed'])
            ->alias('label')
            ->build();

        $result = (new Builder())
            ->from('tickets')
            ->selectCase($case)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHEN status = ? THEN ?', $result->query);
        $this->assertEquals(['active', 'Active', 'pending', 'Pending', 'closed', 'Closed'], $result->bindings);
    }

    public function testCaseWithoutElse(): void
    {
        $case = (new CaseBuilder())
            ->when('active = ?', '?', [1], ['Yes'])
            ->alias('lbl')
            ->build();

        $result = (new Builder())
            ->from('users')
            ->selectCase($case)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CASE WHEN active = ? THEN ? END AS lbl', $result->query);
        $this->assertStringNotContainsString('ELSE', $result->query);
    }

    public function testSetCaseInUpdate(): void
    {
        $case = (new CaseBuilder())
            ->when('age >= ?', '?', [18], ['adult'])
            ->elseResult('?', ['minor'])
            ->build();

        $result = (new Builder())
            ->from('users')
            ->setCase('category', $case)
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UPDATE "users" SET', $result->query);
        $this->assertStringContainsString('CASE WHEN age >= ? THEN ? ELSE ? END', $result->query);
        $this->assertEquals([18, 'adult', 'minor', 1], $result->bindings);
    }

    public function testToRawSqlWithStrings(): void
    {
        $raw = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ['Alice'])])
            ->toRawSql();

        $this->assertStringContainsString("'Alice'", $raw);
        $this->assertStringNotContainsString('?', $raw);
    }

    public function testToRawSqlEscapesSingleQuotes(): void
    {
        $raw = (new Builder())
            ->from('users')
            ->filter([Query::equal('name', ["O'Brien"])])
            ->toRawSql();

        $this->assertStringContainsString("'O''Brien'", $raw);
    }

    public function testBuildWithoutTableThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->build();
    }

    public function testInsertWithoutRowsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->into('users')->insert();
    }

    public function testUpdateWithoutAssignmentsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())->from('users')->filter([Query::equal('id', [1])])->update();
    }

    public function testUpsertWithoutConflictKeysThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->upsert();
    }

    public function testBatchInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'age' => 25])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('VALUES (?, ?), (?, ?)', $result->query);
        $this->assertEquals(['Alice', 30, 'Bob', 25], $result->bindings);
    }

    public function testBatchInsertMismatchedColumnsThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'age' => 30])
            ->set(['name' => 'Bob', 'email' => 'bob@test.com'])
            ->insert();
    }

    public function testRegexUsesTildeWithCaretPattern(): void
    {
        $result = (new Builder())
            ->from('items')
            ->filter([Query::regex('s', '^t')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"s" ~ ?', $result->query);
        $this->assertEquals(['^t'], $result->bindings);
    }

    public function testSearchUsesToTsvectorWithMultipleWords(): void
    {
        $result = (new Builder())
            ->from('articles')
            ->filter([Query::search('body', 'hello world')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('to_tsvector("body") @@ plainto_tsquery(?)', $result->query);
        $this->assertEquals(['hello world'], $result->bindings);
    }

    public function testUpsertUsesOnConflictDoUpdateSet(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], ['name'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ON CONFLICT ("id") DO UPDATE SET', $result->query);
    }

    public function testUpsertConflictUpdateColumnNotInRowThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], ['nonexistent'])
            ->upsert();
    }

    public function testForUpdateLocking(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->forUpdate()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE', $result->query);
    }

    public function testForShareLocking(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->forShare()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE', $result->query);
    }

    public function testBeginCommitRollback(): void
    {
        $builder = new Builder();

        $begin = $builder->begin();
        $this->assertEquals('BEGIN', $begin->query);

        $commit = $builder->commit();
        $this->assertEquals('COMMIT', $commit->query);

        $rollback = $builder->rollback();
        $this->assertEquals('ROLLBACK', $rollback->query);
    }

    public function testSavepointDoubleQuotes(): void
    {
        $result = (new Builder())->savepoint('sp1');

        $this->assertEquals('SAVEPOINT "sp1"', $result->query);
    }

    public function testReleaseSavepointDoubleQuotes(): void
    {
        $result = (new Builder())->releaseSavepoint('sp1');

        $this->assertEquals('RELEASE SAVEPOINT "sp1"', $result->query);
    }

    public function testGroupByWithHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['customer_id'])
            ->having([Query::greaterThan('cnt', 5)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY "customer_id"', $result->query);
        $this->assertStringContainsString('HAVING "cnt" > ?', $result->query);
        $this->assertContains(5, $result->bindings);
    }

    public function testGroupByMultipleColumns(): void
    {
        $result = (new Builder())
            ->from('sales')
            ->count('*', 'cnt')
            ->groupBy(['a', 'b'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY "a", "b"', $result->query);
    }

    public function testWhenTrue(): void
    {
        $result = (new Builder())
            ->from('items')
            ->when(true, fn (Builder $b) => $b->limit(5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LIMIT ?', $result->query);
        $this->assertContains(5, $result->bindings);
    }

    public function testWhenFalse(): void
    {
        $result = (new Builder())
            ->from('items')
            ->when(false, fn (Builder $b) => $b->limit(5))
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('LIMIT', $result->query);
    }

    public function testResetClearsCTEs(): void
    {
        $sub = (new Builder())->from('orders');

        $builder = (new Builder())
            ->with('cte', $sub)
            ->from('cte');

        $builder->reset();

        $result = $builder->from('items')->build();
        $this->assertBindingCount($result);
        $this->assertStringNotContainsString('WITH', $result->query);
    }

    public function testResetClearsJsonSets(): void
    {
        $builder = (new Builder())
            ->from('docs')
            ->setJsonAppend('tags', ['new']);

        $builder->reset();

        $result = $builder
            ->from('docs')
            ->set(['name' => 'test'])
            ->filter([Query::equal('id', [1])])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringNotContainsString('jsonb', $result->query);
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

    public function testEqualWithNullOnly(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"x" IS NULL', $result->query);
    }

    public function testEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('x', [1, null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("x" IN (?) OR "x" IS NULL)', $result->query);
        $this->assertContains(1, $result->bindings);
    }

    public function testNotEqualWithNullAndValues(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notEqual('x', [1, null])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("x" != ? AND "x" IS NOT NULL)', $result->query);
    }

    public function testAndWithTwoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::and([Query::greaterThan('age', 18), Query::lessThan('age', 65)])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("age" > ? AND "age" < ?)', $result->query);
    }

    public function testOrWithTwoFilters(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::or([Query::equal('role', ['admin']), Query::equal('role', ['editor'])])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("role" IN (?) OR "role" IN (?))', $result->query);
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

    public function testBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::between('age', 18, 65)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"age" BETWEEN ? AND ?', $result->query);
        $this->assertEquals([18, 65], $result->bindings);
    }

    public function testNotBetweenFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notBetween('score', 0, 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"score" NOT BETWEEN ? AND ?', $result->query);
        $this->assertEquals([0, 50], $result->bindings);
    }

    public function testExistsSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("name" IS NOT NULL)', $result->query);
    }

    public function testExistsMultipleAttributes(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::exists(['name', 'email'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("name" IS NOT NULL AND "email" IS NOT NULL)', $result->query);
    }

    public function testNotExistsSingleAttribute(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notExists(['name'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("name" IS NULL)', $result->query);
    }

    public function testRawFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::raw('score > ?', [10])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('score > ?', $result->query);
        $this->assertContains(10, $result->bindings);
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
            ->filter([Query::startsWith('val', '100%')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"val" LIKE ?', $result->query);
        $this->assertEquals(['100\%%'], $result->bindings);
    }

    public function testEndsWithEscapesUnderscore(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::endsWith('val', 'a_b')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"val" LIKE ?', $result->query);
        $this->assertEquals(['%a\_b'], $result->bindings);
    }

    public function testContainsEscapesBackslash(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('path', ['a\\b'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"path" LIKE ?', $result->query);
        $this->assertEquals(['%a\\\\b%'], $result->bindings);
    }

    public function testContainsMultipleUsesOr(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::contains('bio', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("bio" LIKE ? OR "bio" LIKE ?)', $result->query);
    }

    public function testContainsAllUsesAnd(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::containsAll('bio', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("bio" LIKE ? AND "bio" LIKE ?)', $result->query);
    }

    public function testNotContainsMultipleUsesAnd(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::notContains('bio', ['foo', 'bar'])])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('("bio" NOT LIKE ? AND "bio" NOT LIKE ?)', $result->query);
    }

    public function testDottedIdentifier(): void
    {
        $result = (new Builder())
            ->from('t')
            ->select(['users.name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"users"."name"', $result->query);
    }

    public function testMultipleOrderBy(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sortAsc('name')
            ->sortDesc('age')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY "name" ASC, "age" DESC', $result->query);
    }

    public function testDistinctWithSelect(): void
    {
        $result = (new Builder())
            ->from('t')
            ->distinct()
            ->select(['name'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SELECT DISTINCT "name"', $result->query);
    }

    public function testSumWithAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('SUM("amount") AS "total"', $result->query);
    }

    public function testMultipleAggregates(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count('*', 'cnt')
            ->sum('amount', 'total')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*) AS "cnt"', $result->query);
        $this->assertStringContainsString('SUM("amount") AS "total"', $result->query);
    }

    public function testCountWithoutAlias(): void
    {
        $result = (new Builder())
            ->from('t')
            ->count()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(*)', $result->query);
        $this->assertStringNotContainsString(' AS ', $result->query);
    }

    public function testRightJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->rightJoin('b', 'a.id', 'b.a_id')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RIGHT JOIN "b" ON "a"."id" = "b"."a_id"', $result->query);
    }

    public function testCrossJoin(): void
    {
        $result = (new Builder())
            ->from('a')
            ->crossJoin('b')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN "b"', $result->query);
        $this->assertStringNotContainsString(' ON ', $result->query);
    }

    public function testJoinInvalidOperatorThrows(): void
    {
        $this->expectException(ValidationException::class);

        (new Builder())
            ->from('a')
            ->join('b', 'a.id', 'b.a_id', 'INVALID')
            ->build();
    }

    public function testIsNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNull('deleted_at')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"deleted_at" IS NULL', $result->query);
    }

    public function testIsNotNullFilter(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::isNotNull('name')])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"name" IS NOT NULL', $result->query);
    }

    public function testLessThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThan('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"age" < ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testLessThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::lessThanEqual('age', 30)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"age" <= ?', $result->query);
        $this->assertEquals([30], $result->bindings);
    }

    public function testGreaterThan(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThan('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"score" > ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testGreaterThanEqual(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::greaterThanEqual('score', 50)])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('"score" >= ?', $result->query);
        $this->assertEquals([50], $result->bindings);
    }

    public function testDeleteWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->filter([Query::equal('status', ['old'])])
            ->sortAsc('id')
            ->limit(100)
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('DELETE FROM "t"', $result->query);
        $this->assertStringContainsString('ORDER BY "id" ASC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testUpdateWithOrderAndLimit(): void
    {
        $result = (new Builder())
            ->from('t')
            ->set(['status' => 'archived'])
            ->filter([Query::equal('active', [false])])
            ->sortAsc('id')
            ->limit(50)
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('UPDATE "t" SET', $result->query);
        $this->assertStringContainsString('ORDER BY "id" ASC', $result->query);
        $this->assertStringContainsString('LIMIT ?', $result->query);
    }

    public function testVectorOrderBindingOrderWithFiltersAndLimit(): void
    {
        $result = (new Builder())
            ->from('items')
            ->filter([Query::equal('status', ['active'])])
            ->orderByVectorDistance('embedding', [0.1, 0.2], VectorMetric::Cosine)
            ->limit(10)
            ->build();
        $this->assertBindingCount($result);

        // Bindings should be: filter bindings, then vector json, then limit value
        $this->assertEquals('active', $result->bindings[0]);
        $vectorJson = '[0.1,0.2]';
        $vectorIdx = array_search($vectorJson, $result->bindings, true);
        $limitIdx = array_search(10, $result->bindings, true);
        $this->assertNotFalse($vectorIdx);
        $this->assertNotFalse($limitIdx);
        $this->assertLessThan($limitIdx, $vectorIdx);
    }

    // Feature 7: insertOrIgnore (PostgreSQL)

    public function testInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John', 'email' => 'john@example.com'])
            ->insertOrIgnore();

        $this->assertEquals(
            'INSERT INTO "users" ("name", "email") VALUES (?, ?) ON CONFLICT DO NOTHING',
            $result->query
        );
        $this->assertEquals(['John', 'john@example.com'], $result->bindings);
    }

    // Feature 8: RETURNING clause

    public function testInsertReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John'])
            ->returning(['id', 'name'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING "id", "name"', $result->query);
    }

    public function testInsertReturningAll(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John'])
            ->returning()
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING *', $result->query);
    }

    public function testUpdateReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'Jane'])
            ->filter([Query::equal('id', [1])])
            ->returning(['id', 'name'])
            ->update();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING "id", "name"', $result->query);
    }

    public function testDeleteReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('id', [1])])
            ->returning(['id'])
            ->delete();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING "id"', $result->query);
    }

    public function testUpsertReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'John', 'email' => 'john@example.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->returning(['id'])
            ->upsert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING "id"', $result->query);
    }

    public function testInsertOrIgnoreReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John'])
            ->returning(['id'])
            ->insertOrIgnore();

        $this->assertStringContainsString('ON CONFLICT DO NOTHING RETURNING "id"', $result->query);
    }

    // Feature 10: LockingOf (PostgreSQL only)

    public function testForUpdateOf(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forUpdateOf('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE OF "users"', $result->query);
    }

    public function testForShareOf(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forShareOf('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE OF "users"', $result->query);
    }

    // Feature 1: Table Aliases (PostgreSQL quotes)

    public function testTableAliasPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM "users" AS "u"', $result->query);
    }

    public function testJoinAliasPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->join('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN "orders" AS "o" ON "u"."id" = "o"."user_id"', $result->query);
    }

    // Feature 2: Subqueries (PostgreSQL)

    public function testFromSubPostgreSQL(): void
    {
        $sub = (new Builder())->from('orders')->select(['user_id'])->groupBy(['user_id']);
        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->select(['user_id'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT "user_id" FROM (SELECT "user_id" FROM "orders" GROUP BY "user_id") AS "sub"',
            $result->query
        );
    }

    // Feature 4: countDistinct (PostgreSQL)

    public function testCountDistinctPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->countDistinct('user_id', 'unique_users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertEquals(
            'SELECT COUNT(DISTINCT "user_id") AS "unique_users" FROM "orders"',
            $result->query
        );
    }

    // Feature 9: EXPLAIN (PostgreSQL)

    public function testExplainPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->explain();

        $this->assertStringStartsWith('EXPLAIN SELECT', $result->query);
    }

    public function testExplainAnalyzePostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->explain(true);

        $this->assertStringStartsWith('EXPLAIN ANALYZE SELECT', $result->query);
    }

    // Feature 10: Locking Variants (PostgreSQL)

    public function testForUpdateSkipLockedPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forUpdateSkipLocked()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE SKIP LOCKED', $result->query);
    }

    public function testForUpdateNoWaitPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forUpdateNoWait()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR UPDATE NOWAIT', $result->query);
    }

    // Subquery bindings (PostgreSQL)

    public function testSubqueryBindingOrderPostgreSQL(): void
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

        $this->assertEquals(['admin', 'completed'], $result->bindings);
    }

    public function testFilterNotExistsPostgreSQL(): void
    {
        $sub = (new Builder())->from('bans')->select(['id']);

        $result = (new Builder())
            ->from('users')
            ->filterNotExists($sub)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('NOT EXISTS (SELECT', $result->query);
    }

    // Raw clauses (PostgreSQL)

    public function testOrderByRawPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->orderByRaw('NULLS LAST')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('ORDER BY NULLS LAST', $result->query);
    }

    public function testGroupByRawPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('events')
            ->count('*', 'cnt')
            ->groupByRaw('date_trunc(?, "created_at")', ['month'])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('GROUP BY date_trunc(?, "created_at")', $result->query);
        $this->assertEquals(['month'], $result->bindings);
    }

    public function testHavingRawPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'cnt')
            ->groupBy(['user_id'])
            ->havingRaw('SUM("amount") > ?', [1000])
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('HAVING SUM("amount") > ?', $result->query);
    }

    // JoinWhere (PostgreSQL)

    public function testJoinWherePostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->joinWhere('orders', function (JoinBuilder $join): void {
                $join->on('users.id', 'orders.user_id')
                     ->where('orders.amount', '>', 100);
            })
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('JOIN "orders" ON "users"."id" = "orders"."user_id"', $result->query);
        $this->assertStringContainsString('orders.amount > ?', $result->query);
        $this->assertEquals([100], $result->bindings);
    }

    // Insert or ignore (PostgreSQL)

    public function testInsertOrIgnorePostgreSQL(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John'])
            ->insertOrIgnore();

        $this->assertStringContainsString('INSERT INTO', $result->query);
        $this->assertStringContainsString('ON CONFLICT DO NOTHING', $result->query);
    }

    // RETURNING with specific columns

    public function testReturningSpecificColumns(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'John'])
            ->returning(['id', 'created_at'])
            ->insert();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('RETURNING "id", "created_at"', $result->query);
    }

    // Locking OF combined

    public function testForUpdateOfWithFilter(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('id', [1])])
            ->forUpdateOf('users')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('WHERE', $result->query);
        $this->assertStringContainsString('FOR UPDATE OF "users"', $result->query);
    }

    // PostgreSQL rename uses ALTER TABLE

    public function testFromSubClearsTablePostgreSQL(): void
    {
        $sub = (new Builder())->from('orders')->select(['id']);

        $result = (new Builder())
            ->fromSub($sub, 'sub')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FROM (SELECT "id" FROM "orders") AS "sub"', $result->query);
    }

    // countDistinct without alias

    public function testCountDistinctWithoutAliasPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->countDistinct('email')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('COUNT(DISTINCT "email")', $result->query);
    }

    // Multiple EXISTS subqueries

    public function testMultipleExistsSubqueries(): void
    {
        $sub1 = (new Builder())->from('orders')->select(['id']);
        $sub2 = (new Builder())->from('payments')->select(['id']);

        $result = (new Builder())
            ->from('users')
            ->filterExists($sub1)
            ->filterNotExists($sub2)
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('EXISTS (SELECT', $result->query);
        $this->assertStringContainsString('NOT EXISTS (SELECT', $result->query);
    }

    // Left join alias PostgreSQL

    public function testLeftJoinAliasPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users', 'u')
            ->leftJoin('orders', 'u.id', 'o.user_id', '=', 'o')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('LEFT JOIN "orders" AS "o"', $result->query);
    }

    // Cross join alias PostgreSQL

    public function testCrossJoinAliasPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->crossJoin('roles', 'r')
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('CROSS JOIN "roles" AS "r"', $result->query);
    }

    // ForShare locking variants

    public function testForShareSkipLockedPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forShareSkipLocked()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE SKIP LOCKED', $result->query);
    }

    public function testForShareNoWaitPostgreSQL(): void
    {
        $result = (new Builder())
            ->from('users')
            ->forShareNoWait()
            ->build();
        $this->assertBindingCount($result);

        $this->assertStringContainsString('FOR SHARE NOWAIT', $result->query);
    }

    // Reset clears new properties (PostgreSQL)

    public function testResetPostgreSQL(): void
    {
        $sub = (new Builder())->from('t')->select(['id']);
        $builder = (new Builder())
            ->from('users', 'u')
            ->filterWhereIn('id', $sub)
            ->selectSub($sub, 'cnt')
            ->orderByRaw('random()')
            ->filterExists($sub)
            ->reset();

        $this->expectException(ValidationException::class);
        $builder->build();
    }

    public function testExactSimpleSelect(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name', 'email'])
            ->filter([Query::equal('status', ['active'])])
            ->sortAsc('name')
            ->limit(10)
            ->offset(5)
            ->build();

        $this->assertSame(
            'SELECT "id", "name", "email" FROM "users" WHERE "status" IN (?) ORDER BY "name" ASC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals(['active', 10, 5], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSelectWithMultipleFilters(): void
    {
        $result = (new Builder())
            ->from('products')
            ->select(['id', 'name', 'price'])
            ->filter([
                Query::greaterThan('price', 10),
                Query::lessThan('price', 100),
                Query::equal('category', ['electronics']),
                Query::isNotNull('name'),
            ])
            ->build();

        $this->assertSame(
            'SELECT "id", "name", "price" FROM "products" WHERE "price" > ? AND "price" < ? AND "category" IN (?) AND "name" IS NOT NULL',
            $result->query
        );
        $this->assertEquals([10, 100, 'electronics'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactMultipleJoins(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['users.id', 'orders.total', 'profiles.bio'])
            ->join('orders', 'users.id', 'orders.user_id')
            ->leftJoin('profiles', 'users.id', 'profiles.user_id')
            ->build();

        $this->assertSame(
            'SELECT "users"."id", "orders"."total", "profiles"."bio" FROM "users" JOIN "orders" ON "users"."id" = "orders"."user_id" LEFT JOIN "profiles" ON "users"."id" = "profiles"."user_id"',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactInsertMultipleRows(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@test.com'])
            ->set(['name' => 'Bob', 'email' => 'bob@test.com'])
            ->insert();

        $this->assertSame(
            'INSERT INTO "users" ("name", "email") VALUES (?, ?), (?, ?)',
            $result->query
        );
        $this->assertEquals(['Alice', 'alice@test.com', 'Bob', 'bob@test.com'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactInsertReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['name' => 'Alice', 'email' => 'alice@test.com'])
            ->returning(['id'])
            ->insert();

        $this->assertSame(
            'INSERT INTO "users" ("name", "email") VALUES (?, ?) RETURNING "id"',
            $result->query
        );
        $this->assertEquals(['Alice', 'alice@test.com'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactUpdateReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->set(['name' => 'Updated'])
            ->filter([Query::equal('id', [1])])
            ->returning(['*'])
            ->update();

        $this->assertSame(
            'UPDATE "users" SET "name" = ? WHERE "id" IN (?) RETURNING *',
            $result->query
        );
        $this->assertEquals(['Updated', 1], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDeleteReturning(): void
    {
        $result = (new Builder())
            ->from('users')
            ->filter([Query::equal('id', [5])])
            ->returning(['id'])
            ->delete();

        $this->assertSame(
            'DELETE FROM "users" WHERE "id" IN (?) RETURNING "id"',
            $result->query
        );
        $this->assertEquals([5], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactUpsertOnConflict(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice', 'email' => 'alice@test.com'])
            ->onConflict(['id'], ['name', 'email'])
            ->upsert();

        $this->assertSame(
            'INSERT INTO "users" ("id", "name", "email") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "email" = EXCLUDED."email"',
            $result->query
        );
        $this->assertEquals([1, 'Alice', 'alice@test.com'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactUpsertOnConflictReturning(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->onConflict(['id'], ['name'])
            ->returning(['id', 'name'])
            ->upsert();

        $this->assertSame(
            'INSERT INTO "users" ("id", "name") VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name" RETURNING "id", "name"',
            $result->query
        );
        $this->assertEquals([1, 'Alice'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactInsertOrIgnore(): void
    {
        $result = (new Builder())
            ->into('users')
            ->set(['id' => 1, 'name' => 'Alice'])
            ->insertOrIgnore();

        $this->assertSame(
            'INSERT INTO "users" ("id", "name") VALUES (?, ?) ON CONFLICT DO NOTHING',
            $result->query
        );
        $this->assertEquals([1, 'Alice'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactVectorSearchCosine(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->select(['id', 'title'])
            ->orderByVectorDistance('embedding', [0.1, 0.2, 0.3], VectorMetric::Cosine)
            ->limit(5)
            ->build();

        $this->assertSame(
            'SELECT "id", "title" FROM "embeddings" ORDER BY ("embedding" <=> ?::vector) ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['[0.1,0.2,0.3]', 5], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactVectorSearchEuclidean(): void
    {
        $result = (new Builder())
            ->from('embeddings')
            ->select(['id', 'title'])
            ->orderByVectorDistance('embedding', [0.5, 0.6], VectorMetric::Euclidean)
            ->limit(10)
            ->build();

        $this->assertSame(
            'SELECT "id", "title" FROM "embeddings" ORDER BY ("embedding" <-> ?::vector) ASC LIMIT ?',
            $result->query
        );
        $this->assertEquals(['[0.5,0.6]', 10], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactJsonbContains(): void
    {
        $result = (new Builder())
            ->from('documents')
            ->select(['id', 'title'])
            ->filterJsonContains('tags', 'php')
            ->build();

        $this->assertSame(
            'SELECT "id", "title" FROM "documents" WHERE "tags" @> ?::jsonb',
            $result->query
        );
        $this->assertEquals(['"php"'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactJsonbOverlaps(): void
    {
        $result = (new Builder())
            ->from('documents')
            ->filterJsonOverlaps('tags', ['php', 'js'])
            ->build();

        $this->assertSame(
            'SELECT * FROM "documents" WHERE "tags" ?| ARRAY(SELECT jsonb_array_elements_text(?::jsonb))',
            $result->query
        );
        $this->assertEquals(['["php","js"]'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactJsonPath(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterJsonPath('metadata', 'key', '=', 'value')
            ->build();

        $this->assertSame(
            'SELECT "id", "name" FROM "users" WHERE "metadata"->>\'key\' = ?',
            $result->query
        );
        $this->assertEquals(['value'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactCte(): void
    {
        $cteQuery = (new Builder())
            ->from('orders')
            ->select(['user_id', 'total'])
            ->filter([Query::greaterThan('total', 100)]);

        $result = (new Builder())
            ->with('big_orders', $cteQuery)
            ->from('big_orders')
            ->select(['user_id', 'total'])
            ->build();

        $this->assertSame(
            'WITH "big_orders" AS (SELECT "user_id", "total" FROM "orders" WHERE "total" > ?) SELECT "user_id", "total" FROM "big_orders"',
            $result->query
        );
        $this->assertEquals([100], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactWindowFunction(): void
    {
        $result = (new Builder())
            ->from('employees')
            ->select(['id', 'name', 'department'])
            ->selectWindow('ROW_NUMBER()', 'row_num', ['department'], ['-salary'])
            ->build();

        $this->assertSame(
            'SELECT "id", "name", "department", ROW_NUMBER() OVER (PARTITION BY "department" ORDER BY "salary" DESC) AS "row_num" FROM "employees"',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactUnion(): void
    {
        $second = (new Builder())
            ->from('archived_users')
            ->select(['id', 'name']);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->union($second)
            ->build();

        $this->assertSame(
            '(SELECT "id", "name" FROM "users") UNION (SELECT "id", "name" FROM "archived_users")',
            $result->query
        );
        $this->assertEquals([], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactForUpdateOf(): void
    {
        $result = (new Builder())
            ->from('accounts')
            ->select(['id', 'balance'])
            ->filter([Query::equal('id', [42])])
            ->forUpdateOf('accounts')
            ->build();

        $this->assertSame(
            'SELECT "id", "balance" FROM "accounts" WHERE "id" IN (?) FOR UPDATE OF "accounts"',
            $result->query
        );
        $this->assertEquals([42], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactForShareSkipLocked(): void
    {
        $result = (new Builder())
            ->from('jobs')
            ->select(['id', 'payload'])
            ->filter([Query::equal('status', ['pending'])])
            ->forShareSkipLocked()
            ->limit(1)
            ->build();

        $this->assertSame(
            'SELECT "id", "payload" FROM "jobs" WHERE "status" IN (?) LIMIT ? FOR SHARE SKIP LOCKED',
            $result->query
        );
        $this->assertEquals(['pending', 1], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactAggregationGroupByHaving(): void
    {
        $result = (new Builder())
            ->from('orders')
            ->count('*', 'order_count')
            ->groupBy(['user_id'])
            ->having([Query::greaterThan('order_count', 5)])
            ->build();

        $this->assertSame(
            'SELECT COUNT(*) AS "order_count" FROM "orders" GROUP BY "user_id" HAVING "order_count" > ?',
            $result->query
        );
        $this->assertEquals([5], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactSubqueryWhereIn(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['user_id'])
            ->filter([Query::greaterThan('total', 500)]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterWhereIn('id', $subquery)
            ->build();

        $this->assertSame(
            'SELECT "id", "name" FROM "users" WHERE "id" IN (SELECT "user_id" FROM "orders" WHERE "total" > ?)',
            $result->query
        );
        $this->assertEquals([500], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactExistsSubquery(): void
    {
        $subquery = (new Builder())
            ->from('orders')
            ->select(['id'])
            ->filter([Query::equal('orders.user_id', [1])]);

        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filterExists($subquery)
            ->build();

        $this->assertSame(
            'SELECT "id", "name" FROM "users" WHERE EXISTS (SELECT "id" FROM "orders" WHERE "orders"."user_id" IN (?))',
            $result->query
        );
        $this->assertEquals([1], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactNestedWhereGroups(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['id', 'name'])
            ->filter([
                Query::equal('status', ['active']),
                Query::or([
                    Query::greaterThan('age', 18),
                    Query::equal('role', ['admin']),
                ]),
            ])
            ->build();

        $this->assertSame(
            'SELECT "id", "name" FROM "users" WHERE "status" IN (?) AND ("age" > ? OR "role" IN (?))',
            $result->query
        );
        $this->assertEquals(['active', 18, 'admin'], $result->bindings);
        $this->assertBindingCount($result);
    }

    public function testExactDistinctWithOffset(): void
    {
        $result = (new Builder())
            ->from('users')
            ->select(['name', 'email'])
            ->distinct()
            ->sortAsc('name')
            ->limit(20)
            ->offset(10)
            ->build();

        $this->assertSame(
            'SELECT DISTINCT "name", "email" FROM "users" ORDER BY "name" ASC LIMIT ? OFFSET ?',
            $result->query
        );
        $this->assertEquals([20, 10], $result->bindings);
        $this->assertBindingCount($result);
    }
}
