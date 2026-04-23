<?php

namespace Tests\Query\Builder\Feature;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\MySQL as MySQLBuilder;
use Utopia\Query\Builder\PostgreSQL as PostgreSQLBuilder;

class SpatialTest extends TestCase
{
    use AssertsBindingCount;

    public function testFilterDistanceBindsPointAndDistanceInOrder(): void
    {
        $result = (new MySQLBuilder())
            ->from('places')
            ->filterDistance('coords', [10.5, 20.25], '<', 100.0)
            ->build();

        $this->assertBindingCount($result);
        $this->assertSame([0 => 'POINT(10.5 20.25)', 1 => 100.0], $result->bindings);
    }

    public function testFilterIntersectsQuotesIdentifierForMySQL(): void
    {
        $result = (new MySQLBuilder())
            ->from('zones')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Intersects(`area`', $result->query);
    }

    public function testFilterIntersectsQuotesIdentifierForPostgreSQL(): void
    {
        $result = (new PostgreSQLBuilder())
            ->from('zones')
            ->filterIntersects('area', [1.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Intersects("area"', $result->query);
    }

    public function testFilterNotIntersectsWrapsWithNot(): void
    {
        $result = (new MySQLBuilder())
            ->from('zones')
            ->filterNotIntersects('area', [1.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('NOT ST_Intersects', $result->query);
    }

    public function testFilterCoversProducesStCoversOnPostgreSQL(): void
    {
        $result = (new PostgreSQLBuilder())
            ->from('zones')
            ->filterCovers('region', [1.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Covers(', $result->query);
    }

    public function testFilterSpatialEqualsProducesStEquals(): void
    {
        $result = (new MySQLBuilder())
            ->from('zones')
            ->filterSpatialEquals('area', [3.0, 4.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Equals(', $result->query);
    }

    public function testFilterTouchesProducesStTouches(): void
    {
        $result = (new MySQLBuilder())
            ->from('zones')
            ->filterTouches('area', [1.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertStringContainsString('ST_Touches(', $result->query);
    }

    public function testFilterCrossesLineStringBindingIsLinestringWkt(): void
    {
        $result = (new MySQLBuilder())
            ->from('paths')
            ->filterCrosses('path', [[0.0, 0.0], [1.0, 1.0]])
            ->build();

        $this->assertBindingCount($result);
        $this->assertIsString($result->bindings[0]);
        $this->assertStringContainsString('LINESTRING', $result->bindings[0]);
    }

    public function testFilterOverlapsChainedAddsAllBindings(): void
    {
        $result = (new MySQLBuilder())
            ->from('zones')
            ->filterOverlaps('a', [1.0, 1.0])
            ->filterNotOverlaps('b', [2.0, 2.0])
            ->build();

        $this->assertBindingCount($result);
        $this->assertCount(2, $result->bindings);
        $this->assertSame('POINT(1 1)', $result->bindings[0]);
        $this->assertSame('POINT(2 2)', $result->bindings[1]);
    }
}
