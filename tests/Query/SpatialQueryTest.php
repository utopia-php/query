<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class SpatialQueryTest extends TestCase
{
    public function testDistanceEqual(): void
    {
        $query = Query::distanceEqual('location', [1.0, 2.0], 100);
        $this->assertEquals(Query::TYPE_DISTANCE_EQUAL, $query->getMethod());
        $this->assertEquals([[[1.0, 2.0], 100, false]], $query->getValues());
    }

    public function testDistanceEqualWithMeters(): void
    {
        $query = Query::distanceEqual('location', [1.0, 2.0], 100, true);
        $this->assertEquals([[[1.0, 2.0], 100, true]], $query->getValues());
    }

    public function testDistanceNotEqual(): void
    {
        $query = Query::distanceNotEqual('location', [1.0, 2.0], 100);
        $this->assertEquals(Query::TYPE_DISTANCE_NOT_EQUAL, $query->getMethod());
    }

    public function testDistanceGreaterThan(): void
    {
        $query = Query::distanceGreaterThan('location', [1.0, 2.0], 100);
        $this->assertEquals(Query::TYPE_DISTANCE_GREATER_THAN, $query->getMethod());
    }

    public function testDistanceLessThan(): void
    {
        $query = Query::distanceLessThan('location', [1.0, 2.0], 100);
        $this->assertEquals(Query::TYPE_DISTANCE_LESS_THAN, $query->getMethod());
    }

    public function testIntersects(): void
    {
        $query = Query::intersects('geo', [[0, 0], [1, 1]]);
        $this->assertEquals(Query::TYPE_INTERSECTS, $query->getMethod());
        $this->assertEquals([[[0, 0], [1, 1]]], $query->getValues());
    }

    public function testNotIntersects(): void
    {
        $query = Query::notIntersects('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_NOT_INTERSECTS, $query->getMethod());
    }

    public function testCrosses(): void
    {
        $query = Query::crosses('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_CROSSES, $query->getMethod());
    }

    public function testNotCrosses(): void
    {
        $query = Query::notCrosses('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_NOT_CROSSES, $query->getMethod());
    }

    public function testOverlaps(): void
    {
        $query = Query::overlaps('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_OVERLAPS, $query->getMethod());
    }

    public function testNotOverlaps(): void
    {
        $query = Query::notOverlaps('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_NOT_OVERLAPS, $query->getMethod());
    }

    public function testTouches(): void
    {
        $query = Query::touches('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_TOUCHES, $query->getMethod());
    }

    public function testNotTouches(): void
    {
        $query = Query::notTouches('geo', [[0, 0]]);
        $this->assertEquals(Query::TYPE_NOT_TOUCHES, $query->getMethod());
    }
}
