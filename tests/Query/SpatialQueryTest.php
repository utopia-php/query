<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Method;
use Utopia\Query\Query;

class SpatialQueryTest extends TestCase
{
    public function testDistanceEqual(): void
    {
        $query = Query::distanceEqual('location', [1.0, 2.0], 100);
        $this->assertSame(Method::DistanceEqual, $query->getMethod());
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
        $this->assertSame(Method::DistanceNotEqual, $query->getMethod());
    }

    public function testDistanceGreaterThan(): void
    {
        $query = Query::distanceGreaterThan('location', [1.0, 2.0], 100);
        $this->assertSame(Method::DistanceGreaterThan, $query->getMethod());
    }

    public function testDistanceLessThan(): void
    {
        $query = Query::distanceLessThan('location', [1.0, 2.0], 100);
        $this->assertSame(Method::DistanceLessThan, $query->getMethod());
    }

    public function testIntersects(): void
    {
        $query = Query::intersects('geo', [[0, 0], [1, 1]]);
        $this->assertSame(Method::Intersects, $query->getMethod());
        $this->assertEquals([[[0, 0], [1, 1]]], $query->getValues());
    }

    public function testNotIntersects(): void
    {
        $query = Query::notIntersects('geo', [[0, 0]]);
        $this->assertSame(Method::NotIntersects, $query->getMethod());
    }

    public function testCrosses(): void
    {
        $query = Query::crosses('geo', [[0, 0]]);
        $this->assertSame(Method::Crosses, $query->getMethod());
    }

    public function testNotCrosses(): void
    {
        $query = Query::notCrosses('geo', [[0, 0]]);
        $this->assertSame(Method::NotCrosses, $query->getMethod());
    }

    public function testOverlaps(): void
    {
        $query = Query::overlaps('geo', [[0, 0]]);
        $this->assertSame(Method::Overlaps, $query->getMethod());
    }

    public function testNotOverlaps(): void
    {
        $query = Query::notOverlaps('geo', [[0, 0]]);
        $this->assertSame(Method::NotOverlaps, $query->getMethod());
    }

    public function testTouches(): void
    {
        $query = Query::touches('geo', [[0, 0]]);
        $this->assertSame(Method::Touches, $query->getMethod());
    }

    public function testNotTouches(): void
    {
        $query = Query::notTouches('geo', [[0, 0]]);
        $this->assertSame(Method::NotTouches, $query->getMethod());
    }
}
