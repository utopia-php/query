<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\BitwiseAggregates;
use Utopia\Query\Builder\Feature\FullTextSearch;
use Utopia\Query\Builder\Feature\Locking;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Builder\Feature\StatisticalAggregates;
use Utopia\Query\Builder\Feature\Transactions;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\QuotesIdentifiers;
use Utopia\Query\Schema\ColumnType;

abstract class SQL extends BaseBuilder implements Locking, Transactions, Upsert, Spatial, FullTextSearch, StatisticalAggregates, BitwiseAggregates
{
    use QuotesIdentifiers;
    use Trait\BitwiseAggregates;
    use Trait\FullTextSearch;
    use Trait\Json;
    use Trait\Locking;
    use Trait\Spatial;
    use Trait\StatisticalAggregates;
    use Trait\Transactions;
    use Trait\Upsert;

    /** @var array<string, Condition> */
    protected array $jsonSets = [];

    abstract protected function compileConflictClause(): string;

    abstract public function insertOrIgnore(): Statement;

    #[\Override]
    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAndWrap($query->getAttribute());

        if ($method === Method::Search) {
            return $this->compileSearchExpr($attribute, $query->getValues(), false);
        }

        if ($method === Method::NotSearch) {
            return $this->compileSearchExpr($attribute, $query->getValues(), true);
        }

        if ($method->isSpatial()) {
            return $this->compileSpatialFilter($method, $attribute, $query);
        }

        $attrType = $query->getAttributeType();
        $isSpatialAttr = \in_array($attrType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true);
        if ($isSpatialAttr) {
            $spatialMethod = match ($method) {
                Method::Equal => Method::SpatialEquals,
                Method::NotEqual => Method::NotSpatialEquals,
                Method::Contains => Method::Covers,
                Method::NotContains => Method::NotCovers,
                default => null,
            };
            if ($spatialMethod !== null) {
                return $this->compileSpatialFilter($spatialMethod, $attribute, $query);
            }
        }

        if ($method->isJson()) {
            return $this->compileJsonFilter($method, $attribute, $query);
        }

        if ($query->onArray() && \in_array($method, [Method::Contains, Method::ContainsAny, Method::NotContains, Method::ContainsAll], true)) {
            return $this->compileArrayFilter($method, $attribute, $query);
        }

        return parent::compileFilter($query);
    }

    protected function compileArrayFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::Contains,
            Method::ContainsAny => $this->compileJsonOverlapsExpr($attribute, [$values]),
            Method::NotContains => 'NOT ' . $this->compileJsonOverlapsExpr($attribute, [$values]),
            Method::ContainsAll => $this->compileJsonContainsExpr($attribute, [$values], false),
            default => parent::compileFilter($query),
        };
    }

    protected function compileSpatialFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::DistanceLessThan,
            Method::DistanceGreaterThan,
            Method::DistanceEqual,
            Method::DistanceNotEqual => $this->compileSpatialDistance($method, $attribute, $values),
            Method::Intersects => $this->compileSpatialPredicate('ST_Intersects', $attribute, $values, false),
            Method::NotIntersects => $this->compileSpatialPredicate('ST_Intersects', $attribute, $values, true),
            Method::Crosses => $this->compileSpatialPredicate('ST_Crosses', $attribute, $values, false),
            Method::NotCrosses => $this->compileSpatialPredicate('ST_Crosses', $attribute, $values, true),
            Method::Overlaps => $this->compileSpatialPredicate('ST_Overlaps', $attribute, $values, false),
            Method::NotOverlaps => $this->compileSpatialPredicate('ST_Overlaps', $attribute, $values, true),
            Method::Touches => $this->compileSpatialPredicate('ST_Touches', $attribute, $values, false),
            Method::NotTouches => $this->compileSpatialPredicate('ST_Touches', $attribute, $values, true),
            Method::Covers => $this->compileSpatialCoversPredicate($attribute, $values, false),
            Method::NotCovers => $this->compileSpatialCoversPredicate($attribute, $values, true),
            Method::SpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, false),
            Method::NotSpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, true),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialDistance(Method $method, string $attribute, array $values): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string;

    /**
     * Compile covers/not-covers spatial predicate. MySQL uses ST_Contains, PostgreSQL uses ST_Covers.
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialCoversPredicate(string $attribute, array $values, bool $not): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSearchExpr(string $attribute, array $values, bool $not): string;

    protected function compileJsonFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::JsonContains => $this->compileJsonContainsExpr($attribute, $values, false),
            Method::JsonNotContains => $this->compileJsonContainsExpr($attribute, $values, true),
            Method::JsonOverlaps => $this->compileJsonOverlapsExpr($attribute, $values),
            Method::JsonPath => $this->compileJsonPathExpr($attribute, $values),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileJsonOverlapsExpr(string $attribute, array $values): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileJsonPathExpr(string $attribute, array $values): string;
}
