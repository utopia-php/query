<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder\Feature\MariaDB\Returning;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;

class MariaDB extends MySQL implements Returning
{
    use Trait\MariaDB\Returning;

    /** @var list<string> */
    protected array $returningColumns = [];

    #[\Override]
    public function insert(): Plan
    {
        return $this->appendReturning(parent::insert());
    }

    #[\Override]
    public function insertOrIgnore(): Plan
    {
        return $this->appendReturning(parent::insertOrIgnore());
    }

    #[\Override]
    public function update(): Plan
    {
        return $this->appendReturning(parent::update());
    }

    #[\Override]
    public function delete(): Plan
    {
        return $this->appendReturning(parent::delete());
    }

    #[\Override]
    public function upsert(): Plan
    {
        return $this->appendReturning(parent::upsert());
    }

    #[\Override]
    public function reset(): static
    {
        parent::reset();
        $this->returningColumns = [];

        return $this;
    }

    private function appendReturning(Plan $result): Plan
    {
        if (empty($this->returningColumns)) {
            return $result;
        }

        $columns = \array_map(
            fn (string $col): string => $col === '*' ? '*' : $this->resolveAndWrap($col),
            $this->returningColumns
        );

        return new Plan(
            $result->query . ' RETURNING ' . \implode(', ', $columns),
            $result->bindings,
            executor: $this->executor,
        );
    }

    #[\Override]
    protected function compileSpatialFilter(Method $method, string $attribute, Query $query): string
    {
        if (\in_array($method, [Method::DistanceLessThan, Method::DistanceGreaterThan, Method::DistanceEqual, Method::DistanceNotEqual], true)) {
            $values = $query->getValues();
            /** @var array{0: string|array<mixed>, 1: float, 2: bool} $tuple */
            $tuple = $values[0];
            $filter = SpatialDistanceFilter::fromTuple($tuple);

            if ($filter->meters && $query->getAttributeType() !== '') {
                $wkt = \is_array($filter->geometry) ? $this->geometryToWkt($filter->geometry) : $filter->geometry;
                $pos = \strpos($wkt, '(');
                $wktType = $pos !== false ? \strtolower(\trim(\substr($wkt, 0, $pos))) : '';
                $attrType = \strtolower($query->getAttributeType());

                if ($wktType !== ColumnType::Point->value || $attrType !== ColumnType::Point->value) {
                    throw new ValidationException('Distance in meters is not supported between ' . $attrType . ' and ' . $wktType);
                }
            }
        }

        return parent::compileSpatialFilter($method, $attribute, $query);
    }

    #[\Override]
    protected function geomFromText(int $srid): string
    {
        return "ST_GeomFromText(?, {$srid})";
    }

    #[\Override]
    protected function compileSpatialDistance(Method $method, string $attribute, array $values): string
    {
        /** @var array{0: string|array<mixed>, 1: float, 2: bool} $tuple */
        $tuple = $values[0];
        $filter = SpatialDistanceFilter::fromTuple($tuple);
        $wkt = \is_array($filter->geometry) ? $this->geometryToWkt($filter->geometry) : $filter->geometry;

        $operator = match ($method) {
            Method::DistanceLessThan => '<',
            Method::DistanceGreaterThan => '>',
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            default => '<',
        };

        $this->addBinding($wkt);
        $this->addBinding($filter->distance);

        if ($filter->meters) {
            return 'ST_DISTANCE_SPHERE(' . $attribute . ', ST_GeomFromText(?, 4326)) ' . $operator . ' ?';
        }

        return 'ST_Distance(' . $attribute . ', ST_GeomFromText(?, 4326)) ' . $operator . ' ?';
    }
}
