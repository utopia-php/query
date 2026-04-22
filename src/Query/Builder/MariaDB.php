<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;

class MariaDB extends MySQL
{
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

    protected function geomFromText(int $srid): string
    {
        return "ST_GeomFromText(?, {$srid})";
    }

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
