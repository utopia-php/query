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
            /** @var array{0: string|array<mixed>, 1: float, 2: bool} $data */
            $data = $values[0];
            $meters = $data[2];

            if ($meters && $query->getAttributeType() !== '') {
                $wkt = \is_array($data[0]) ? $this->geometryToWkt($data[0]) : $data[0];
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
        /** @var array{0: string|array<mixed>, 1: float, 2: bool} $data */
        $data = $values[0];
        $wkt = \is_array($data[0]) ? $this->geometryToWkt($data[0]) : $data[0];
        $distance = $data[1];
        $meters = $data[2];

        $operator = match ($method) {
            Method::DistanceLessThan => '<',
            Method::DistanceGreaterThan => '>',
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            default => '<',
        };

        if ($meters) {
            $this->addBinding($wkt);
            $this->addBinding($distance);

            return 'ST_DISTANCE_SPHERE(' . $attribute . ', ST_GeomFromText(?, 4326)) ' . $operator . ' ?';
        }

        $this->addBinding($wkt);
        $this->addBinding($distance);

        return 'ST_Distance(' . $attribute . ', ST_GeomFromText(?, 4326)) ' . $operator . ' ?';
    }
}
