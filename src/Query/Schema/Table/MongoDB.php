<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\Table;

class MongoDB extends Table
{
    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\MongoDB
    {
        return new Column\MongoDB($this, $name, $type, $length, $precision);
    }

    #[\Override]
    public function id(string $name = 'id'): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::id($name);
    }

    #[\Override]
    public function string(string $name, int $length = 255): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::string($name, $length);
    }

    #[\Override]
    public function text(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::text($name);
    }

    #[\Override]
    public function mediumText(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::mediumText($name);
    }

    #[\Override]
    public function longText(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::longText($name);
    }

    #[\Override]
    public function integer(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::integer($name);
    }

    #[\Override]
    public function bigInteger(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::bigInteger($name);
    }

    #[\Override]
    public function serial(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::serial($name);
    }

    #[\Override]
    public function bigSerial(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::bigSerial($name);
    }

    #[\Override]
    public function smallSerial(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::smallSerial($name);
    }

    #[\Override]
    public function float(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::float($name);
    }

    #[\Override]
    public function boolean(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::boolean($name);
    }

    #[\Override]
    public function datetime(string $name, int $precision = 0): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::datetime($name, $precision);
    }

    #[\Override]
    public function timestamp(string $name, int $precision = 0): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::timestamp($name, $precision);
    }

    #[\Override]
    public function json(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::json($name);
    }

    #[\Override]
    public function binary(string $name): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::binary($name);
    }

    /**
     * @param  string[]  $values
     */
    #[\Override]
    public function enum(string $name, array $values): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::enum($name, $values);
    }

    #[\Override]
    public function point(string $name, int $srid = 4326): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::point($name, $srid);
    }

    #[\Override]
    public function linestring(string $name, int $srid = 4326): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::linestring($name, $srid);
    }

    #[\Override]
    public function polygon(string $name, int $srid = 4326): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::polygon($name, $srid);
    }

    #[\Override]
    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::addColumn($name, $type, $lengthOrPrecision);
    }

    #[\Override]
    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\MongoDB
    {
        /** @var Column\MongoDB */
        return parent::modifyColumn($name, $type, $lengthOrPrecision);
    }

    /**
     * @return Column\MongoDB
     */
    public function vector(string $name, int $dimensions): Column
    {
        $col = $this->newColumn($name, ColumnType::Vector)->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }
}
