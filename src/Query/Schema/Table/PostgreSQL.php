<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

class PostgreSQL extends Table
{
    use Trait\Checks;
    use Trait\CompositePrimary;
    use Trait\ForeignKeys;
    use Trait\FulltextSpatialIndex;
    use Trait\StandardPartitioning;

    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\PostgreSQL
    {
        return new Column\PostgreSQL($this, $name, $type, $length, $precision);
    }

    #[\Override]
    protected function newForeignKey(string $column): ForeignKey\PostgreSQL
    {
        return new ForeignKey\PostgreSQL($this, $column);
    }

    #[\Override]
    public function id(string $name = 'id'): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::id($name);
    }

    #[\Override]
    public function string(string $name, int $length = 255): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::string($name, $length);
    }

    #[\Override]
    public function text(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::text($name);
    }

    #[\Override]
    public function mediumText(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::mediumText($name);
    }

    #[\Override]
    public function longText(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::longText($name);
    }

    #[\Override]
    public function integer(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::integer($name);
    }

    #[\Override]
    public function bigInteger(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::bigInteger($name);
    }

    #[\Override]
    public function serial(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::serial($name);
    }

    #[\Override]
    public function bigSerial(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::bigSerial($name);
    }

    #[\Override]
    public function smallSerial(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::smallSerial($name);
    }

    #[\Override]
    public function float(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::float($name);
    }

    #[\Override]
    public function boolean(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::boolean($name);
    }

    #[\Override]
    public function datetime(string $name, int $precision = 0): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::datetime($name, $precision);
    }

    #[\Override]
    public function timestamp(string $name, int $precision = 0): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::timestamp($name, $precision);
    }

    #[\Override]
    public function json(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::json($name);
    }

    #[\Override]
    public function binary(string $name): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::binary($name);
    }

    /**
     * @param  string[]  $values
     */
    #[\Override]
    public function enum(string $name, array $values): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::enum($name, $values);
    }

    #[\Override]
    public function point(string $name, int $srid = 4326): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::point($name, $srid);
    }

    #[\Override]
    public function linestring(string $name, int $srid = 4326): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::linestring($name, $srid);
    }

    #[\Override]
    public function polygon(string $name, int $srid = 4326): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::polygon($name, $srid);
    }

    #[\Override]
    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::addColumn($name, $type, $lengthOrPrecision);
    }

    #[\Override]
    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\PostgreSQL
    {
        /** @var Column\PostgreSQL */
        return parent::modifyColumn($name, $type, $lengthOrPrecision);
    }

    /**
     * @return Column\PostgreSQL
     */
    public function vector(string $name, int $dimensions): Column
    {
        $col = $this->newColumn($name, ColumnType::Vector)->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }

    public function foreignKey(string $column): ForeignKey\PostgreSQL
    {
        $fk = $this->newForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addForeignKey(string $column): ForeignKey\PostgreSQL
    {
        return $this->foreignKey($column);
    }
}
