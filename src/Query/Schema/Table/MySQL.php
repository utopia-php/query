<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

class MySQL extends Table
{
    use Trait\Checks;
    use Trait\CompositePrimary;
    use Trait\ForeignKeys;
    use Trait\FulltextSpatialIndex;
    use Trait\StandardPartitioning;

    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\MySQL
    {
        return new Column\MySQL($this, $name, $type, $length, $precision);
    }

    #[\Override]
    protected function newForeignKey(string $column): ForeignKey\MySQL
    {
        return new ForeignKey\MySQL($this, $column);
    }

    #[\Override]
    public function id(string $name = 'id'): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::id($name);
    }

    #[\Override]
    public function string(string $name, int $length = 255): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::string($name, $length);
    }

    #[\Override]
    public function text(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::text($name);
    }

    #[\Override]
    public function mediumText(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::mediumText($name);
    }

    #[\Override]
    public function longText(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::longText($name);
    }

    #[\Override]
    public function integer(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::integer($name);
    }

    #[\Override]
    public function bigInteger(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::bigInteger($name);
    }

    #[\Override]
    public function serial(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::serial($name);
    }

    #[\Override]
    public function bigSerial(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::bigSerial($name);
    }

    #[\Override]
    public function smallSerial(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::smallSerial($name);
    }

    #[\Override]
    public function float(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::float($name);
    }

    #[\Override]
    public function boolean(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::boolean($name);
    }

    #[\Override]
    public function datetime(string $name, int $precision = 0): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::datetime($name, $precision);
    }

    #[\Override]
    public function timestamp(string $name, int $precision = 0): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::timestamp($name, $precision);
    }

    #[\Override]
    public function json(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::json($name);
    }

    #[\Override]
    public function binary(string $name): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::binary($name);
    }

    /**
     * @param  string[]  $values
     */
    #[\Override]
    public function enum(string $name, array $values): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::enum($name, $values);
    }

    #[\Override]
    public function point(string $name, int $srid = 4326): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::point($name, $srid);
    }

    #[\Override]
    public function linestring(string $name, int $srid = 4326): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::linestring($name, $srid);
    }

    #[\Override]
    public function polygon(string $name, int $srid = 4326): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::polygon($name, $srid);
    }

    #[\Override]
    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::addColumn($name, $type, $lengthOrPrecision);
    }

    #[\Override]
    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\MySQL
    {
        /** @var Column\MySQL */
        return parent::modifyColumn($name, $type, $lengthOrPrecision);
    }

    public function foreignKey(string $column): ForeignKey\MySQL
    {
        $fk = $this->newForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addForeignKey(string $column): ForeignKey\MySQL
    {
        return $this->foreignKey($column);
    }
}
