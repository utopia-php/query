<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

class SQLite extends Table
{
    use Trait\Checks;
    use Trait\CompositePrimary;
    use Trait\ForeignKeys;

    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\SQLite
    {
        return new Column\SQLite($this, $name, $type, $length, $precision);
    }

    #[\Override]
    protected function newForeignKey(string $column): ForeignKey\SQLite
    {
        return new ForeignKey\SQLite($this, $column);
    }

    #[\Override]
    public function id(string $name = 'id'): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::id($name);
    }

    #[\Override]
    public function string(string $name, int $length = 255): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::string($name, $length);
    }

    #[\Override]
    public function text(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::text($name);
    }

    #[\Override]
    public function mediumText(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::mediumText($name);
    }

    #[\Override]
    public function longText(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::longText($name);
    }

    #[\Override]
    public function integer(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::integer($name);
    }

    #[\Override]
    public function bigInteger(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::bigInteger($name);
    }

    #[\Override]
    public function serial(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::serial($name);
    }

    #[\Override]
    public function bigSerial(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::bigSerial($name);
    }

    #[\Override]
    public function smallSerial(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::smallSerial($name);
    }

    #[\Override]
    public function float(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::float($name);
    }

    #[\Override]
    public function boolean(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::boolean($name);
    }

    #[\Override]
    public function datetime(string $name, int $precision = 0): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::datetime($name, $precision);
    }

    #[\Override]
    public function timestamp(string $name, int $precision = 0): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::timestamp($name, $precision);
    }

    #[\Override]
    public function json(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::json($name);
    }

    #[\Override]
    public function binary(string $name): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::binary($name);
    }

    /**
     * @param  string[]  $values
     */
    #[\Override]
    public function enum(string $name, array $values): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::enum($name, $values);
    }

    #[\Override]
    public function point(string $name, int $srid = 4326): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::point($name, $srid);
    }

    #[\Override]
    public function linestring(string $name, int $srid = 4326): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::linestring($name, $srid);
    }

    #[\Override]
    public function polygon(string $name, int $srid = 4326): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::polygon($name, $srid);
    }

    #[\Override]
    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::addColumn($name, $type, $lengthOrPrecision);
    }

    #[\Override]
    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column\SQLite
    {
        /** @var Column\SQLite */
        return parent::modifyColumn($name, $type, $lengthOrPrecision);
    }

    public function foreignKey(string $column): ForeignKey\SQLite
    {
        $fk = $this->newForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addForeignKey(string $column): ForeignKey\SQLite
    {
        return $this->foreignKey($column);
    }
}
