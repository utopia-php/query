<?php

namespace Utopia\Query\Schema;

class Blueprint
{
    /** @var list<Column> */
    private array $columns = [];

    /** @var list<Index> */
    private array $indexes = [];

    /** @var list<ForeignKey> */
    private array $foreignKeys = [];

    /** @var list<string> */
    private array $dropColumns = [];

    /** @var list<array{from: string, to: string}> */
    private array $renameColumns = [];

    /** @var list<string> */
    private array $dropIndexes = [];

    /** @var list<string> */
    private array $dropForeignKeys = [];

    public function id(string $name = 'id'): Column
    {
        $col = new Column($name, 'bigInteger');
        $col->isUnsigned = true;
        $col->isAutoIncrement = true;
        $col->isPrimary = true;
        $this->columns[] = $col;

        return $col;
    }

    public function string(string $name, int $length = 255): Column
    {
        $col = new Column($name, 'string', $length);
        $this->columns[] = $col;

        return $col;
    }

    public function text(string $name): Column
    {
        $col = new Column($name, 'text');
        $this->columns[] = $col;

        return $col;
    }

    public function integer(string $name): Column
    {
        $col = new Column($name, 'integer');
        $this->columns[] = $col;

        return $col;
    }

    public function bigInteger(string $name): Column
    {
        $col = new Column($name, 'bigInteger');
        $this->columns[] = $col;

        return $col;
    }

    public function float(string $name): Column
    {
        $col = new Column($name, 'float');
        $this->columns[] = $col;

        return $col;
    }

    public function boolean(string $name): Column
    {
        $col = new Column($name, 'boolean');
        $this->columns[] = $col;

        return $col;
    }

    public function datetime(string $name, int $precision = 0): Column
    {
        $col = new Column($name, 'datetime', precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function timestamp(string $name, int $precision = 0): Column
    {
        $col = new Column($name, 'timestamp', precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function json(string $name): Column
    {
        $col = new Column($name, 'json');
        $this->columns[] = $col;

        return $col;
    }

    public function binary(string $name): Column
    {
        $col = new Column($name, 'binary');
        $this->columns[] = $col;

        return $col;
    }

    /**
     * @param  string[]  $values
     */
    public function enum(string $name, array $values): Column
    {
        $col = new Column($name, 'enum');
        $col->enumValues = $values;
        $this->columns[] = $col;

        return $col;
    }

    public function point(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, 'point');
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function linestring(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, 'linestring');
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function polygon(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, 'polygon');
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function vector(string $name, int $dimensions): Column
    {
        $col = new Column($name, 'vector');
        $col->dimensions = $dimensions;
        $this->columns[] = $col;

        return $col;
    }

    public function timestamps(int $precision = 3): void
    {
        $this->datetime('created_at', $precision);
        $this->datetime('updated_at', $precision);
    }

    /**
     * @param  string[]  $columns
     */
    public function index(array $columns, string $name = '', string $method = '', string $operatorClass = ''): void
    {
        if ($name === '') {
            $name = 'idx_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, method: $method, operatorClass: $operatorClass);
    }

    /**
     * @param  string[]  $columns
     */
    public function uniqueIndex(array $columns, string $name = ''): void
    {
        if ($name === '') {
            $name = 'uniq_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, 'unique');
    }

    public function foreignKey(string $column): ForeignKey
    {
        $fk = new ForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addColumn(string $name, string $type, int|null $lengthOrPrecision = null): Column
    {
        $col = new Column($name, $type, $type === 'string' ? $lengthOrPrecision : null, $type !== 'string' ? $lengthOrPrecision : null);
        $this->columns[] = $col;

        return $col;
    }

    public function modifyColumn(string $name, string $type, int|null $lengthOrPrecision = null): Column
    {
        $col = new Column($name, $type, $type === 'string' ? $lengthOrPrecision : null, $type !== 'string' ? $lengthOrPrecision : null);
        $col->isModify = true;
        $this->columns[] = $col;

        return $col;
    }

    public function renameColumn(string $from, string $to): void
    {
        $this->renameColumns[] = ['from' => $from, 'to' => $to];
    }

    public function dropColumn(string $name): void
    {
        $this->dropColumns[] = $name;
    }

    /**
     * @param  string[]  $columns
     */
    public function addIndex(string $name, array $columns): void
    {
        $this->indexes[] = new Index($name, $columns);
    }

    public function dropIndex(string $name): void
    {
        $this->dropIndexes[] = $name;
    }

    public function addForeignKey(string $column): ForeignKey
    {
        $fk = new ForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function dropForeignKey(string $name): void
    {
        $this->dropForeignKeys[] = $name;
    }

    /** @return list<Column> */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /** @return list<Index> */
    public function getIndexes(): array
    {
        return $this->indexes;
    }

    /** @return list<ForeignKey> */
    public function getForeignKeys(): array
    {
        return $this->foreignKeys;
    }

    /** @return list<string> */
    public function getDropColumns(): array
    {
        return $this->dropColumns;
    }

    /** @return list<array{from: string, to: string}> */
    public function getRenameColumns(): array
    {
        return $this->renameColumns;
    }

    /** @return list<string> */
    public function getDropIndexes(): array
    {
        return $this->dropIndexes;
    }

    /** @return list<string> */
    public function getDropForeignKeys(): array
    {
        return $this->dropForeignKeys;
    }
}
