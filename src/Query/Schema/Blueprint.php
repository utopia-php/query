<?php

namespace Utopia\Query\Schema;

class Blueprint
{
    /** @var list<Column> */
    public private(set) array $columns = [];

    /** @var list<Index> */
    public private(set) array $indexes = [];

    /** @var list<ForeignKey> */
    public private(set) array $foreignKeys = [];

    /** @var list<string> */
    public private(set) array $dropColumns = [];

    /** @var list<RenameColumn> */
    public private(set) array $renameColumns = [];

    /** @var list<string> */
    public private(set) array $dropIndexes = [];

    /** @var list<string> */
    public private(set) array $dropForeignKeys = [];

    /** @var list<string> Raw SQL column definitions (bypass typed Column objects) */
    public private(set) array $rawColumnDefs = [];

    /** @var list<string> Raw SQL index definitions (bypass typed Index objects) */
    public private(set) array $rawIndexDefs = [];

    public function id(string $name = 'id'): Column
    {
        $col = new Column($name, ColumnType::BigInteger);
        $col->isUnsigned = true;
        $col->isAutoIncrement = true;
        $col->isPrimary = true;
        $this->columns[] = $col;

        return $col;
    }

    public function string(string $name, int $length = 255): Column
    {
        $col = new Column($name, ColumnType::String, $length);
        $this->columns[] = $col;

        return $col;
    }

    public function text(string $name): Column
    {
        $col = new Column($name, ColumnType::Text);
        $this->columns[] = $col;

        return $col;
    }

    public function mediumText(string $name): Column
    {
        $col = new Column($name, ColumnType::MediumText);
        $this->columns[] = $col;

        return $col;
    }

    public function longText(string $name): Column
    {
        $col = new Column($name, ColumnType::LongText);
        $this->columns[] = $col;

        return $col;
    }

    public function integer(string $name): Column
    {
        $col = new Column($name, ColumnType::Integer);
        $this->columns[] = $col;

        return $col;
    }

    public function bigInteger(string $name): Column
    {
        $col = new Column($name, ColumnType::BigInteger);
        $this->columns[] = $col;

        return $col;
    }

    public function float(string $name): Column
    {
        $col = new Column($name, ColumnType::Float);
        $this->columns[] = $col;

        return $col;
    }

    public function boolean(string $name): Column
    {
        $col = new Column($name, ColumnType::Boolean);
        $this->columns[] = $col;

        return $col;
    }

    public function datetime(string $name, int $precision = 0): Column
    {
        $col = new Column($name, ColumnType::Datetime, precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function timestamp(string $name, int $precision = 0): Column
    {
        $col = new Column($name, ColumnType::Timestamp, precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function json(string $name): Column
    {
        $col = new Column($name, ColumnType::Json);
        $this->columns[] = $col;

        return $col;
    }

    public function binary(string $name): Column
    {
        $col = new Column($name, ColumnType::Binary);
        $this->columns[] = $col;

        return $col;
    }

    /**
     * @param  string[]  $values
     */
    public function enum(string $name, array $values): Column
    {
        $col = new Column($name, ColumnType::Enum);
        $col->enumValues = $values;
        $this->columns[] = $col;

        return $col;
    }

    public function point(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, ColumnType::Point);
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function linestring(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, ColumnType::Linestring);
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function polygon(string $name, int $srid = 4326): Column
    {
        $col = new Column($name, ColumnType::Polygon);
        $col->srid = $srid;
        $this->columns[] = $col;

        return $col;
    }

    public function vector(string $name, int $dimensions): Column
    {
        $col = new Column($name, ColumnType::Vector);
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
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations
     */
    public function index(
        array $columns,
        string $name = '',
        string $method = '',
        string $operatorClass = '',
        array $lengths = [],
        array $orders = [],
        array $collations = [],
    ): void {
        if ($name === '') {
            $name = 'idx_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Index, $lengths, $orders, $method, $operatorClass, $collations);
    }

    /**
     * @param  string[]  $columns
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations
     */
    public function uniqueIndex(
        array $columns,
        string $name = '',
        array $lengths = [],
        array $orders = [],
        array $collations = [],
    ): void {
        if ($name === '') {
            $name = 'uniq_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Unique, $lengths, $orders, collations: $collations);
    }

    /**
     * @param  string[]  $columns
     */
    public function fulltextIndex(array $columns, string $name = ''): void
    {
        if ($name === '') {
            $name = 'ft_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Fulltext);
    }

    /**
     * @param  string[]  $columns
     */
    public function spatialIndex(array $columns, string $name = ''): void
    {
        if ($name === '') {
            $name = 'sp_' . \implode('_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Spatial);
    }

    public function foreignKey(string $column): ForeignKey
    {
        $fk = new ForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addColumn(string $name, ColumnType|string $type, int|null $lengthOrPrecision = null): Column
    {
        if (\is_string($type)) {
            $type = ColumnType::from($type);
        }
        $col = new Column($name, $type, $type === ColumnType::String ? $lengthOrPrecision : null, $type !== ColumnType::String ? $lengthOrPrecision : null);
        $this->columns[] = $col;

        return $col;
    }

    public function modifyColumn(string $name, ColumnType|string $type, int|null $lengthOrPrecision = null): Column
    {
        if (\is_string($type)) {
            $type = ColumnType::from($type);
        }
        $col = new Column($name, $type, $type === ColumnType::String ? $lengthOrPrecision : null, $type !== ColumnType::String ? $lengthOrPrecision : null);
        $col->isModify = true;
        $this->columns[] = $col;

        return $col;
    }

    public function renameColumn(string $from, string $to): void
    {
        $this->renameColumns[] = new RenameColumn($from, $to);
    }

    public function dropColumn(string $name): void
    {
        $this->dropColumns[] = $name;
    }

    /**
     * @param  string[]  $columns
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations
     * @param  list<string>  $rawColumns  Raw SQL expressions appended to column list (bypass quoting)
     */
    public function addIndex(
        string $name,
        array $columns,
        IndexType|string $type = IndexType::Index,
        array $lengths = [],
        array $orders = [],
        string $method = '',
        string $operatorClass = '',
        array $collations = [],
        array $rawColumns = [],
    ): void {
        if (\is_string($type)) {
            $type = IndexType::from($type);
        }
        $this->indexes[] = new Index($name, $columns, $type, $lengths, $orders, $method, $operatorClass, $collations, $rawColumns);
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

    /**
     * Add a raw SQL column definition (bypass typed Column objects).
     *
     * Example: $table->rawColumn('`my_col` VARCHAR(255) NOT NULL DEFAULT ""')
     */
    public function rawColumn(string $definition): void
    {
        $this->rawColumnDefs[] = $definition;
    }

    /**
     * Add a raw SQL index definition (bypass typed Index objects).
     *
     * Example: $table->rawIndex('INDEX `idx_name` (`col1`, `col2`)')
     */
    public function rawIndex(string $definition): void
    {
        $this->rawIndexDefs[] = $definition;
    }

}
