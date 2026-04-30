<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Statement;
use Utopia\Query\Schema\ClickHouse\Engine;

class ForeignKey
{
    public private(set) string $refTable = '';

    public private(set) string $refColumn = '';

    public private(set) ?ForeignKeyAction $onDelete = null;

    public private(set) ?ForeignKeyAction $onUpdate = null;

    public function __construct(
        public readonly Table $table,
        public readonly string $column,
    ) {
    }

    public function references(string $column): static
    {
        $this->refColumn = $column;

        return $this;
    }

    public function on(string $table): static
    {
        $this->refTable = $table;

        return $this;
    }

    public function onDelete(ForeignKeyAction $action): static
    {
        $this->onDelete = $action;

        return $this;
    }

    public function onUpdate(ForeignKeyAction $action): static
    {
        $this->onUpdate = $action;

        return $this;
    }

    public function id(string $name = 'id'): Column
    {
        return $this->table->id($name);
    }

    public function string(string $name, int $length = 255): Column
    {
        return $this->table->string($name, $length);
    }

    public function text(string $name): Column
    {
        return $this->table->text($name);
    }

    public function mediumText(string $name): Column
    {
        return $this->table->mediumText($name);
    }

    public function longText(string $name): Column
    {
        return $this->table->longText($name);
    }

    public function integer(string $name): Column
    {
        return $this->table->integer($name);
    }

    public function bigInteger(string $name): Column
    {
        return $this->table->bigInteger($name);
    }

    public function serial(string $name): Column
    {
        return $this->table->serial($name);
    }

    public function bigSerial(string $name): Column
    {
        return $this->table->bigSerial($name);
    }

    public function smallSerial(string $name): Column
    {
        return $this->table->smallSerial($name);
    }

    public function float(string $name): Column
    {
        return $this->table->float($name);
    }

    public function boolean(string $name): Column
    {
        return $this->table->boolean($name);
    }

    public function datetime(string $name, int $precision = 0): Column
    {
        return $this->table->datetime($name, $precision);
    }

    public function timestamp(string $name, int $precision = 0): Column
    {
        return $this->table->timestamp($name, $precision);
    }

    public function json(string $name): Column
    {
        return $this->table->json($name);
    }

    public function binary(string $name): Column
    {
        return $this->table->binary($name);
    }

    /**
     * @param  string[]  $values
     */
    public function enum(string $name, array $values): Column
    {
        return $this->table->enum($name, $values);
    }

    public function point(string $name, int $srid = 4326): Column
    {
        return $this->table->point($name, $srid);
    }

    public function linestring(string $name, int $srid = 4326): Column
    {
        return $this->table->linestring($name, $srid);
    }

    public function polygon(string $name, int $srid = 4326): Column
    {
        return $this->table->polygon($name, $srid);
    }

    public function vector(string $name, int $dimensions): Column
    {
        return $this->table->vector($name, $dimensions);
    }

    public function timestamps(int $precision = 3): Table
    {
        return $this->table->timestamps($precision);
    }

    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column
    {
        return $this->table->addColumn($name, $type, $lengthOrPrecision);
    }

    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column
    {
        return $this->table->modifyColumn($name, $type, $lengthOrPrecision);
    }

    public function renameColumn(string $from, string $to): Table
    {
        return $this->table->renameColumn($from, $to);
    }

    public function dropColumn(string $name): Table
    {
        return $this->table->dropColumn($name);
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
    ): Table {
        return $this->table->index($columns, $name, $method, $operatorClass, $lengths, $orders, $collations);
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
    ): Table {
        return $this->table->uniqueIndex($columns, $name, $lengths, $orders, $collations);
    }

    /**
     * @param  string[]  $columns
     */
    public function fulltextIndex(array $columns, string $name = ''): Table
    {
        return $this->table->fulltextIndex($columns, $name);
    }

    /**
     * @param  string[]  $columns
     */
    public function spatialIndex(array $columns, string $name = ''): Table
    {
        return $this->table->spatialIndex($columns, $name);
    }

    /**
     * @param  string[]  $columns
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations
     * @param  list<string>  $rawColumns
     */
    public function addIndex(
        string $name,
        array $columns,
        IndexType $type = IndexType::Index,
        array $lengths = [],
        array $orders = [],
        string $method = '',
        string $operatorClass = '',
        array $collations = [],
        array $rawColumns = [],
    ): Table {
        return $this->table->addIndex($name, $columns, $type, $lengths, $orders, $method, $operatorClass, $collations, $rawColumns);
    }

    public function dropIndex(string $name): Table
    {
        return $this->table->dropIndex($name);
    }

    public function foreignKey(string $column): ForeignKey
    {
        return $this->table->foreignKey($column);
    }

    public function addForeignKey(string $column): ForeignKey
    {
        return $this->table->addForeignKey($column);
    }

    public function dropForeignKey(string $name): Table
    {
        return $this->table->dropForeignKey($name);
    }

    /**
     * @param  list<string>  $columns
     */
    public function primary(array $columns): Table
    {
        return $this->table->primary($columns);
    }

    public function check(string $name, string $expression): Table
    {
        return $this->table->check($name, $expression);
    }

    public function rawColumn(string $definition): Table
    {
        return $this->table->rawColumn($definition);
    }

    public function rawIndex(string $definition): Table
    {
        return $this->table->rawIndex($definition);
    }

    public function partitionByRange(string $expression): Table
    {
        return $this->table->partitionByRange($expression);
    }

    public function partitionByList(string $expression): Table
    {
        return $this->table->partitionByList($expression);
    }

    public function partitionByHash(string $expression, ?int $partitions = null): Table
    {
        return $this->table->partitionByHash($expression, $partitions);
    }

    public function engine(Engine $engine, string ...$args): Table
    {
        return $this->table->engine($engine, ...$args);
    }

    /**
     * @param  list<string>  $columns
     */
    public function orderBy(array $columns): Table
    {
        return $this->table->orderBy($columns);
    }

    public function ttl(string $expression): Table
    {
        return $this->table->ttl($expression);
    }

    public function create(bool $ifNotExists = false): Statement
    {
        return $this->table->create($ifNotExists);
    }

    public function createIfNotExists(): Statement
    {
        return $this->table->createIfNotExists();
    }

    public function alter(): Statement
    {
        return $this->table->alter();
    }

    public function drop(): Statement
    {
        return $this->table->drop();
    }

    public function dropIfExists(): Statement
    {
        return $this->table->dropIfExists();
    }
}
