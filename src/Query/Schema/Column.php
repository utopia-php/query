<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Statement;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\ClickHouse\IndexAlgorithm;

class Column
{
    public private(set) bool $isNullable = false;

    public private(set) mixed $default = null;

    public private(set) bool $hasDefault = false;

    public private(set) bool $isUnsigned = false;

    public private(set) bool $isUnique = false;

    public private(set) bool $isPrimary = false;

    public private(set) bool $isAutoIncrement = false;

    public private(set) ?string $after = null;

    public private(set) ?string $comment = null;

    /** @var string[] */
    public private(set) array $enumValues = [];

    public private(set) ?int $srid = null;

    public private(set) ?int $dimensions = null;

    public private(set) bool $isModify = false;

    public private(set) ?string $collation = null;

    public private(set) ?string $checkExpression = null;

    public private(set) ?string $generatedExpression = null;

    /**
     * Null when {@see generatedAs()} has not been called.
     * True = STORED, false = VIRTUAL.
     */
    public private(set) ?bool $generatedStored = null;

    public private(set) ?string $ttl = null;

    public private(set) ?string $userTypeName = null;

    public function __construct(
        public Table $table,
        public string $name,
        public ColumnType $type,
        public ?int $length = null,
        public ?int $precision = null,
    ) {
    }

    public function nullable(): static
    {
        $this->isNullable = true;

        return $this;
    }

    public function default(mixed $value): static
    {
        $this->default = $value;
        $this->hasDefault = true;

        return $this;
    }

    public function unsigned(): static
    {
        $this->isUnsigned = true;

        return $this;
    }

    public function unique(): static
    {
        $this->isUnique = true;

        return $this;
    }

    /**
     * Mark this column as a primary key (no args), or declare a composite
     * primary key on the parent table (when an array is passed).
     *
     * @param  list<string>  $columns
     *
     * @phpstan-return ($columns is array{} ? static : Table)
     */
    public function primary(array $columns = []): static|Table
    {
        if ($columns === []) {
            $this->isPrimary = true;

            return $this;
        }

        return $this->table->primary($columns);
    }

    public function after(string $column): static
    {
        $this->after = $column;

        return $this;
    }

    public function autoIncrement(): static
    {
        $this->isAutoIncrement = true;

        return $this;
    }

    public function comment(string $comment): static
    {
        $this->comment = $comment;

        return $this;
    }

    public function collation(string $collation): static
    {
        $this->collation = $collation;

        return $this;
    }

    /**
     * Set the allowed values on this enum column (when called with one array
     * argument), or add a new enum column to the parent table (when called
     * with a name and a list of values).
     *
     * @param  string|string[]  $nameOrValues
     * @param  string[]|null    $values
     *
     * @throws ValidationException if the value list is empty.
     */
    public function enum(string|array $nameOrValues, ?array $values = null): static|Column
    {
        if (\is_array($nameOrValues)) {
            if ($nameOrValues === []) {
                throw new ValidationException('enum() requires at least one allowed value.');
            }

            $this->enumValues = $nameOrValues;

            return $this;
        }

        return $this->table->enum($nameOrValues, $values ?? []);
    }

    public function srid(int $srid): static
    {
        $this->srid = $srid;

        return $this;
    }

    public function dimensions(int $dimensions): static
    {
        $this->dimensions = $dimensions;

        return $this;
    }

    public function modify(): static
    {
        $this->isModify = true;

        return $this;
    }

    /**
     * Attach a CHECK constraint. Called with one argument it sets a column-
     * level CHECK on this column; called with two arguments it adds a named
     * table-level CHECK constraint via the parent table.
     *
     * @phpstan-return ($expression is null ? static : Table)
     */
    public function check(string $expressionOrName, ?string $expression = null): static|Table
    {
        if ($expression === null) {
            $this->checkExpression = $expressionOrName;

            return $this;
        }

        return $this->table->check($expressionOrName, $expression);
    }

    /**
     * Mark the column as a generated column computed from the given expression.
     */
    public function generatedAs(string $expression): static
    {
        $this->generatedExpression = $expression;

        return $this;
    }

    public function stored(): static
    {
        $this->generatedStored = true;

        return $this;
    }

    public function virtual(): static
    {
        $this->generatedStored = false;

        return $this;
    }

    /**
     * Attach a column-level TTL expression (ClickHouse only).
     *
     * @throws ValidationException if the expression is empty or contains a semicolon.
     */
    public function ttl(string $expression): static
    {
        $trimmed = \trim($expression);

        if ($trimmed === '') {
            throw new ValidationException('TTL expression must not be empty.');
        }

        if (\str_contains($trimmed, ';')) {
            throw new ValidationException('TTL expression must not contain ";".');
        }

        $this->ttl = $trimmed;

        return $this;
    }

    /**
     * Reference a user-defined type (e.g. a PostgreSQL enum type created via CREATE TYPE).
     *
     * @throws ValidationException if $name is not a valid identifier.
     */
    public function userType(string $name): static
    {
        if (! \preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $name)) {
            throw new ValidationException('Invalid user-defined type name: ' . $name);
        }

        $this->userTypeName = $name;

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
     * @param  list<string|int|float>  $algorithmArgs  ClickHouse skip-index algorithm args
     */
    public function index(
        array $columns,
        string $name = '',
        string $method = '',
        string $operatorClass = '',
        array $lengths = [],
        array $orders = [],
        array $collations = [],
        ?IndexAlgorithm $algorithm = null,
        array $algorithmArgs = [],
        int $granularity = 1,
    ): Table {
        return $this->table->index(
            $columns,
            $name,
            $method,
            $operatorClass,
            $lengths,
            $orders,
            $collations,
            $algorithm,
            $algorithmArgs,
            $granularity,
        );
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
     * @param  array<string, string|int|float|bool>  $settings
     */
    public function settings(array $settings): Table
    {
        return $this->table->settings($settings);
    }

    /**
     * @param  list<string>  $columns
     */
    public function orderBy(array $columns): Table
    {
        return $this->table->orderBy($columns);
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

    public function truncate(): Statement
    {
        return $this->table->truncate();
    }

    public function rename(string $to): Statement
    {
        return $this->table->rename($to);
    }
}
