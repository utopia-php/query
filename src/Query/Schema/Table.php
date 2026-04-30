<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\ClickHouse\SkipIndexAlgorithm;

class Table
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

    /** @var list<CheckConstraint> */
    public private(set) array $checks = [];

    /** @var list<string> */
    public private(set) array $compositePrimaryKey = [];

    public private(set) ?PartitionType $partitionType = null;
    public private(set) string $partitionExpression = '';
    public private(set) ?int $partitionCount = null;

    public private(set) ?Engine $engine = null;

    /** @var list<string> */
    public private(set) array $engineArgs = [];

    public private(set) ?string $ttl = null;

    /** @var array<string, string> Table-level engine SETTINGS (ClickHouse only) */
    public private(set) array $settings = [];

    /**
     * Add a table-level CHECK constraint.
     *
     * The expression is emitted verbatim inside `CHECK (...)` and must come from
     * trusted (developer-controlled) source — never from untrusted input. The
     * constraint name is validated as a standard SQL identifier.
     *
     * @throws ValidationException if $name is not a valid identifier.
     */
    public function check(string $name, string $expression): static
    {
        $this->checks[] = new CheckConstraint($name, $expression);

        return $this;
    }

    /**
     * Declare a composite PRIMARY KEY across two or more columns.
     *
     * For a single-column primary key, use {@see Column::primary()} instead.
     *
     * @param  list<string>  $columns
     *
     * @throws ValidationException if fewer than two columns are provided or any column name is invalid.
     */
    public function primary(array $columns): static
    {
        if (\count($columns) < 2) {
            throw new ValidationException('Table::primary(array) requires at least two columns; use Column::primary() for single-column keys.');
        }

        foreach ($columns as $column) {
            if (! \preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                throw new ValidationException('Invalid column name in composite primary key: ' . $column);
            }
        }

        $this->compositePrimaryKey = $columns;

        return $this;
    }

    public function id(string $name = 'id'): Column
    {
        $col = (new Column($name, ColumnType::BigInteger))
            ->unsigned()
            ->autoIncrement()
            ->primary();
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

    /**
     * Auto-incrementing integer column (PostgreSQL SERIAL; INT AUTO_INCREMENT on MySQL;
     * INTEGER on SQLite; throws UnsupportedException on ClickHouse/MongoDB).
     */
    public function serial(string $name): Column
    {
        $col = (new Column($name, ColumnType::Serial))
            ->autoIncrement();
        $this->columns[] = $col;

        return $col;
    }

    /**
     * Auto-incrementing big integer column (PostgreSQL BIGSERIAL; BIGINT AUTO_INCREMENT on MySQL;
     * INTEGER on SQLite; throws UnsupportedException on ClickHouse/MongoDB).
     */
    public function bigSerial(string $name): Column
    {
        $col = (new Column($name, ColumnType::BigSerial))
            ->autoIncrement();
        $this->columns[] = $col;

        return $col;
    }

    /**
     * Auto-incrementing small integer column (PostgreSQL SMALLSERIAL; SMALLINT AUTO_INCREMENT on MySQL;
     * INTEGER on SQLite; throws UnsupportedException on ClickHouse/MongoDB).
     */
    public function smallSerial(string $name): Column
    {
        $col = (new Column($name, ColumnType::SmallSerial))
            ->autoIncrement();
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
        $col = (new Column($name, ColumnType::Enum))
            ->enum($values);
        $this->columns[] = $col;

        return $col;
    }

    public function point(string $name, int $srid = 4326): Column
    {
        $col = (new Column($name, ColumnType::Point))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function linestring(string $name, int $srid = 4326): Column
    {
        $col = (new Column($name, ColumnType::Linestring))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function polygon(string $name, int $srid = 4326): Column
    {
        $col = (new Column($name, ColumnType::Polygon))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function vector(string $name, int $dimensions): Column
    {
        $col = (new Column($name, ColumnType::Vector))
            ->dimensions($dimensions);
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
        ?SkipIndexAlgorithm $algorithm = null,
        array $algorithmArgs = [],
        int $granularity = 1,
    ): void {
        if ($name === '') {
            $name = $this->autoIndexName('idx_', $columns);
        }
        $this->indexes[] = new Index(
            $name,
            $columns,
            IndexType::Index,
            $lengths,
            $orders,
            $method,
            $operatorClass,
            $collations,
            algorithm: $algorithm,
            algorithmArgs: $algorithmArgs,
            granularity: $granularity,
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
    ): void {
        if ($name === '') {
            $name = $this->autoIndexName('uniq_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Unique, $lengths, $orders, collations: $collations);
    }

    /**
     * @param  string[]  $columns
     */
    public function fulltextIndex(array $columns, string $name = ''): void
    {
        if ($name === '') {
            $name = $this->autoIndexName('ft_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Fulltext);
    }

    /**
     * @param  string[]  $columns
     */
    public function spatialIndex(array $columns, string $name = ''): void
    {
        if ($name === '') {
            $name = $this->autoIndexName('sp_', $columns);
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
        $col = (new Column($name, $type, $type === ColumnType::String ? $lengthOrPrecision : null, $type !== ColumnType::String ? $lengthOrPrecision : null))
            ->modify();
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

    public function partitionByRange(string $expression): void
    {
        $this->partitionType = PartitionType::Range;
        $this->partitionExpression = $expression;
        $this->partitionCount = null;
    }

    public function partitionByList(string $expression): void
    {
        $this->partitionType = PartitionType::List;
        $this->partitionExpression = $expression;
        $this->partitionCount = null;
    }

    /**
     * Partition by hash of the given expression.
     *
     * When $partitions is non-null, the DDL emits `PARTITIONS <count>`. Per
     * MySQL/MariaDB semantics, this only applies to HASH (and KEY/LINEAR HASH/
     * LINEAR KEY variants) partitioning.
     *
     * @throws ValidationException if $partitions is less than 1.
     */
    public function partitionByHash(string $expression, ?int $partitions = null): static
    {
        if ($partitions !== null && $partitions < 1) {
            throw new ValidationException('Partition count must be at least 1.');
        }

        $this->partitionType = PartitionType::Hash;
        $this->partitionExpression = $expression;
        $this->partitionCount = $partitions;

        return $this;
    }

    /**
     * Select the table engine (ClickHouse only). Other dialects ignore this.
     *
     * Engine-specific arguments are validated against the engine variant:
     * - CollapsingMergeTree requires exactly one sign column.
     * - ReplicatedMergeTree requires a zookeeper path and replica name.
     *
     * @throws ValidationException if required engine arguments are missing.
     */
    public function engine(Engine $engine, string ...$args): static
    {
        if ($engine === Engine::CollapsingMergeTree && ! isset($args[0])) {
            throw new ValidationException('CollapsingMergeTree requires a sign column.');
        }

        if ($engine === Engine::ReplicatedMergeTree && (! isset($args[0]) || ! isset($args[1]))) {
            throw new ValidationException('ReplicatedMergeTree requires zookeeper_path and replica_name.');
        }

        $this->engine = $engine;
        $this->engineArgs = \array_values($args);

        return $this;
    }

    /**
     * Attach a table-level TTL expression (ClickHouse only).
     *
     * Emitted verbatim as `TTL <expression>` after ORDER BY/PARTITION BY.
     * Other dialects throw UnsupportedException when compiling the blueprint.
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
     * Build an auto-generated index name with a prefix, sanitising any
     * non-identifier characters in the column names so the result is always a
     * valid SQL identifier.
     *
     * @param  string[]  $columns
     */
    private function autoIndexName(string $prefix, array $columns): string
    {
        $sanitised = \array_map(
            fn (string $c): string => \preg_replace('/[^A-Za-z0-9_]+/', '_', $c) ?? $c,
            $columns,
        );

        return $prefix . \implode('_', $sanitised);
    }

    /**
     * Set table-level engine SETTINGS (ClickHouse only). Other dialects ignore.
     *
     * Compiled as `SETTINGS k=v, ...` after the TTL clause. Booleans become
     * `1` / `0`, ints/floats are stringified, strings are passed through after
     * a conservative character allow-list check.
     *
     * Calling this method replaces previously-set settings.
     *
     * @param  array<string, string|int|float|bool>  $settings
     *
     * @throws ValidationException if any key is not a valid identifier or any
     *                             string value contains characters outside the
     *                             allow-list.
     */
    public function settings(array $settings): static
    {
        $sanitized = [];

        foreach ($settings as $key => $value) {
            if (! \preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $key)) {
                throw new ValidationException('Invalid setting name: ' . $key);
            }

            if (\is_bool($value)) {
                $sanitized[$key] = $value ? '1' : '0';
            } elseif (\is_int($value) || \is_float($value)) {
                $sanitized[$key] = (string) $value;
            } elseif (\is_string($value)) {
                if (! \preg_match('/^[A-Za-z0-9_.\-+\/]*$/', $value)) {
                    throw new ValidationException(
                        'Invalid setting value for ' . $key . ': must match [A-Za-z0-9_.\\-+/]*'
                    );
                }
                $sanitized[$key] = $value;
            } else {
                throw new ValidationException(
                    'Setting value for ' . $key . ' must be string, int, float, or bool.'
                );
            }
        }

        $this->settings = $sanitized;

        return $this;
    }
}
