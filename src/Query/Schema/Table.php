<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Statement;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Schema;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\ClickHouse\IndexAlgorithm;

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

    /** @var list<string> ClickHouse ORDER BY columns; falls back to primary key when empty */
    public private(set) array $orderBy = [];

    public private(set) ?PartitionType $partitionType = null;
    public private(set) string $partitionExpression = '';
    public private(set) ?int $partitionCount = null;

    public private(set) ?Engine $engine = null;

    /** @var list<string> */
    public private(set) array $engineArgs = [];

    public private(set) ?string $ttl = null;

    /** @var array<string, string> Table-level engine SETTINGS (ClickHouse only) */
    public private(set) array $settings = [];

    public function __construct(
        private readonly ?Schema $schema = null,
        public readonly string $name = '',
    ) {
    }

    public function create(bool $ifNotExists = false): Statement
    {
        return $this->requireSchema()->compileCreate($this, $ifNotExists);
    }

    public function createIfNotExists(): Statement
    {
        return $this->create(true);
    }

    public function alter(): Statement
    {
        return $this->requireSchema()->compileAlter($this);
    }

    public function drop(): Statement
    {
        return $this->requireSchema()->compileDrop($this->name, false);
    }

    public function dropIfExists(): Statement
    {
        return $this->requireSchema()->compileDrop($this->name, true);
    }

    public function truncate(): Statement
    {
        return $this->requireSchema()->compileTruncate($this->name);
    }

    public function rename(string $to): Statement
    {
        return $this->requireSchema()->compileRename($this->name, $to);
    }

    private function requireSchema(): Schema
    {
        if ($this->schema === null) {
            throw new UnsupportedException('Cannot compile a Table without a Schema. Use Schema::table($name) to obtain a builder.');
        }

        return $this->schema;
    }

    /**
     * Add a table-level CHECK constraint.
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
        $col = new Column($this, $name, ColumnType::BigInteger);
        $col->unsigned()->autoIncrement()->primary();
        $this->columns[] = $col;

        return $col;
    }

    public function string(string $name, int $length = 255): Column
    {
        $col = new Column($this, $name, ColumnType::String, $length);
        $this->columns[] = $col;

        return $col;
    }

    public function text(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Text);
        $this->columns[] = $col;

        return $col;
    }

    public function mediumText(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::MediumText);
        $this->columns[] = $col;

        return $col;
    }

    public function longText(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::LongText);
        $this->columns[] = $col;

        return $col;
    }

    public function integer(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Integer);
        $this->columns[] = $col;

        return $col;
    }

    public function bigInteger(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::BigInteger);
        $this->columns[] = $col;

        return $col;
    }

    /**
     * Auto-incrementing integer column (PostgreSQL SERIAL; INT AUTO_INCREMENT on MySQL;
     * INTEGER on SQLite; throws UnsupportedException on ClickHouse/MongoDB).
     */
    public function serial(string $name): Column
    {
        $col = (new Column($this, $name, ColumnType::Serial))
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
        $col = (new Column($this, $name, ColumnType::BigSerial))
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
        $col = (new Column($this, $name, ColumnType::SmallSerial))
            ->autoIncrement();
        $this->columns[] = $col;

        return $col;
    }

    public function float(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Float);
        $this->columns[] = $col;

        return $col;
    }

    public function boolean(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Boolean);
        $this->columns[] = $col;

        return $col;
    }

    public function datetime(string $name, int $precision = 0): Column
    {
        $col = new Column($this, $name, ColumnType::Datetime, precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function timestamp(string $name, int $precision = 0): Column
    {
        $col = new Column($this, $name, ColumnType::Timestamp, precision: $precision);
        $this->columns[] = $col;

        return $col;
    }

    public function json(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Json);
        $this->columns[] = $col;

        return $col;
    }

    public function binary(string $name): Column
    {
        $col = new Column($this, $name, ColumnType::Binary);
        $this->columns[] = $col;

        return $col;
    }

    /**
     * @param  string[]  $values
     *
     * @throws ValidationException if the value list is empty.
     */
    public function enum(string $name, array $values): Column
    {
        if ($values === []) {
            throw new ValidationException('enum() requires at least one allowed value.');
        }

        $col = (new Column($this, $name, ColumnType::Enum))
            ->enum($values);
        $this->columns[] = $col;

        return $col;
    }

    public function point(string $name, int $srid = 4326): Column
    {
        $col = (new Column($this, $name, ColumnType::Point))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function linestring(string $name, int $srid = 4326): Column
    {
        $col = (new Column($this, $name, ColumnType::Linestring))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function polygon(string $name, int $srid = 4326): Column
    {
        $col = (new Column($this, $name, ColumnType::Polygon))
            ->srid($srid);
        $this->columns[] = $col;

        return $col;
    }

    public function vector(string $name, int $dimensions): Column
    {
        $col = (new Column($this, $name, ColumnType::Vector))
            ->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }

    public function timestamps(int $precision = 3): static
    {
        $this->datetime('created_at', $precision);
        $this->datetime('updated_at', $precision);

        return $this;
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
    ): static {
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

        return $this;
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
    ): static {
        if ($name === '') {
            $name = $this->autoIndexName('uniq_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Unique, $lengths, $orders, collations: $collations);

        return $this;
    }

    /**
     * @param  string[]  $columns
     */
    public function fulltextIndex(array $columns, string $name = ''): static
    {
        if ($name === '') {
            $name = $this->autoIndexName('ft_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Fulltext);

        return $this;
    }

    /**
     * @param  string[]  $columns
     */
    public function spatialIndex(array $columns, string $name = ''): static
    {
        if ($name === '') {
            $name = $this->autoIndexName('sp_', $columns);
        }
        $this->indexes[] = new Index($name, $columns, IndexType::Spatial);

        return $this;
    }

    /**
     * Declare a foreign key. The behaviour is identical for create and alter
     * contexts — the dialect compiler switches between `FOREIGN KEY (...)` (in
     * a CREATE TABLE column list) and `ADD FOREIGN KEY (...)` (in an ALTER
     * TABLE clause) when emitting the statement. {@see addForeignKey()} is
     * an alias for use in alter chains; both register the same FK exactly once.
     */
    public function foreignKey(string $column): ForeignKey
    {
        $fk = new ForeignKey($this, $column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    public function addColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column
    {
        $col = new Column(
            $this,
            $name,
            $type,
            $type === ColumnType::String ? $lengthOrPrecision : null,
            $type !== ColumnType::String ? $lengthOrPrecision : null,
        );
        $this->columns[] = $col;

        return $col;
    }

    public function modifyColumn(string $name, ColumnType $type, ?int $lengthOrPrecision = null): Column
    {
        $col = (new Column(
            $this,
            $name,
            $type,
            $type === ColumnType::String ? $lengthOrPrecision : null,
            $type !== ColumnType::String ? $lengthOrPrecision : null,
        ))->modify();
        $this->columns[] = $col;

        return $col;
    }

    public function renameColumn(string $from, string $to): static
    {
        $this->renameColumns[] = new RenameColumn($from, $to);

        return $this;
    }

    public function dropColumn(string $name): static
    {
        $this->dropColumns[] = $name;

        return $this;
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
        IndexType $type = IndexType::Index,
        array $lengths = [],
        array $orders = [],
        string $method = '',
        string $operatorClass = '',
        array $collations = [],
        array $rawColumns = [],
    ): static {
        $this->indexes[] = new Index($name, $columns, $type, $lengths, $orders, $method, $operatorClass, $collations, $rawColumns);

        return $this;
    }

    public function dropIndex(string $name): static
    {
        $this->dropIndexes[] = $name;

        return $this;
    }

    /**
     * Alias of {@see foreignKey()}, for symmetry with the other `add*`/`drop*`
     * alter helpers. Returns the same registered {@see ForeignKey}; calling
     * both methods for the same column registers the FK twice.
     */
    public function addForeignKey(string $column): ForeignKey
    {
        return $this->foreignKey($column);
    }

    public function dropForeignKey(string $name): static
    {
        $this->dropForeignKeys[] = $name;

        return $this;
    }

    /**
     * Add a raw SQL column definition (bypass typed Column objects).
     *
     * Example: $table->rawColumn('`my_col` VARCHAR(255) NOT NULL DEFAULT ""')
     */
    public function rawColumn(string $definition): static
    {
        $this->rawColumnDefs[] = $definition;

        return $this;
    }

    /**
     * Add a raw SQL index definition (bypass typed Index objects).
     *
     * Example: $table->rawIndex('INDEX `idx_name` (`col1`, `col2`)')
     */
    public function rawIndex(string $definition): static
    {
        $this->rawIndexDefs[] = $definition;

        return $this;
    }

    public function partitionByRange(string $expression): static
    {
        $this->partitionType = PartitionType::Range;
        $this->partitionExpression = $expression;
        $this->partitionCount = null;

        return $this;
    }

    public function partitionByList(string $expression): static
    {
        $this->partitionType = PartitionType::List;
        $this->partitionExpression = $expression;
        $this->partitionCount = null;

        return $this;
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
     * Set the ClickHouse ORDER BY clause. When unset, ClickHouse falls back to the
     * primary key columns.
     *
     * @param  list<string>  $columns
     *
     * @throws ValidationException if any column name is not a valid identifier.
     */
    public function orderBy(array $columns): static
    {
        foreach ($columns as $column) {
            if (! \preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                throw new ValidationException('Invalid column name in ORDER BY: ' . $column);
            }
        }

        $this->orderBy = $columns;

        return $this;
    }

    /**
     * Attach a table-level TTL expression (ClickHouse only).
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
            } elseif (\is_int($value)) {
                $sanitized[$key] = (string) $value;
            } elseif (\is_float($value)) {
                // Avoid scientific notation (e.g. 1.0E-5), which ClickHouse
                // rejects in SETTINGS values; trim trailing zeros for clean
                // output.
                $sanitized[$key] = \rtrim(\rtrim(\sprintf('%F', $value), '0'), '.');
            } elseif (\is_string($value)) {
                if (! \preg_match('/^[A-Za-z0-9_.\ -+\/]+$/', $value)) {
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
