<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder;
use Utopia\Query\Builder\Statement;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\QuotesIdentifiers;
use Utopia\Query\Schema;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\ClickHouse\SkipIndex;
use Utopia\Query\Schema\Feature\ColumnComments;
use Utopia\Query\Schema\Feature\DropPartition;
use Utopia\Query\Schema\Feature\TableComments;

class ClickHouse extends Schema implements TableComments, ColumnComments, DropPartition
{
    use QuotesIdentifiers;

    protected function compileColumnType(Column $column): string
    {
        if ($column->userTypeName !== null) {
            throw new UnsupportedException('User-defined types are not supported in ClickHouse.');
        }

        $type = match ($column->type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Relationship => 'String',
            ColumnType::Text => 'String',
            ColumnType::MediumText, ColumnType::LongText => 'String',
            ColumnType::Integer => $column->isUnsigned ? 'UInt32' : 'Int32',
            ColumnType::BigInteger, ColumnType::Id => $column->isUnsigned ? 'UInt64' : 'Int64',
            ColumnType::Float, ColumnType::Double => 'Float64',
            ColumnType::Boolean => 'UInt8',
            ColumnType::Datetime => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            ColumnType::Timestamp => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            ColumnType::Json, ColumnType::Object => 'String',
            ColumnType::Binary => 'String',
            ColumnType::Enum => $this->compileClickHouseEnum($column->enumValues),
            ColumnType::Point => 'Tuple(Float64, Float64)',
            ColumnType::Linestring => 'Array(Tuple(Float64, Float64))',
            ColumnType::Polygon => 'Array(Array(Tuple(Float64, Float64)))',
            ColumnType::Uuid7 => 'FixedString(36)',
            ColumnType::Vector => 'Array(Float64)',
            ColumnType::Serial, ColumnType::BigSerial, ColumnType::SmallSerial => throw new UnsupportedException('SERIAL types are not supported in ClickHouse.'),
        };

        if ($column->isNullable) {
            $type = 'Nullable(' . $type . ')';
        }

        return $type;
    }

    protected function compileAutoIncrement(): string
    {
        return '';
    }

    protected function compileUnsigned(): string
    {
        return '';
    }

    protected function compileColumnDefinition(Column $column): string
    {
        if ($column->generatedExpression !== null) {
            throw new UnsupportedException('Generated columns are not supported in ClickHouse.');
        }

        if ($column->checkExpression !== null) {
            throw new UnsupportedException('CHECK constraints are not supported in ClickHouse.');
        }

        $parts = [
            $this->quote($column->name),
            $this->compileColumnType($column),
        ];

        if ($column->hasDefault) {
            $parts[] = 'DEFAULT ' . $this->compileDefaultValue($column->default);
        }

        if ($column->ttl !== null) {
            $parts[] = 'TTL ' . $column->ttl;
        }

        if ($column->comment !== null) {
            $parts[] = "COMMENT '" . \str_replace(['\\', "'"], ['\\\\', "''"], $column->comment) . "'";
        }

        return \implode(' ', $parts);
    }

    public function dropIndex(string $table, string $name): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table)
            . ' DROP INDEX ' . $this->quote($name),
            [],
            executor: $this->executor,
        );
    }

    /**
     * @param  callable(Table): void  $definition
     */
    public function alter(string $table, callable $definition): Statement
    {
        $blueprint = new Table();
        $definition($blueprint);

        $alterations = [];

        foreach ($blueprint->columns as $column) {
            $keyword = $column->isModify ? 'MODIFY COLUMN' : 'ADD COLUMN';
            $alterations[] = $keyword . ' ' . $this->compileColumnDefinition($column);
        }

        foreach ($blueprint->renameColumns as $rename) {
            $alterations[] = 'RENAME COLUMN ' . $this->quote($rename->from)
                . ' TO ' . $this->quote($rename->to);
        }

        foreach ($blueprint->dropColumns as $col) {
            $alterations[] = 'DROP COLUMN ' . $this->quote($col);
        }

        foreach ($blueprint->dropIndexes as $name) {
            $alterations[] = 'DROP INDEX ' . $this->quote($name);
        }

        foreach ($blueprint->skipIndexes as $skip) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $skip->columns);
            $expr = \count($cols) === 1 ? $cols[0] : '(' . \implode(', ', $cols) . ')';
            $alterations[] = 'ADD INDEX ' . $this->quote($skip->name)
                . ' ' . $expr . ' TYPE ' . $this->compileSkipAlgorithm($skip)
                . ' GRANULARITY ' . $skip->granularity;
        }

        if (! empty($blueprint->foreignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        if (! empty($blueprint->dropForeignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        if (! empty($blueprint->settings)) {
            throw new UnsupportedException(
                'Table SETTINGS can only be set on CREATE TABLE; emit `ALTER TABLE ... MODIFY SETTING` directly to change them.'
            );
        }

        if (empty($alterations)) {
            throw new ValidationException('ALTER TABLE requires at least one alteration.');
        }

        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ' . \implode(', ', $alterations);

        return new Statement($sql, [], executor: $this->executor);
    }

    /**
     * @param  callable(Table): void  $definition
     */
    public function create(string $table, callable $definition, bool $ifNotExists = false): Statement
    {
        $blueprint = new Table();
        $definition($blueprint);

        $columnDefs = [];
        $primaryKeys = [];

        foreach ($blueprint->columns as $column) {
            $def = $this->compileColumnDefinition($column);
            $columnDefs[] = $def;

            if ($column->isPrimary) {
                $primaryKeys[] = $this->quote($column->name);
            }
        }

        if (! empty($blueprint->compositePrimaryKey) && ! empty($primaryKeys)) {
            throw new ValidationException('Cannot combine column-level primary() with Table::primary() composite key.');
        }

        if (empty($primaryKeys) && ! empty($blueprint->compositePrimaryKey)) {
            $primaryKeys = \array_map(fn (string $c): string => $this->quote($c), $blueprint->compositePrimaryKey);
        }

        // Indexes (ClickHouse uses INDEX ... TYPE ... GRANULARITY ...)
        foreach ($blueprint->indexes as $index) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $index->columns);
            $expr = \count($cols) === 1 ? $cols[0] : '(' . \implode(', ', $cols) . ')';
            $columnDefs[] = 'INDEX ' . $this->quote($index->name)
                . ' ' . $expr . ' TYPE minmax GRANULARITY 3';
        }

        foreach ($blueprint->skipIndexes as $skip) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $skip->columns);
            $expr = \count($cols) === 1 ? $cols[0] : '(' . \implode(', ', $cols) . ')';
            $columnDefs[] = 'INDEX ' . $this->quote($skip->name)
                . ' ' . $expr . ' TYPE ' . $this->compileSkipAlgorithm($skip)
                . ' GRANULARITY ' . $skip->granularity;
        }

        if (! empty($blueprint->foreignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        if (! empty($blueprint->checks)) {
            throw new UnsupportedException('CHECK constraints are not supported in ClickHouse.');
        }

        $engine = $blueprint->engine ?? Engine::MergeTree;

        $sql = 'CREATE TABLE ' . ($ifNotExists ? 'IF NOT EXISTS ' : '') . $this->quote($table)
            . ' (' . \implode(', ', $columnDefs) . ')'
            . ' ENGINE = ' . $this->compileEngine($engine, $blueprint->engineArgs);

        if ($blueprint->partitionType !== null) {
            $sql .= ' PARTITION BY ' . $blueprint->partitionExpression;
        }

        if ($engine->requiresOrderBy()) {
            $sql .= ! empty($primaryKeys)
                ? ' ORDER BY (' . \implode(', ', $primaryKeys) . ')'
                : ' ORDER BY tuple()';
        }

        if ($blueprint->ttl !== null) {
            $sql .= ' TTL ' . $blueprint->ttl;
        }

        if (! empty($blueprint->settings)) {
            $kv = [];
            foreach ($blueprint->settings as $k => $v) {
                $kv[] = $k . ' = ' . $v;
            }
            $sql .= ' SETTINGS ' . \implode(', ', $kv);
        }

        return new Statement($sql, [], executor: $this->executor);
    }

    /**
     * Render a `TYPE <algorithm>(args)` fragment for a data-skipping index.
     *
     * String args are emitted as single-quoted SQL literals (with `'` doubled);
     * numeric args are emitted verbatim. Argument values come from the
     * application — never from untrusted input.
     */
    private function compileSkipAlgorithm(SkipIndex $skip): string
    {
        if ($skip->algorithmArgs === []) {
            return $skip->algorithm->value;
        }

        $args = \array_map(
            fn (string|int|float $arg): string => match (true) {
                \is_string($arg) => "'" . \str_replace("'", "''", $arg) . "'",
                // sprintf('%F', ...) avoids scientific notation (e.g. 1.0E-5)
                // which ClickHouse rejects in index type arguments. Trim
                // trailing zeros so 0.01 stays "0.010000" → "0.01".
                \is_float($arg) => \rtrim(\rtrim(\sprintf('%F', $arg), '0'), '.'),
                default => (string) $arg,
            },
            $skip->algorithmArgs,
        );

        return $skip->algorithm->value . '(' . \implode(', ', $args) . ')';
    }

    /**
     * Compile an engine declaration: `<Name>` or `<Name>(<args...>)`.
     *
     * Identifier-type args (version column, sign column, column lists) are
     * quoted. Zookeeper path and replica name for ReplicatedMergeTree are
     * emitted as single-quoted string literals.
     *
     * @param  list<string>  $args
     */
    private function compileEngine(Engine $engine, array $args): string
    {
        return match ($engine) {
            Engine::MergeTree,
            Engine::AggregatingMergeTree => $engine->value . '()',

            Engine::ReplacingMergeTree => $engine->value . '('
                . (isset($args[0]) ? $this->quote($args[0]) : '')
                . ')',

            Engine::SummingMergeTree => $engine->value . '('
                . (empty($args)
                    ? ''
                    : \implode(', ', \array_map(fn (string $c): string => $this->quote($c), $args)))
                . ')',

            Engine::CollapsingMergeTree => $engine->value . '(' . $this->quote($args[0]) . ')',

            Engine::ReplicatedMergeTree => $engine->value
                . "('" . \str_replace("'", "''", $args[0]) . "'"
                . ", '" . \str_replace("'", "''", $args[1]) . "')",

            Engine::Memory,
            Engine::Log,
            Engine::TinyLog,
            Engine::StripeLog => $engine->value,
        };
    }

    public function createView(string $name, Builder $query): Statement
    {
        $result = $query->build();
        $sql = 'CREATE VIEW ' . $this->quote($name) . ' AS ' . $result->query;

        return new Statement($sql, $result->bindings, executor: $this->executor);
    }

    /**
     * @param  string[]  $values
     */
    private function compileClickHouseEnum(array $values): string
    {
        $parts = [];
        foreach (\array_values($values) as $i => $value) {
            $parts[] = "'" . \str_replace(['\\', "'"], ['\\\\', "\\'"], $value) . "' = " . ($i + 1);
        }

        return 'Enum8(' . \implode(', ', $parts) . ')';
    }

    public function commentOnTable(string $table, string $comment): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table) . " MODIFY COMMENT '" . str_replace(['\\', "'"], ['\\\\', "''"], $comment) . "'",
            [],
            executor: $this->executor,
        );
    }

    public function commentOnColumn(string $table, string $column, string $comment): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table) . ' COMMENT COLUMN ' . $this->quote($column) . " '" . str_replace(['\\', "'"], ['\\\\', "''"], $comment) . "'",
            [],
            executor: $this->executor,
        );
    }

    public function dropPartition(string $table, string $name): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table) . " DROP PARTITION '" . str_replace(['\\', "'"], ['\\\\', "''"], $name) . "'",
            [],
            executor: $this->executor,
        );
    }
}
