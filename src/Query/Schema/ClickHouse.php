<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\QuotesIdentifiers;
use Utopia\Query\Schema;

class ClickHouse extends Schema
{
    use QuotesIdentifiers;

    protected function compileColumnType(Column $column): string
    {
        $type = match ($column->type) {
            ColumnType::String => 'String',
            ColumnType::Text => 'String',
            ColumnType::MediumText, ColumnType::LongText => 'String',
            ColumnType::Integer => $column->isUnsigned ? 'UInt32' : 'Int32',
            ColumnType::BigInteger => $column->isUnsigned ? 'UInt64' : 'Int64',
            ColumnType::Float => 'Float64',
            ColumnType::Boolean => 'UInt8',
            ColumnType::Datetime => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            ColumnType::Timestamp => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            ColumnType::Json => 'String',
            ColumnType::Binary => 'String',
            ColumnType::Enum => $this->compileClickHouseEnum($column->enumValues),
            ColumnType::Point => 'Tuple(Float64, Float64)',
            ColumnType::Linestring => 'Array(Tuple(Float64, Float64))',
            ColumnType::Polygon => 'Array(Array(Tuple(Float64, Float64)))',
            ColumnType::Vector => 'Array(Float64)',
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
        $parts = [
            $this->quote($column->name),
            $this->compileColumnType($column),
        ];

        if ($column->hasDefault) {
            $parts[] = 'DEFAULT ' . $this->compileDefaultValue($column->default);
        }

        if ($column->comment !== null) {
            $parts[] = "COMMENT '" . \str_replace("'", "''", $column->comment) . "'";
        }

        return \implode(' ', $parts);
    }

    public function dropIndex(string $table, string $name): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table)
            . ' DROP INDEX ' . $this->quote($name),
            []
        );
    }

    /**
     * @param  callable(Blueprint): void  $definition
     */
    public function alter(string $table, callable $definition): BuildResult
    {
        $blueprint = new Blueprint();
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

        if (! empty($blueprint->foreignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        if (! empty($blueprint->dropForeignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ' . \implode(', ', $alterations);

        return new BuildResult($sql, []);
    }

    /**
     * @param  callable(Blueprint): void  $definition
     */
    public function create(string $table, callable $definition): BuildResult
    {
        $blueprint = new Blueprint();
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

        // Indexes (ClickHouse uses INDEX ... TYPE ... GRANULARITY ...)
        foreach ($blueprint->indexes as $index) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $index->columns);
            $expr = \count($cols) === 1 ? $cols[0] : '(' . \implode(', ', $cols) . ')';
            $columnDefs[] = 'INDEX ' . $this->quote($index->name)
                . ' ' . $expr . ' TYPE minmax GRANULARITY 3';
        }

        if (! empty($blueprint->foreignKeys)) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        $sql = 'CREATE TABLE ' . $this->quote($table)
            . ' (' . \implode(', ', $columnDefs) . ')'
            . ' ENGINE = MergeTree()';

        if (! empty($primaryKeys)) {
            $sql .= ' ORDER BY (' . \implode(', ', $primaryKeys) . ')';
        }

        return new BuildResult($sql, []);
    }

    public function createView(string $name, Builder $query): BuildResult
    {
        $result = $query->build();
        $sql = 'CREATE VIEW ' . $this->quote($name) . ' AS ' . $result->query;

        return new BuildResult($sql, $result->bindings);
    }

    /**
     * @param  string[]  $values
     */
    private function compileClickHouseEnum(array $values): string
    {
        $parts = [];
        foreach (\array_values($values) as $i => $value) {
            $parts[] = "'" . \str_replace("'", "\\'", $value) . "' = " . ($i + 1);
        }

        return 'Enum8(' . \implode(', ', $parts) . ')';
    }
}
