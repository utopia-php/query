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
            'string' => 'String',
            'text' => 'String',
            'integer' => $column->isUnsigned ? 'UInt32' : 'Int32',
            'bigInteger' => $column->isUnsigned ? 'UInt64' : 'Int64',
            'float' => 'Float64',
            'boolean' => 'UInt8',
            'datetime' => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            'timestamp' => $column->precision ? 'DateTime64(' . $column->precision . ')' : 'DateTime',
            'json' => 'String',
            'binary' => 'String',
            'enum' => $this->compileClickHouseEnum($column->enumValues),
            'point' => 'Tuple(Float64, Float64)',
            'linestring' => 'Array(Tuple(Float64, Float64))',
            'polygon' => 'Array(Array(Tuple(Float64, Float64)))',
            'vector' => 'Array(Float64)',
            default => throw new UnsupportedException('Unknown column type: ' . $column->type),
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

        foreach ($blueprint->getColumns() as $column) {
            $keyword = $column->isModify ? 'MODIFY COLUMN' : 'ADD COLUMN';
            $alterations[] = $keyword . ' ' . $this->compileColumnDefinition($column);
        }

        foreach ($blueprint->getRenameColumns() as $rename) {
            $alterations[] = 'RENAME COLUMN ' . $this->quote($rename['from'])
                . ' TO ' . $this->quote($rename['to']);
        }

        foreach ($blueprint->getDropColumns() as $col) {
            $alterations[] = 'DROP COLUMN ' . $this->quote($col);
        }

        foreach ($blueprint->getDropIndexes() as $name) {
            $alterations[] = 'DROP INDEX ' . $this->quote($name);
        }

        if (! empty($blueprint->getForeignKeys())) {
            throw new UnsupportedException('Foreign keys are not supported in ClickHouse.');
        }

        if (! empty($blueprint->getDropForeignKeys())) {
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

        foreach ($blueprint->getColumns() as $column) {
            $def = $this->compileColumnDefinition($column);
            $columnDefs[] = $def;

            if ($column->isPrimary) {
                $primaryKeys[] = $this->quote($column->name);
            }
        }

        // Indexes (ClickHouse uses INDEX ... TYPE ... GRANULARITY ...)
        foreach ($blueprint->getIndexes() as $index) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $index->columns);
            $expr = \count($cols) === 1 ? $cols[0] : '(' . \implode(', ', $cols) . ')';
            $columnDefs[] = 'INDEX ' . $this->quote($index->name)
                . ' ' . $expr . ' TYPE minmax GRANULARITY 3';
        }

        if (! empty($blueprint->getForeignKeys())) {
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
