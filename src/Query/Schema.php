<?php

namespace Utopia\Query;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\Column;

abstract class Schema
{
    abstract protected function quote(string $identifier): string;

    abstract protected function compileColumnType(Column $column): string;

    abstract protected function compileAutoIncrement(): string;

    /**
     * @param  callable(Blueprint): void  $definition
     */
    public function create(string $table, callable $definition): BuildResult
    {
        $blueprint = new Blueprint();
        $definition($blueprint);

        $columnDefs = [];
        $primaryKeys = [];
        $uniqueColumns = [];

        foreach ($blueprint->getColumns() as $column) {
            $def = $this->compileColumnDefinition($column);
            $columnDefs[] = $def;

            if ($column->isPrimary) {
                $primaryKeys[] = $this->quote($column->name);
            }
            if ($column->isUnique) {
                $uniqueColumns[] = $column->name;
            }
        }

        // Inline PRIMARY KEY constraint
        if (! empty($primaryKeys)) {
            $columnDefs[] = 'PRIMARY KEY (' . \implode(', ', $primaryKeys) . ')';
        }

        // Inline UNIQUE constraints for columns marked unique
        foreach ($uniqueColumns as $col) {
            $columnDefs[] = 'UNIQUE (' . $this->quote($col) . ')';
        }

        // Indexes
        foreach ($blueprint->getIndexes() as $index) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $index->columns);
            $keyword = $index->type === 'unique' ? 'UNIQUE INDEX' : 'INDEX';
            $columnDefs[] = $keyword . ' ' . $this->quote($index->name)
                . ' (' . \implode(', ', $cols) . ')';
        }

        // Foreign keys
        foreach ($blueprint->getForeignKeys() as $fk) {
            $def = 'FOREIGN KEY (' . $this->quote($fk->column) . ')'
                . ' REFERENCES ' . $this->quote($fk->refTable)
                . ' (' . $this->quote($fk->refColumn) . ')';
            if ($fk->onDelete !== '') {
                $def .= ' ON DELETE ' . $fk->onDelete;
            }
            if ($fk->onUpdate !== '') {
                $def .= ' ON UPDATE ' . $fk->onUpdate;
            }
            $columnDefs[] = $def;
        }

        $sql = 'CREATE TABLE ' . $this->quote($table)
            . ' (' . \implode(', ', $columnDefs) . ')';

        return new BuildResult($sql, []);
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
            $def = $keyword . ' ' . $this->compileColumnDefinition($column);
            if ($column->after !== null) {
                $def .= ' AFTER ' . $this->quote($column->after);
            }
            $alterations[] = $def;
        }

        foreach ($blueprint->getRenameColumns() as $rename) {
            $alterations[] = 'RENAME COLUMN ' . $this->quote($rename['from'])
                . ' TO ' . $this->quote($rename['to']);
        }

        foreach ($blueprint->getDropColumns() as $col) {
            $alterations[] = 'DROP COLUMN ' . $this->quote($col);
        }

        foreach ($blueprint->getIndexes() as $index) {
            $cols = \array_map(fn (string $c): string => $this->quote($c), $index->columns);
            $alterations[] = 'ADD INDEX ' . $this->quote($index->name)
                . ' (' . \implode(', ', $cols) . ')';
        }

        foreach ($blueprint->getDropIndexes() as $name) {
            $alterations[] = 'DROP INDEX ' . $this->quote($name);
        }

        foreach ($blueprint->getForeignKeys() as $fk) {
            $def = 'ADD FOREIGN KEY (' . $this->quote($fk->column) . ')'
                . ' REFERENCES ' . $this->quote($fk->refTable)
                . ' (' . $this->quote($fk->refColumn) . ')';
            if ($fk->onDelete !== '') {
                $def .= ' ON DELETE ' . $fk->onDelete;
            }
            if ($fk->onUpdate !== '') {
                $def .= ' ON UPDATE ' . $fk->onUpdate;
            }
            $alterations[] = $def;
        }

        foreach ($blueprint->getDropForeignKeys() as $name) {
            $alterations[] = 'DROP FOREIGN KEY ' . $this->quote($name);
        }

        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ' . \implode(', ', $alterations);

        return new BuildResult($sql, []);
    }

    public function drop(string $table): BuildResult
    {
        return new BuildResult('DROP TABLE ' . $this->quote($table), []);
    }

    public function dropIfExists(string $table): BuildResult
    {
        return new BuildResult('DROP TABLE IF EXISTS ' . $this->quote($table), []);
    }

    public function rename(string $from, string $to): BuildResult
    {
        return new BuildResult(
            'RENAME TABLE ' . $this->quote($from) . ' TO ' . $this->quote($to),
            []
        );
    }

    public function truncate(string $table): BuildResult
    {
        return new BuildResult('TRUNCATE TABLE ' . $this->quote($table), []);
    }

    /**
     * @param  string[]  $columns
     */
    public function createIndex(
        string $table,
        string $name,
        array $columns,
        bool $unique = false,
        string $type = '',
    ): BuildResult {
        $cols = \array_map(fn (string $c): string => $this->quote($c), $columns);

        $keyword = match (true) {
            $unique => 'CREATE UNIQUE INDEX',
            $type === 'fulltext' => 'CREATE FULLTEXT INDEX',
            $type === 'spatial' => 'CREATE SPATIAL INDEX',
            default => 'CREATE INDEX',
        };

        $sql = $keyword . ' ' . $this->quote($name)
            . ' ON ' . $this->quote($table)
            . ' (' . \implode(', ', $cols) . ')';

        return new BuildResult($sql, []);
    }

    public function dropIndex(string $table, string $name): BuildResult
    {
        return new BuildResult(
            'DROP INDEX ' . $this->quote($name) . ' ON ' . $this->quote($table),
            []
        );
    }

    public function createView(string $name, Builder $query): BuildResult
    {
        $result = $query->build();
        $sql = 'CREATE VIEW ' . $this->quote($name) . ' AS ' . $result->query;

        return new BuildResult($sql, $result->bindings);
    }

    public function createOrReplaceView(string $name, Builder $query): BuildResult
    {
        $result = $query->build();
        $sql = 'CREATE OR REPLACE VIEW ' . $this->quote($name) . ' AS ' . $result->query;

        return new BuildResult($sql, $result->bindings);
    }

    public function dropView(string $name): BuildResult
    {
        return new BuildResult('DROP VIEW ' . $this->quote($name), []);
    }

    protected function compileColumnDefinition(Column $column): string
    {
        $parts = [
            $this->quote($column->name),
            $this->compileColumnType($column),
        ];

        if ($column->isUnsigned) {
            $unsigned = $this->compileUnsigned();
            if ($unsigned !== '') {
                $parts[] = $unsigned;
            }
        }

        if ($column->isAutoIncrement) {
            $parts[] = $this->compileAutoIncrement();
        }

        if (! $column->isNullable) {
            $parts[] = 'NOT NULL';
        } else {
            $parts[] = 'NULL';
        }

        if ($column->hasDefault) {
            $parts[] = 'DEFAULT ' . $this->compileDefaultValue($column->default);
        }

        if ($column->comment !== null) {
            $parts[] = "COMMENT '" . \str_replace("'", "''", $column->comment) . "'";
        }

        return \implode(' ', $parts);
    }

    protected function compileDefaultValue(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }
        if (\is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (\is_int($value) || \is_float($value)) {
            return (string) $value;
        }

        /** @var string|int|float $value */
        return "'" . \str_replace("'", "''", (string) $value) . "'";
    }

    protected function compileUnsigned(): string
    {
        return 'UNSIGNED';
    }
}
