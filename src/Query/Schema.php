<?php

namespace Utopia\Query;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\IndexType;

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

        foreach ($blueprint->columns as $column) {
            $def = $this->compileColumnDefinition($column);
            $columnDefs[] = $def;

            if ($column->isPrimary) {
                $primaryKeys[] = $this->quote($column->name);
            }
            if ($column->isUnique) {
                $uniqueColumns[] = $column->name;
            }
        }

        // Raw column definitions (bypass typed Column objects)
        foreach ($blueprint->rawColumnDefs as $rawDef) {
            $columnDefs[] = $rawDef;
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
        foreach ($blueprint->indexes as $index) {
            $keyword = match ($index->type) {
                IndexType::Unique => 'UNIQUE INDEX',
                IndexType::Fulltext => 'FULLTEXT INDEX',
                IndexType::Spatial => 'SPATIAL INDEX',
                default => 'INDEX',
            };
            $columnDefs[] = $keyword . ' ' . $this->quote($index->name)
                . ' (' . $this->compileIndexColumns($index) . ')';
        }

        // Raw index definitions (bypass typed Index objects)
        foreach ($blueprint->rawIndexDefs as $rawIdx) {
            $columnDefs[] = $rawIdx;
        }

        // Foreign keys
        foreach ($blueprint->foreignKeys as $fk) {
            $def = 'FOREIGN KEY (' . $this->quote($fk->column) . ')'
                . ' REFERENCES ' . $this->quote($fk->refTable)
                . ' (' . $this->quote($fk->refColumn) . ')';
            if ($fk->onDelete !== null) {
                $def .= ' ON DELETE ' . $fk->onDelete->value;
            }
            if ($fk->onUpdate !== null) {
                $def .= ' ON UPDATE ' . $fk->onUpdate->value;
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

        foreach ($blueprint->columns as $column) {
            $keyword = $column->isModify ? 'MODIFY COLUMN' : 'ADD COLUMN';
            $def = $keyword . ' ' . $this->compileColumnDefinition($column);
            if ($column->after !== null) {
                $def .= ' AFTER ' . $this->quote($column->after);
            }
            $alterations[] = $def;
        }

        foreach ($blueprint->renameColumns as $rename) {
            $alterations[] = 'RENAME COLUMN ' . $this->quote($rename->from)
                . ' TO ' . $this->quote($rename->to);
        }

        foreach ($blueprint->dropColumns as $col) {
            $alterations[] = 'DROP COLUMN ' . $this->quote($col);
        }

        foreach ($blueprint->indexes as $index) {
            $keyword = match ($index->type) {
                IndexType::Unique => 'ADD UNIQUE INDEX',
                IndexType::Fulltext => 'ADD FULLTEXT INDEX',
                IndexType::Spatial => 'ADD SPATIAL INDEX',
                default => 'ADD INDEX',
            };
            $alterations[] = $keyword . ' ' . $this->quote($index->name)
                . ' (' . $this->compileIndexColumns($index) . ')';
        }

        foreach ($blueprint->dropIndexes as $name) {
            $alterations[] = 'DROP INDEX ' . $this->quote($name);
        }

        foreach ($blueprint->foreignKeys as $fk) {
            $def = 'ADD FOREIGN KEY (' . $this->quote($fk->column) . ')'
                . ' REFERENCES ' . $this->quote($fk->refTable)
                . ' (' . $this->quote($fk->refColumn) . ')';
            if ($fk->onDelete !== null) {
                $def .= ' ON DELETE ' . $fk->onDelete->value;
            }
            if ($fk->onUpdate !== null) {
                $def .= ' ON UPDATE ' . $fk->onUpdate->value;
            }
            $alterations[] = $def;
        }

        foreach ($blueprint->dropForeignKeys as $name) {
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
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations
     * @param  list<string>  $rawColumns  Raw SQL expressions appended to column list (bypass quoting)
     */
    public function createIndex(
        string $table,
        string $name,
        array $columns,
        bool $unique = false,
        string $type = '',
        string $method = '',
        string $operatorClass = '',
        array $lengths = [],
        array $orders = [],
        array $collations = [],
        array $rawColumns = [],
    ): BuildResult {
        $keyword = match (true) {
            $unique => 'CREATE UNIQUE INDEX',
            $type === 'fulltext' => 'CREATE FULLTEXT INDEX',
            $type === 'spatial' => 'CREATE SPATIAL INDEX',
            default => 'CREATE INDEX',
        };

        $indexType = $unique ? IndexType::Unique : ($type !== '' ? IndexType::from($type) : IndexType::Index);
        $index = new Schema\Index($name, $columns, $indexType, $lengths, $orders, $method, $operatorClass, $collations, $rawColumns);

        $sql = $keyword . ' ' . $this->quote($name)
            . ' ON ' . $this->quote($table);

        if ($method !== '') {
            $sql .= ' USING ' . \strtoupper($method);
        }

        $sql .= ' (' . $this->compileIndexColumns($index) . ')';

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

    /**
     * Compile index column list with lengths, orders, collations, and operator classes.
     */
    protected function compileIndexColumns(Schema\Index $index): string
    {
        $parts = [];

        foreach ($index->columns as $col) {
            $part = $this->quote($col);

            if (isset($index->collations[$col])) {
                $part .= ' COLLATE ' . $index->collations[$col];
            }

            if (isset($index->lengths[$col])) {
                $part .= '(' . $index->lengths[$col] . ')';
            }

            if ($index->operatorClass !== '') {
                $part .= ' ' . $index->operatorClass;
            }

            if (isset($index->orders[$col])) {
                $part .= ' ' . \strtoupper($index->orders[$col]);
            }

            $parts[] = $part;
        }

        // Append raw expressions (bypass quoting) — for CAST ARRAY, JSONB paths, etc.
        foreach ($index->rawColumns as $raw) {
            $parts[] = $raw;
        }

        return \implode(', ', $parts);
    }

    public function renameIndex(string $table, string $from, string $to): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table) . ' RENAME INDEX ' . $this->quote($from) . ' TO ' . $this->quote($to),
            []
        );
    }

    public function createDatabase(string $name): BuildResult
    {
        return new BuildResult('CREATE DATABASE ' . $this->quote($name), []);
    }

    public function dropDatabase(string $name): BuildResult
    {
        return new BuildResult('DROP DATABASE ' . $this->quote($name), []);
    }

    public function analyzeTable(string $table): BuildResult
    {
        return new BuildResult('ANALYZE TABLE ' . $this->quote($table), []);
    }
}
