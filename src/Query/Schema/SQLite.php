<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Plan;
use Utopia\Query\Exception\UnsupportedException;

class SQLite extends SQL
{
    protected function compileColumnType(Column $column): string
    {
        return match ($column->type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Relationship => 'VARCHAR(' . ($column->length ?? 255) . ')',
            ColumnType::Text, ColumnType::MediumText, ColumnType::LongText => 'TEXT',
            ColumnType::Integer, ColumnType::BigInteger, ColumnType::Id => 'INTEGER',
            ColumnType::Float, ColumnType::Double => 'REAL',
            ColumnType::Boolean => 'INTEGER',
            ColumnType::Datetime, ColumnType::Timestamp => 'TEXT',
            ColumnType::Json, ColumnType::Object => 'TEXT',
            ColumnType::Binary => 'BLOB',
            ColumnType::Enum => 'TEXT',
            ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon => 'TEXT',
            ColumnType::Uuid7 => 'VARCHAR(36)',
            ColumnType::Vector => throw new UnsupportedException('Vector type is not supported in SQLite.'),
        };
    }

    protected function compileAutoIncrement(): string
    {
        return 'AUTOINCREMENT';
    }

    protected function compileUnsigned(): string
    {
        return '';
    }

    public function createDatabase(string $name): Plan
    {
        throw new UnsupportedException('SQLite does not support CREATE DATABASE.');
    }

    public function dropDatabase(string $name): Plan
    {
        throw new UnsupportedException('SQLite does not support DROP DATABASE.');
    }

    public function rename(string $from, string $to): Plan
    {
        return new Plan(
            'ALTER TABLE ' . $this->quote($from) . ' RENAME TO ' . $this->quote($to),
            [],
            executor: $this->executor,
        );
    }

    public function truncate(string $table): Plan
    {
        return new Plan('DELETE FROM ' . $this->quote($table), [], executor: $this->executor);
    }

    public function dropIndex(string $table, string $name): Plan
    {
        return new Plan('DROP INDEX ' . $this->quote($name), [], executor: $this->executor);
    }

    public function renameIndex(string $table, string $from, string $to): Plan
    {
        throw new UnsupportedException('SQLite does not support renaming indexes directly.');
    }
}
