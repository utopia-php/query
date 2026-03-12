<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\BuildResult;
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

    public function createDatabase(string $name): BuildResult
    {
        throw new UnsupportedException('SQLite does not support CREATE DATABASE.');
    }

    public function dropDatabase(string $name): BuildResult
    {
        throw new UnsupportedException('SQLite does not support DROP DATABASE.');
    }

    public function rename(string $from, string $to): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($from) . ' RENAME TO ' . $this->quote($to),
            []
        );
    }

    public function truncate(string $table): BuildResult
    {
        return new BuildResult('DELETE FROM ' . $this->quote($table), []);
    }

    public function dropIndex(string $table, string $name): BuildResult
    {
        return new BuildResult('DROP INDEX ' . $this->quote($name), []);
    }

    public function renameIndex(string $table, string $from, string $to): BuildResult
    {
        throw new UnsupportedException('SQLite does not support renaming indexes directly.');
    }
}
