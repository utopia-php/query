<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\BuildResult;

class MySQL extends SQL
{
    protected function compileColumnType(Column $column): string
    {
        return match ($column->type) {
            ColumnType::String => 'VARCHAR(' . ($column->length ?? 255) . ')',
            ColumnType::Text => 'TEXT',
            ColumnType::MediumText => 'MEDIUMTEXT',
            ColumnType::LongText => 'LONGTEXT',
            ColumnType::Integer => 'INT',
            ColumnType::BigInteger => 'BIGINT',
            ColumnType::Float => 'DOUBLE',
            ColumnType::Boolean => 'TINYINT(1)',
            ColumnType::Datetime => $column->precision ? 'DATETIME(' . $column->precision . ')' : 'DATETIME',
            ColumnType::Timestamp => $column->precision ? 'TIMESTAMP(' . $column->precision . ')' : 'TIMESTAMP',
            ColumnType::Json => 'JSON',
            ColumnType::Binary => 'BLOB',
            ColumnType::Enum => "ENUM('" . \implode("','", \array_map(fn ($v) => \str_replace("'", "''", $v), $column->enumValues)) . "')",
            ColumnType::Point => 'POINT' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Linestring => 'LINESTRING' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Polygon => 'POLYGON' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Vector => throw new \Utopia\Query\Exception\UnsupportedException('Vector type is not supported in MySQL.'),
        };
    }

    protected function compileAutoIncrement(): string
    {
        return 'AUTO_INCREMENT';
    }

    public function createDatabase(string $name): BuildResult
    {
        return new BuildResult(
            'CREATE DATABASE ' . $this->quote($name) . ' /*!40100 DEFAULT CHARACTER SET utf8mb4 */',
            []
        );
    }

    /**
     * MySQL CHANGE COLUMN: rename and/or retype a column in one statement.
     */
    public function changeColumn(string $table, string $oldName, string $newName, string $type): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table)
            . ' CHANGE COLUMN ' . $this->quote($oldName) . ' ' . $this->quote($newName) . ' ' . $type,
            []
        );
    }

    /**
     * MySQL MODIFY COLUMN: retype a column without renaming.
     */
    public function modifyColumn(string $table, string $name, string $type): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table)
            . ' MODIFY ' . $this->quote($name) . ' ' . $type,
            []
        );
    }
}
