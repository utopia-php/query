<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Schema\Feature\CreatePartition;
use Utopia\Query\Schema\Feature\DropPartition;
use Utopia\Query\Schema\Feature\TableComments;

class MySQL extends SQL implements TableComments, CreatePartition, DropPartition
{
    protected function compileColumnType(Column $column): string
    {
        return match ($column->type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Relationship => 'VARCHAR(' . ($column->length ?? 255) . ')',
            ColumnType::Text => 'TEXT',
            ColumnType::MediumText => 'MEDIUMTEXT',
            ColumnType::LongText => 'LONGTEXT',
            ColumnType::Integer => 'INT',
            ColumnType::BigInteger, ColumnType::Id => 'BIGINT',
            ColumnType::Float, ColumnType::Double => 'DOUBLE',
            ColumnType::Boolean => 'TINYINT(1)',
            ColumnType::Datetime => $column->precision ? 'DATETIME(' . $column->precision . ')' : 'DATETIME',
            ColumnType::Timestamp => $column->precision ? 'TIMESTAMP(' . $column->precision . ')' : 'TIMESTAMP',
            ColumnType::Json, ColumnType::Object => 'JSON',
            ColumnType::Binary => 'BLOB',
            ColumnType::Enum => "ENUM('" . \implode("','", \array_map(fn ($v) => \str_replace("'", "''", $v), $column->enumValues)) . "')",
            ColumnType::Point => 'POINT' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Linestring => 'LINESTRING' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Polygon => 'POLYGON' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            ColumnType::Uuid7 => 'VARCHAR(36)',
            ColumnType::Vector => throw new UnsupportedException('Vector type is not supported in MySQL.'),
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

    public function commentOnTable(string $table, string $comment): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table) . " COMMENT = '" . str_replace("'", "''", $comment) . "'",
            []
        );
    }

    public function createPartition(string $parent, string $name, string $expression): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($parent) . ' ADD PARTITION (PARTITION ' . $this->quote($name) . ' ' . $expression . ')',
            []
        );
    }

    public function dropPartition(string $table, string $name): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table) . ' DROP PARTITION ' . $this->quote($name),
            []
        );
    }
}
