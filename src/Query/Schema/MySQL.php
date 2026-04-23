<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Statement;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Schema\Feature\CreatePartition;
use Utopia\Query\Schema\Feature\DropPartition;
use Utopia\Query\Schema\Feature\TableComments;

class MySQL extends SQL implements TableComments, CreatePartition, DropPartition
{
    protected function compileColumnType(Column $column): string
    {
        if ($column->userTypeName !== null) {
            throw new UnsupportedException('User-defined types are not supported in MySQL.');
        }

        return match ($column->type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Relationship => 'VARCHAR(' . ($column->length ?? 255) . ')',
            ColumnType::Text => 'TEXT',
            ColumnType::MediumText => 'MEDIUMTEXT',
            ColumnType::LongText => 'LONGTEXT',
            ColumnType::Integer, ColumnType::Serial => 'INT',
            ColumnType::BigInteger, ColumnType::Id, ColumnType::BigSerial => 'BIGINT',
            ColumnType::SmallSerial => 'SMALLINT',
            ColumnType::Float, ColumnType::Double => 'DOUBLE',
            ColumnType::Boolean => 'TINYINT(1)',
            ColumnType::Datetime => $column->precision ? 'DATETIME(' . $column->precision . ')' : 'DATETIME',
            ColumnType::Timestamp => $column->precision ? 'TIMESTAMP(' . $column->precision . ')' : 'TIMESTAMP',
            ColumnType::Json, ColumnType::Object => 'JSON',
            ColumnType::Binary => 'BLOB',
            ColumnType::Enum => "ENUM('" . \implode("','", \array_map(fn ($v) => \str_replace(['\\', "'"], ['\\\\', "''"], $v), $column->enumValues)) . "')",
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

    public function createDatabase(string $name): Statement
    {
        return new Statement(
            'CREATE DATABASE ' . $this->quote($name) . ' /*!40100 DEFAULT CHARACTER SET utf8mb4 */',
            [],
            executor: $this->executor,
        );
    }

    /**
     * MySQL CHANGE COLUMN: rename and/or retype a column in one statement.
     */
    public function changeColumn(string $table, string $oldName, string $newName, string $type): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table)
            . ' CHANGE COLUMN ' . $this->quote($oldName) . ' ' . $this->quote($newName) . ' ' . $type,
            [],
            executor: $this->executor,
        );
    }

    /**
     * MySQL MODIFY COLUMN: retype a column without renaming.
     */
    public function modifyColumn(string $table, string $name, string $type): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table)
            . ' MODIFY ' . $this->quote($name) . ' ' . $type,
            [],
            executor: $this->executor,
        );
    }

    public function commentOnTable(string $table, string $comment): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table) . " COMMENT = '" . str_replace(['\\', "'"], ['\\\\', "''"], $comment) . "'",
            [],
            executor: $this->executor,
        );
    }

    public function createPartition(string $parent, string $name, string $expression): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($parent) . ' ADD PARTITION (PARTITION ' . $this->quote($name) . ' ' . $expression . ')',
            [],
            executor: $this->executor,
        );
    }

    public function dropPartition(string $table, string $name): Statement
    {
        return new Statement(
            'ALTER TABLE ' . $this->quote($table) . ' DROP PARTITION ' . $this->quote($name),
            [],
            executor: $this->executor,
        );
    }
}
