<?php

namespace Utopia\Query\Schema;

class MySQL extends SQL
{
    protected function compileColumnType(Column $column): string
    {
        return match ($column->type) {
            'string' => 'VARCHAR(' . ($column->length ?? 255) . ')',
            'text' => 'TEXT',
            'integer' => 'INT',
            'bigInteger' => 'BIGINT',
            'float' => 'DOUBLE',
            'boolean' => 'TINYINT(1)',
            'datetime' => $column->precision ? 'DATETIME(' . $column->precision . ')' : 'DATETIME',
            'timestamp' => $column->precision ? 'TIMESTAMP(' . $column->precision . ')' : 'TIMESTAMP',
            'json' => 'JSON',
            'binary' => 'BLOB',
            'enum' => "ENUM('" . \implode("','", \array_map(fn ($v) => \str_replace("'", "''", $v), $column->enumValues)) . "')",
            'point' => 'POINT' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            'linestring' => 'LINESTRING' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            'polygon' => 'POLYGON' . ($column->srid !== null ? ' SRID ' . $column->srid : ''),
            default => throw new \Utopia\Query\Exception\UnsupportedException('Unknown column type: ' . $column->type),
        };
    }

    protected function compileAutoIncrement(): string
    {
        return 'AUTO_INCREMENT';
    }
}
