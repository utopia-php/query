<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface ForeignKeys
{
    public function addForeignKey(
        string $table,
        string $name,
        string $column,
        string $refTable,
        string $refColumn,
        string $onDelete = '',
        string $onUpdate = '',
    ): BuildResult;

    public function dropForeignKey(string $table, string $name): BuildResult;
}
