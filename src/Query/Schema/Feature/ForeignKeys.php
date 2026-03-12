<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Schema\ForeignKeyAction;

interface ForeignKeys
{
    public function addForeignKey(
        string $table,
        string $name,
        string $column,
        string $refTable,
        string $refColumn,
        ?ForeignKeyAction $onDelete = null,
        ?ForeignKeyAction $onUpdate = null,
    ): BuildResult;

    public function dropForeignKey(string $table, string $name): BuildResult;
}
