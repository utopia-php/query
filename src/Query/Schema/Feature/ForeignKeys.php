<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;
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
    ): Plan;

    public function dropForeignKey(string $table, string $name): Plan;
}
