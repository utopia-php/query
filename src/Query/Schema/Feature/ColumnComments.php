<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface ColumnComments
{
    public function commentOnColumn(string $table, string $column, string $comment): Plan;
}
