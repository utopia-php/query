<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface TableComments
{
    public function commentOnTable(string $table, string $comment): Plan;
}
