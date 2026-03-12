<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface TableComments
{
    public function commentOnTable(string $table, string $comment): BuildResult;
}
