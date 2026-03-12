<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface DropPartition
{
    public function dropPartition(string $table, string $name): BuildResult;
}
