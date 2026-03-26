<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface DropPartition
{
    public function dropPartition(string $table, string $name): Plan;
}
