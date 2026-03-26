<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface CreatePartition
{
    public function createPartition(string $parent, string $name, string $expression): Plan;
}
