<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface Types
{
    /**
     * @param  list<string>  $values
     */
    public function createType(string $name, array $values): Plan;

    public function dropType(string $name): Plan;
}
