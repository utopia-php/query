<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface Types
{
    /**
     * @param  list<string>  $values
     */
    public function createType(string $name, array $values): BuildResult;

    public function dropType(string $name): BuildResult;
}
