<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface Procedures
{
    /**
     * @param  list<array{0: string, 1: string, 2: string}>  $params
     */
    public function createProcedure(string $name, array $params, string $body): BuildResult;

    public function dropProcedure(string $name): BuildResult;
}
