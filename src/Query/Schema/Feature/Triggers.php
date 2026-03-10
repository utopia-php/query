<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface Triggers
{
    public function createTrigger(
        string $name,
        string $table,
        string $timing,
        string $event,
        string $body,
    ): BuildResult;

    public function dropTrigger(string $name): BuildResult;
}
