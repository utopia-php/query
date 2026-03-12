<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Schema\TriggerEvent;
use Utopia\Query\Schema\TriggerTiming;

interface Triggers
{
    public function createTrigger(
        string $name,
        string $table,
        TriggerTiming $timing,
        TriggerEvent $event,
        string $body,
    ): BuildResult;

    public function dropTrigger(string $name): BuildResult;
}
