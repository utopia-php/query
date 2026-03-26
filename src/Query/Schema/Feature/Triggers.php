<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;
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
    ): Plan;

    public function dropTrigger(string $name): Plan;
}
