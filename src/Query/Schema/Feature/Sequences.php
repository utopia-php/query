<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\Plan;

interface Sequences
{
    public function createSequence(string $name, int $start = 1, int $incrementBy = 1): Plan;

    public function dropSequence(string $name): Plan;

    public function nextVal(string $name): Plan;
}
