<?php

namespace Utopia\Query\Schema\Feature;

use Utopia\Query\Builder\BuildResult;

interface Sequences
{
    public function createSequence(string $name, int $start = 1, int $incrementBy = 1): BuildResult;

    public function dropSequence(string $name): BuildResult;

    public function nextVal(string $name): BuildResult;
}
