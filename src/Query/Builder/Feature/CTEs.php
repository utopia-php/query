<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder;

interface CTEs
{
    public function with(string $name, Builder $query): static;

    public function withRecursive(string $name, Builder $query): static;

    public function withRecursiveSeedStep(string $name, Builder $seed, Builder $step): static;
}
