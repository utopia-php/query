<?php

namespace Utopia\Query\Builder\Feature;

interface LockingOf
{
    public function forUpdateOf(string $table): static;

    public function forShareOf(string $table): static;
}
