<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\BuildResult;

interface Deletes
{
    public function from(string $table, string $alias = ''): static;

    public function delete(): BuildResult;
}
