<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\BuildResult;

interface Updates
{
    public function from(string $table): static;

    /**
     * @param  array<string, mixed>  $row
     */
    public function set(array $row): static;

    /**
     * @param  list<mixed>  $bindings
     */
    public function setRaw(string $column, string $expression, array $bindings = []): static;

    public function update(): BuildResult;
}
