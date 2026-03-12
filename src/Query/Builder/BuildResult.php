<?php

namespace Utopia\Query\Builder;

readonly class BuildResult
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $query,
        public array $bindings,
        public bool $readOnly = false,
    ) {
    }
}
