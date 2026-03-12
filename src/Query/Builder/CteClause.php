<?php

namespace Utopia\Query\Builder;

readonly class CteClause
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $name,
        public string $query,
        public array $bindings,
        public bool $recursive,
    ) {
    }
}
