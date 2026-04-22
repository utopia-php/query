<?php

namespace Utopia\Query\Builder\Case;

readonly class Result
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $sql,
        public array $bindings,
    ) {
    }
}
