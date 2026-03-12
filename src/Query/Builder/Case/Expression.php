<?php

namespace Utopia\Query\Builder\Case;

readonly class Expression
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
