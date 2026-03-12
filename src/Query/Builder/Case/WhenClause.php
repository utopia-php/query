<?php

namespace Utopia\Query\Builder\Case;

readonly class WhenClause
{
    /**
     * @param  list<mixed>  $conditionBindings
     * @param  list<mixed>  $resultBindings
     */
    public function __construct(
        public string $condition,
        public string $result,
        public array $conditionBindings,
        public array $resultBindings,
    ) {
    }
}
