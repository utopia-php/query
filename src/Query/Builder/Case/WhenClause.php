<?php

namespace Utopia\Query\Builder\Case;

readonly class WhenClause
{
    /**
     * @param  list<mixed>  $values
     * @param  list<mixed>  $rawBindings
     */
    public function __construct(
        public Kind $kind,
        public ?string $column,
        public ?string $operator,
        public mixed $value,
        public mixed $then,
        public array $values = [],
        public ?string $rawCondition = null,
        public array $rawBindings = [],
    ) {
    }
}
