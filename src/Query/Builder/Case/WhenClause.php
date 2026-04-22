<?php

namespace Utopia\Query\Builder\Case;

use Utopia\Query\Method;

readonly class WhenClause
{
    /**
     * @param  list<mixed>  $values
     * @param  list<mixed>  $rawBindings
     */
    public function __construct(
        public Kind $kind,
        public ?string $column,
        public ?Method $operator,
        public mixed $value,
        public mixed $then,
        public array $values = [],
        public ?string $rawCondition = null,
        public array $rawBindings = [],
    ) {
    }
}
