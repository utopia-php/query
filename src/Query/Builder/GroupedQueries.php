<?php

namespace Utopia\Query\Builder;

use Utopia\Query\CursorDirection;
use Utopia\Query\OrderDirection;
use Utopia\Query\Query;

readonly class GroupedQueries
{
    /**
     * @param  list<Query>  $filters
     * @param  list<Query>  $selections
     * @param  list<Query>  $aggregations
     * @param  list<string>  $groupBy
     * @param  list<Query>  $having
     * @param  list<Query>  $joins
     * @param  list<Query>  $unions
     * @param  array<string>  $orderAttributes
     * @param  array<OrderDirection>  $orderTypes
     */
    public function __construct(
        public array $filters = [],
        public array $selections = [],
        public array $aggregations = [],
        public array $groupBy = [],
        public array $having = [],
        public bool $distinct = false,
        public array $joins = [],
        public array $unions = [],
        public ?int $limit = null,
        public ?int $offset = null,
        public array $orderAttributes = [],
        public array $orderTypes = [],
        public mixed $cursor = null,
        public ?CursorDirection $cursorDirection = null,
    ) {
    }
}
