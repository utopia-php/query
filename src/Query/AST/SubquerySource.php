<?php

namespace Utopia\Query\AST;

readonly class SubquerySource
{
    public function __construct(
        public SelectStatement $query,
        public string $alias,
    ) {}
}
