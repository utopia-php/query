<?php

namespace Utopia\Query\AST;

readonly class OrderByItem
{
    public function __construct(
        public Expression $expression,
        public string $direction = 'ASC',
        public ?string $nulls = null,
    ) {
    }
}
