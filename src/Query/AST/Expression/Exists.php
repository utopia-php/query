<?php

namespace Utopia\Query\AST\Expression;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\SelectStatement;

readonly class Exists implements Expression
{
    public function __construct(
        public SelectStatement $subquery,
        public bool $negated = false,
    ) {
    }
}
