<?php

namespace Utopia\Query\AST\Expression;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\SelectStatement;

readonly class Subquery implements Expression
{
    public function __construct(
        public SelectStatement $query,
    ) {
    }
}
