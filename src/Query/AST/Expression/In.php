<?php

namespace Utopia\Query\AST\Expression;

use Utopia\Query\AST\Expression;
use Utopia\Query\AST\SelectStatement;

readonly class In implements Expression
{
    /**
     * @param Expression[]|SelectStatement $list
     */
    public function __construct(
        public Expression $expression,
        public array|SelectStatement $list,
        public bool $negated = false,
    ) {
    }
}
