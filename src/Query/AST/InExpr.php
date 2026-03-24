<?php

namespace Utopia\Query\AST;

readonly class InExpr implements Expr
{
    /**
     * @param Expr[]|SelectStatement $list
     */
    public function __construct(
        public Expr $expr,
        public array|SelectStatement $list,
        public bool $negated = false,
    ) {
    }
}
