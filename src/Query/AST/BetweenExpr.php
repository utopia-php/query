<?php

namespace Utopia\Query\AST;

readonly class BetweenExpr implements Expr
{
    public function __construct(
        public Expr $expr,
        public Expr $low,
        public Expr $high,
        public bool $negated = false,
    ) {}
}
