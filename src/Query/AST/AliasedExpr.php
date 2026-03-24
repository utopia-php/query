<?php

namespace Utopia\Query\AST;

readonly class AliasedExpr implements Expr
{
    public function __construct(
        public Expr $expr,
        public string $alias,
    ) {
    }
}
