<?php

namespace Utopia\Query\AST;

readonly class CastExpr implements Expr
{
    public function __construct(
        public Expr $expr,
        public string $type,
    ) {
    }
}
