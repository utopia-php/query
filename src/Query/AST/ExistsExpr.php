<?php

namespace Utopia\Query\AST;

readonly class ExistsExpr implements Expr
{
    public function __construct(
        public SelectStatement $subquery,
        public bool $negated = false,
    ) {}
}
