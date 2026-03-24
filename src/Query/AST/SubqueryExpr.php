<?php

namespace Utopia\Query\AST;

readonly class SubqueryExpr implements Expr
{
    public function __construct(
        public SelectStatement $query,
    ) {
    }
}
