<?php

namespace Utopia\Query\AST;

readonly class BinaryExpr implements Expr
{
    public function __construct(
        public Expr $left,
        public string $operator,
        public Expr $right,
    ) {}
}
