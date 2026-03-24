<?php

namespace Utopia\Query\AST;

readonly class UnaryExpr implements Expr
{
    public function __construct(
        public string $operator,
        public Expr $operand,
        public bool $prefix = true,
    ) {}
}
