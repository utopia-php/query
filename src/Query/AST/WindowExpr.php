<?php

namespace Utopia\Query\AST;

readonly class WindowExpr implements Expr
{
    public function __construct(
        public Expr $function,
        public ?string $windowName = null,
        public ?WindowSpec $spec = null,
    ) {
    }
}
