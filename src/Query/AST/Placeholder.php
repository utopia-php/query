<?php

namespace Utopia\Query\AST;

readonly class Placeholder implements Expr
{
    public function __construct(
        public string $value,
    ) {
    }
}
