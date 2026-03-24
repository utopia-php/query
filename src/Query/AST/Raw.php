<?php

namespace Utopia\Query\AST;

readonly class Raw implements Expr
{
    public function __construct(
        public string $sql,
    ) {
    }
}
