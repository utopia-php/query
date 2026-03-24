<?php

namespace Utopia\Query\AST;

readonly class Literal implements Expr
{
    public function __construct(
        public string|int|float|bool|null $value,
    ) {}
}
