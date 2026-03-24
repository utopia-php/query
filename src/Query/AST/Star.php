<?php

namespace Utopia\Query\AST;

readonly class Star implements Expr
{
    public function __construct(
        public ?string $table = null,
    ) {}
}
