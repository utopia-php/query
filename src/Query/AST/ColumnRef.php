<?php

namespace Utopia\Query\AST;

readonly class ColumnRef implements Expr
{
    public function __construct(
        public string $name,
        public ?string $table = null,
        public ?string $schema = null,
    ) {}
}
