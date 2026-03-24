<?php

namespace Utopia\Query\AST;

readonly class TableRef
{
    public function __construct(
        public string $name,
        public ?string $alias = null,
        public ?string $schema = null,
    ) {}
}
