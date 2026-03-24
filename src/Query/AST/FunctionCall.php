<?php

namespace Utopia\Query\AST;

readonly class FunctionCall implements Expression
{
    /**
     * @param Expression[] $arguments
     */
    public function __construct(
        public string $name,
        public array $arguments = [],
        public bool $distinct = false,
        public ?Expression $filter = null,
    ) {
    }
}
