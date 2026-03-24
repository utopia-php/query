<?php

namespace Utopia\Query\AST;

readonly class FunctionCall implements Expr
{
    /**
     * @param Expr[] $arguments
     */
    public function __construct(
        public string $name,
        public array $arguments = [],
        public bool $distinct = false,
        public ?Expr $filter = null,
    ) {}
}
