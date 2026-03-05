<?php

namespace Utopia\Query\Builder;

readonly class Condition
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $expression,
        public array $bindings = [],
    ) {
    }

    public function getExpression(): string
    {
        return $this->expression;
    }

    /** @return list<mixed> */
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
