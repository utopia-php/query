<?php

namespace Utopia\Query;

class Condition
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        protected string $expression,
        protected array $bindings = [],
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
