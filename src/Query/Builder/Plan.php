<?php

namespace Utopia\Query\Builder;

readonly class Plan
{
    /**
     * @param  list<mixed>  $bindings
     * @param  (\Closure(Plan): (array<mixed>|int))|null  $executor
     */
    public function __construct(
        public string $query,
        public array $bindings,
        public bool $readOnly = false,
        private ?\Closure $executor = null,
    ) {
    }

    /**
     * @return array<mixed>|int
     */
    public function execute(): array|int
    {
        if ($this->executor === null) {
            throw new \BadMethodCallException('No executor configured on this plan');
        }

        return ($this->executor)($this);
    }

    public function withExecutor(\Closure $executor): self
    {
        return new self($this->query, $this->bindings, $this->readOnly, $executor);
    }
}
