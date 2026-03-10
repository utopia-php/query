<?php

namespace Utopia\Query\Builder\Case;

readonly class Expression
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $sql,
        public array $bindings,
    ) {
    }

    /**
     * @return array{sql: string, bindings: list<mixed>}
     */
    public function toSql(): array
    {
        return ['sql' => $this->sql, 'bindings' => $this->bindings];
    }
}
