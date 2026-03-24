<?php

namespace Utopia\Query\AST;

readonly class CteDefinition
{
    /**
     * @param string[] $columns
     */
    public function __construct(
        public string $name,
        public SelectStatement $query,
        public array $columns = [],
        public bool $recursive = false,
    ) {}
}
