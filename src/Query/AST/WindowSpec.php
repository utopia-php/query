<?php

namespace Utopia\Query\AST;

readonly class WindowSpec
{
    /**
     * @param Expr[] $partitionBy
     * @param OrderByItem[] $orderBy
     */
    public function __construct(
        public array $partitionBy = [],
        public array $orderBy = [],
        public ?string $frameType = null,
        public ?string $frameStart = null,
        public ?string $frameEnd = null,
    ) {}
}
