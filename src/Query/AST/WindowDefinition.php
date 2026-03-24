<?php

namespace Utopia\Query\AST;

readonly class WindowDefinition
{
    public function __construct(
        public string $name,
        public WindowSpecification $specification,
    ) {
    }
}
