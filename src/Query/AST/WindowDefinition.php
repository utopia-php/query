<?php

namespace Utopia\Query\AST;

use Utopia\Query\AST\Specification\Window;

readonly class WindowDefinition
{
    public function __construct(
        public string $name,
        public Window $specification,
    ) {
    }
}
