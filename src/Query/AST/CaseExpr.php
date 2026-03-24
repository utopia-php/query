<?php

namespace Utopia\Query\AST;

readonly class CaseExpr implements Expr
{
    /**
     * @param CaseWhen[] $whens
     */
    public function __construct(
        public ?Expr $operand,
        public array $whens,
        public ?Expr $else = null,
    ) {}
}
