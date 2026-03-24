<?php

namespace Utopia\Query\AST;

readonly class CaseWhen implements Expr
{
    public function __construct(
        public Expr $condition,
        public Expr $result,
    ) {
    }
}
