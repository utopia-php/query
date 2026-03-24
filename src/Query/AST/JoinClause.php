<?php

namespace Utopia\Query\AST;

readonly class JoinClause
{
    public function __construct(
        public string $type,
        public TableRef|SubquerySource $table,
        public ?Expr $condition = null,
    ) {}
}
