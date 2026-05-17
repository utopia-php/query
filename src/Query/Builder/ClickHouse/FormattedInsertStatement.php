<?php

namespace Utopia\Query\Builder\ClickHouse;

use Utopia\Query\Builder\Statement;

readonly class FormattedInsertStatement extends Statement
{
    /**
     * @param  list<string>  $columns
     * @param  list<mixed>  $bindings
     * @param  (\Closure(Statement): (array<mixed>|int))|null  $executor
     */
    public function __construct(
        string $query,
        array $bindings,
        public array $columns,
        public string $format,
        bool $readOnly = false,
        ?\Closure $executor = null,
    ) {
        parent::__construct($query, $bindings, $readOnly, $executor);
    }
}
