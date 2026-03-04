<?php

namespace Utopia\Query\Hook;

use Utopia\Query\Condition;

class TenantFilterHook implements FilterHook
{
    /**
     * @param  list<string>  $tenantIds
     */
    public function __construct(
        protected array $tenantIds,
        protected string $column = '_tenant',
    ) {
    }

    public function filter(string $table): Condition
    {
        $placeholders = implode(', ', array_fill(0, count($this->tenantIds), '?'));

        return new Condition(
            "{$this->column} IN ({$placeholders})",
            $this->tenantIds,
        );
    }
}
