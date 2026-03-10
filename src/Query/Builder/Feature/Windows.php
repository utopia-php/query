<?php

namespace Utopia\Query\Builder\Feature;

interface Windows
{
    /**
     * Add a window function to the SELECT clause.
     *
     * @param  string  $function  Raw expression: 'ROW_NUMBER()', 'RANK()', 'LAG(col, 1)', 'SUM(amount)'
     * @param  string  $alias  Column alias for the result
     * @param  list<string>|null  $partitionBy  Columns for PARTITION BY
     * @param  list<string>|null  $orderBy  Columns for ORDER BY (prefix with - for DESC)
     */
    public function selectWindow(string $function, string $alias, ?array $partitionBy = null, ?array $orderBy = null): static;
}
