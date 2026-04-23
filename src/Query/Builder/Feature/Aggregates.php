<?php

namespace Utopia\Query\Builder\Feature;

interface Aggregates
{
    public function count(string $attribute = '*', string $alias = ''): static;

    public function countDistinct(string $attribute, string $alias = ''): static;

    public function sum(string $attribute, string $alias = ''): static;

    public function avg(string $attribute, string $alias = ''): static;

    public function min(string $attribute, string $alias = ''): static;

    public function max(string $attribute, string $alias = ''): static;

    /**
     * @param  array<string>  $columns
     */
    public function groupBy(array $columns): static;

    /**
     * @param  array<\Utopia\Query\Query>  $queries
     */
    public function having(array $queries): static;
}
