<?php

namespace Utopia\Query\Builder\Feature;

interface Joins
{
    public function join(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static;

    public function leftJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static;

    public function rightJoin(string $table, string $left, string $right, string $operator = '=', string $alias = ''): static;

    public function crossJoin(string $table, string $alias = ''): static;

    /**
     * @param  \Closure(\Utopia\Query\Builder\JoinBuilder): void  $callback
     */
    public function joinWhere(string $table, \Closure $callback, string $type = 'JOIN', string $alias = ''): static;
}
