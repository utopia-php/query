<?php

namespace Utopia\Query\Builder\Feature;

use Closure;
use Utopia\Query\Builder\BuildResult;

interface Selects
{
    public function from(string $table, string $alias = ''): static;

    /**
     * @param  array<string>  $columns
     */
    public function select(array $columns): static;

    /**
     * @param  list<mixed>  $bindings
     */
    public function selectRaw(string $expression, array $bindings = []): static;

    public function distinct(): static;

    /**
     * @param  array<\Utopia\Query\Query>  $queries
     */
    public function filter(array $queries): static;

    /**
     * @param  array<\Utopia\Query\Query>  $queries
     */
    public function queries(array $queries): static;

    public function sortAsc(string $attribute): static;

    public function sortDesc(string $attribute): static;

    public function sortRandom(): static;

    public function limit(int $value): static;

    public function offset(int $value): static;

    public function page(int $page, int $perPage = 25): static;

    public function cursorAfter(mixed $value): static;

    public function cursorBefore(mixed $value): static;

    public function when(bool $condition, Closure $callback): static;

    public function build(): BuildResult;

    public function toRawSql(): string;

    /**
     * @return list<mixed>
     */
    public function getBindings(): array;

    public function reset(): static;
}
