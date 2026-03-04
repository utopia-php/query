<?php

namespace Utopia\Query;

use Closure;
use Utopia\Query\Hook\AttributeHook;
use Utopia\Query\Hook\FilterHook;

abstract class Builder implements Compiler
{
    protected string $table = '';

    /**
     * @var array<Query>
     */
    protected array $pendingQueries = [];

    /**
     * @var list<mixed>
     */
    protected array $bindings = [];

    /**
     * @var array<array{type: string, query: string, bindings: list<mixed>}>
     */
    protected array $unions = [];

    /** @var list<FilterHook> */
    protected array $filterHooks = [];

    /** @var list<AttributeHook> */
    protected array $attributeHooks = [];

    // ── Abstract (dialect-specific) ──

    abstract protected function wrapIdentifier(string $identifier): string;

    /**
     * Compile a random ordering expression (e.g. RAND() or rand())
     */
    abstract protected function compileRandom(): string;

    /**
     * Compile a regex filter
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileRegex(string $attribute, array $values): string;

    /**
     * Compile a full-text search filter
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileSearch(string $attribute, array $values, bool $not): string;

    // ── Hooks (overridable) ──

    protected function buildTableClause(): string
    {
        return 'FROM ' . $this->wrapIdentifier($this->table);
    }

    /**
     * Hook called after JOIN clauses, before WHERE. Override to inject e.g. PREWHERE.
     *
     * @param  array<string>  $parts
     * @param  array<string, mixed>  $grouped
     */
    protected function buildAfterJoins(array &$parts, array $grouped): void
    {
        // no-op by default
    }

    // ── Fluent API ──

    public function from(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    /**
     * @param  array<string>  $columns
     */
    public function select(array $columns): static
    {
        $this->pendingQueries[] = Query::select($columns);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function filter(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    public function sortAsc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderAsc($attribute);

        return $this;
    }

    public function sortDesc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderDesc($attribute);

        return $this;
    }

    public function sortRandom(): static
    {
        $this->pendingQueries[] = Query::orderRandom();

        return $this;
    }

    public function limit(int $value): static
    {
        $this->pendingQueries[] = Query::limit($value);

        return $this;
    }

    public function offset(int $value): static
    {
        $this->pendingQueries[] = Query::offset($value);

        return $this;
    }

    public function cursorAfter(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorAfter($value);

        return $this;
    }

    public function cursorBefore(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorBefore($value);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function queries(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    public function addHook(Hook $hook): static
    {
        if ($hook instanceof FilterHook) {
            $this->filterHooks[] = $hook;
        }
        if ($hook instanceof AttributeHook) {
            $this->attributeHooks[] = $hook;
        }

        return $this;
    }

    // ── Aggregation fluent API ──

    public function count(string $attribute = '*', string $alias = ''): static
    {
        $this->pendingQueries[] = Query::count($attribute, $alias);

        return $this;
    }

    public function sum(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::sum($attribute, $alias);

        return $this;
    }

    public function avg(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::avg($attribute, $alias);

        return $this;
    }

    public function min(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::min($attribute, $alias);

        return $this;
    }

    public function max(string $attribute, string $alias = ''): static
    {
        $this->pendingQueries[] = Query::max($attribute, $alias);

        return $this;
    }

    /**
     * @param  array<string>  $columns
     */
    public function groupBy(array $columns): static
    {
        $this->pendingQueries[] = Query::groupBy($columns);

        return $this;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function having(array $queries): static
    {
        $this->pendingQueries[] = Query::having($queries);

        return $this;
    }

    public function distinct(): static
    {
        $this->pendingQueries[] = Query::distinct();

        return $this;
    }

    // ── Join fluent API ──

    public function join(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->pendingQueries[] = Query::join($table, $left, $right, $operator);

        return $this;
    }

    public function leftJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->pendingQueries[] = Query::leftJoin($table, $left, $right, $operator);

        return $this;
    }

    public function rightJoin(string $table, string $left, string $right, string $operator = '='): static
    {
        $this->pendingQueries[] = Query::rightJoin($table, $left, $right, $operator);

        return $this;
    }

    public function crossJoin(string $table): static
    {
        $this->pendingQueries[] = Query::crossJoin($table);

        return $this;
    }

    // ── Union fluent API ──

    public function union(self $other): static
    {
        $result = $other->build();
        $this->unions[] = [
            'type' => 'UNION',
            'query' => $result['query'],
            'bindings' => $result['bindings'],
        ];

        return $this;
    }

    public function unionAll(self $other): static
    {
        $result = $other->build();
        $this->unions[] = [
            'type' => 'UNION ALL',
            'query' => $result['query'],
            'bindings' => $result['bindings'],
        ];

        return $this;
    }

    // ── Convenience methods ──

    public function when(bool $condition, Closure $callback): static
    {
        if ($condition) {
            $callback($this);
        }

        return $this;
    }

    public function page(int $page, int $perPage = 25): static
    {
        $this->pendingQueries[] = Query::limit($perPage);
        $this->pendingQueries[] = Query::offset(max(0, ($page - 1) * $perPage));

        return $this;
    }

    public function toRawSql(): string
    {
        $result = $this->build();
        $sql = $result['query'];
        $offset = 0;

        foreach ($result['bindings'] as $binding) {
            if (\is_string($binding)) {
                $value = "'" . str_replace("'", "''", $binding) . "'";
            } elseif (\is_int($binding) || \is_float($binding)) {
                $value = (string) $binding;
            } elseif (\is_bool($binding)) {
                $value = $binding ? '1' : '0';
            } else {
                $value = 'NULL';
            }

            $pos = \strpos($sql, '?', $offset);
            if ($pos !== false) {
                $sql = \substr_replace($sql, $value, $pos, 1);
                $offset = $pos + \strlen($value);
            }
        }

        return $sql;
    }

    /**
     * @return array{query: string, bindings: list<mixed>}
     */
    public function build(): array
    {
        $this->bindings = [];

        $grouped = Query::groupByType($this->pendingQueries);

        $parts = [];

        // SELECT
        $selectParts = [];

        if (! empty($grouped['aggregations'])) {
            foreach ($grouped['aggregations'] as $agg) {
                $selectParts[] = $this->compileAggregate($agg);
            }
        }

        if (! empty($grouped['selections'])) {
            $selectParts[] = $this->compileSelect($grouped['selections'][0]);
        }

        $selectSQL = ! empty($selectParts) ? \implode(', ', $selectParts) : '*';

        $selectKeyword = $grouped['distinct'] ? 'SELECT DISTINCT' : 'SELECT';
        $parts[] = $selectKeyword . ' ' . $selectSQL;

        // FROM
        $parts[] = $this->buildTableClause();

        // JOINS
        if (! empty($grouped['joins'])) {
            foreach ($grouped['joins'] as $joinQuery) {
                $parts[] = $this->compileJoin($joinQuery);
            }
        }

        // Hook: after joins (e.g. ClickHouse PREWHERE)
        $this->buildAfterJoins($parts, $grouped);

        // WHERE
        $whereClauses = [];

        foreach ($grouped['filters'] as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        foreach ($this->filterHooks as $hook) {
            $condition = $hook->filter($this->table);
            $whereClauses[] = $condition->getExpression();
            foreach ($condition->getBindings() as $binding) {
                $this->addBinding($binding);
            }
        }

        $cursorSQL = '';
        if ($grouped['cursor'] !== null && $grouped['cursorDirection'] !== null) {
            $cursorQueries = Query::getCursorQueries($this->pendingQueries, false);
            if (! empty($cursorQueries)) {
                $cursorSQL = $this->compileCursor($cursorQueries[0]);
            }
        }
        if ($cursorSQL !== '') {
            $whereClauses[] = $cursorSQL;
        }

        if (! empty($whereClauses)) {
            $parts[] = 'WHERE ' . \implode(' AND ', $whereClauses);
        }

        // GROUP BY
        if (! empty($grouped['groupBy'])) {
            $groupByCols = \array_map(
                fn (string $col): string => $this->resolveAndWrap($col),
                $grouped['groupBy']
            );
            $parts[] = 'GROUP BY ' . \implode(', ', $groupByCols);
        }

        // HAVING
        if (! empty($grouped['having'])) {
            $havingClauses = [];
            foreach ($grouped['having'] as $havingQuery) {
                foreach ($havingQuery->getValues() as $subQuery) {
                    /** @var Query $subQuery */
                    $havingClauses[] = $this->compileFilter($subQuery);
                }
            }
            if (! empty($havingClauses)) {
                $parts[] = 'HAVING ' . \implode(' AND ', $havingClauses);
            }
        }

        // ORDER BY
        $orderClauses = [];
        $orderQueries = Query::getByType($this->pendingQueries, [
            Query::TYPE_ORDER_ASC,
            Query::TYPE_ORDER_DESC,
            Query::TYPE_ORDER_RANDOM,
        ], false);
        foreach ($orderQueries as $orderQuery) {
            $orderClauses[] = $this->compileOrder($orderQuery);
        }
        if (! empty($orderClauses)) {
            $parts[] = 'ORDER BY ' . \implode(', ', $orderClauses);
        }

        // LIMIT
        if ($grouped['limit'] !== null) {
            $parts[] = 'LIMIT ?';
            $this->addBinding($grouped['limit']);
        }

        // OFFSET (only emit if LIMIT is also present)
        if ($grouped['offset'] !== null && $grouped['limit'] !== null) {
            $parts[] = 'OFFSET ?';
            $this->addBinding($grouped['offset']);
        }

        $sql = \implode(' ', $parts);

        // UNION
        if (!empty($this->unions)) {
            $sql = '(' . $sql . ')';
        }
        foreach ($this->unions as $union) {
            $sql .= ' ' . $union['type'] . ' (' . $union['query'] . ')';
            foreach ($union['bindings'] as $binding) {
                $this->addBinding($binding);
            }
        }

        return [
            'query' => $sql,
            'bindings' => $this->bindings,
        ];
    }

    /**
     * @return list<mixed>
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    public function reset(): static
    {
        $this->pendingQueries = [];
        $this->bindings = [];
        $this->table = '';
        $this->unions = [];

        return $this;
    }

    // ── Compiler interface ──

    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAndWrap($query->getAttribute());
        $values = $query->getValues();

        return match ($method) {
            Query::TYPE_EQUAL => $this->compileIn($attribute, $values),
            Query::TYPE_NOT_EQUAL => $this->compileNotIn($attribute, $values),
            Query::TYPE_LESSER => $this->compileComparison($attribute, '<', $values),
            Query::TYPE_LESSER_EQUAL => $this->compileComparison($attribute, '<=', $values),
            Query::TYPE_GREATER => $this->compileComparison($attribute, '>', $values),
            Query::TYPE_GREATER_EQUAL => $this->compileComparison($attribute, '>=', $values),
            Query::TYPE_BETWEEN => $this->compileBetween($attribute, $values, false),
            Query::TYPE_NOT_BETWEEN => $this->compileBetween($attribute, $values, true),
            Query::TYPE_STARTS_WITH => $this->compileLike($attribute, $values, '', '%', false),
            Query::TYPE_NOT_STARTS_WITH => $this->compileLike($attribute, $values, '', '%', true),
            Query::TYPE_ENDS_WITH => $this->compileLike($attribute, $values, '%', '', false),
            Query::TYPE_NOT_ENDS_WITH => $this->compileLike($attribute, $values, '%', '', true),
            Query::TYPE_CONTAINS => $this->compileContains($attribute, $values),
            Query::TYPE_CONTAINS_ANY => $this->compileIn($attribute, $values),
            Query::TYPE_CONTAINS_ALL => $this->compileContainsAll($attribute, $values),
            Query::TYPE_NOT_CONTAINS => $this->compileNotContains($attribute, $values),
            Query::TYPE_SEARCH => $this->compileSearch($attribute, $values, false),
            Query::TYPE_NOT_SEARCH => $this->compileSearch($attribute, $values, true),
            Query::TYPE_REGEX => $this->compileRegex($attribute, $values),
            Query::TYPE_IS_NULL => $attribute . ' IS NULL',
            Query::TYPE_IS_NOT_NULL => $attribute . ' IS NOT NULL',
            Query::TYPE_AND => $this->compileLogical($query, 'AND'),
            Query::TYPE_OR => $this->compileLogical($query, 'OR'),
            Query::TYPE_HAVING => $this->compileLogical($query, 'AND'),
            Query::TYPE_EXISTS => $this->compileExists($query),
            Query::TYPE_NOT_EXISTS => $this->compileNotExists($query),
            Query::TYPE_RAW => $this->compileRaw($query),
            default => throw new Exception('Unsupported filter type: ' . $method),
        };
    }

    public function compileOrder(Query $query): string
    {
        return match ($query->getMethod()) {
            Query::TYPE_ORDER_ASC => $this->resolveAndWrap($query->getAttribute()) . ' ASC',
            Query::TYPE_ORDER_DESC => $this->resolveAndWrap($query->getAttribute()) . ' DESC',
            Query::TYPE_ORDER_RANDOM => $this->compileRandom(),
            default => throw new Exception('Unsupported order type: ' . $query->getMethod()),
        };
    }

    public function compileLimit(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'LIMIT ?';
    }

    public function compileOffset(Query $query): string
    {
        $this->addBinding($query->getValue());

        return 'OFFSET ?';
    }

    public function compileSelect(Query $query): string
    {
        /** @var array<string> $values */
        $values = $query->getValues();
        $columns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $values
        );

        return \implode(', ', $columns);
    }

    public function compileCursor(Query $query): string
    {
        $value = $query->getValue();
        $this->addBinding($value);

        $operator = $query->getMethod() === Query::TYPE_CURSOR_AFTER ? '>' : '<';

        return $this->wrapIdentifier('_cursor') . ' ' . $operator . ' ?';
    }

    public function compileAggregate(Query $query): string
    {
        $funcMap = [
            Query::TYPE_COUNT => 'COUNT',
            Query::TYPE_SUM   => 'SUM',
            Query::TYPE_AVG   => 'AVG',
            Query::TYPE_MIN   => 'MIN',
            Query::TYPE_MAX   => 'MAX',
        ];
        $func = $funcMap[$query->getMethod()] ?? throw new \InvalidArgumentException("Unknown aggregate: {$query->getMethod()}");
        $attr = $query->getAttribute();
        $col = ($attr === '*' || $attr === '') ? '*' : $this->resolveAndWrap($attr);
        /** @var string $alias */
        $alias = $query->getValue('');
        $sql = $func . '(' . $col . ')';

        if ($alias !== '') {
            $sql .= ' AS ' . $this->wrapIdentifier($alias);
        }

        return $sql;
    }

    public function compileGroupBy(Query $query): string
    {
        /** @var array<string> $values */
        $values = $query->getValues();
        $columns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $values
        );

        return \implode(', ', $columns);
    }

    public function compileJoin(Query $query): string
    {
        $type = match ($query->getMethod()) {
            Query::TYPE_JOIN => 'JOIN',
            Query::TYPE_LEFT_JOIN => 'LEFT JOIN',
            Query::TYPE_RIGHT_JOIN => 'RIGHT JOIN',
            Query::TYPE_CROSS_JOIN => 'CROSS JOIN',
            default => throw new Exception('Unsupported join type: ' . $query->getMethod()),
        };

        $table = $this->wrapIdentifier($query->getAttribute());
        $values = $query->getValues();

        if (empty($values)) {
            return $type . ' ' . $table;
        }

        /** @var string $leftCol */
        $leftCol = $values[0];
        /** @var string $operator */
        $operator = $values[1];
        /** @var string $rightCol */
        $rightCol = $values[2];

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        if (!\in_array($operator, $allowedOperators, true)) {
            throw new \InvalidArgumentException('Invalid join operator: ' . $operator);
        }

        $left = $this->resolveAndWrap($leftCol);
        $right = $this->resolveAndWrap($rightCol);

        return $type . ' ' . $table . ' ON ' . $left . ' ' . $operator . ' ' . $right;
    }

    // ── Protected helpers ──

    protected function resolveAttribute(string $attribute): string
    {
        foreach ($this->attributeHooks as $hook) {
            $attribute = $hook->resolve($attribute);
        }

        return $attribute;
    }

    protected function resolveAndWrap(string $attribute): string
    {
        return $this->wrapIdentifier($this->resolveAttribute($attribute));
    }

    protected function addBinding(mixed $value): void
    {
        $this->bindings[] = $value;
    }

    // ── Private helpers (shared SQL syntax) ──

    /**
     * @param  array<mixed>  $values
     */
    private function compileIn(string $attribute, array $values): string
    {
        if ($values === []) {
            return '1 = 0';
        }

        $hasNulls = false;
        $nonNulls = [];

        foreach ($values as $value) {
            if ($value === null) {
                $hasNulls = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        $hasNonNulls = $nonNulls !== [];

        if ($hasNulls && ! $hasNonNulls) {
            return $attribute . ' IS NULL';
        }

        $placeholders = \array_fill(0, \count($nonNulls), '?');
        foreach ($nonNulls as $value) {
            $this->addBinding($value);
        }
        $inClause = $attribute . ' IN (' . \implode(', ', $placeholders) . ')';

        if ($hasNulls) {
            return '(' . $inClause . ' OR ' . $attribute . ' IS NULL)';
        }

        return $inClause;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileNotIn(string $attribute, array $values): string
    {
        if ($values === []) {
            return '1 = 1';
        }

        $hasNulls = false;
        $nonNulls = [];

        foreach ($values as $value) {
            if ($value === null) {
                $hasNulls = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        $hasNonNulls = $nonNulls !== [];

        if ($hasNulls && ! $hasNonNulls) {
            return $attribute . ' IS NOT NULL';
        }

        if (\count($nonNulls) === 1) {
            $this->addBinding($nonNulls[0]);
            $notClause = $attribute . ' != ?';
        } else {
            $placeholders = \array_fill(0, \count($nonNulls), '?');
            foreach ($nonNulls as $value) {
                $this->addBinding($value);
            }
            $notClause = $attribute . ' NOT IN (' . \implode(', ', $placeholders) . ')';
        }

        if ($hasNulls) {
            return '(' . $notClause . ' AND ' . $attribute . ' IS NOT NULL)';
        }

        return $notClause;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileComparison(string $attribute, string $operator, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' ' . $operator . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileBetween(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);
        $this->addBinding($values[1]);
        $keyword = $not ? 'NOT BETWEEN' : 'BETWEEN';

        return $attribute . ' ' . $keyword . ' ? AND ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileLike(string $attribute, array $values, string $prefix, string $suffix, bool $not): string
    {
        /** @var string $rawVal */
        $rawVal = $values[0];
        $val = $this->escapeLikeValue($rawVal);
        $this->addBinding($prefix . $val . $suffix);
        $keyword = $not ? 'NOT LIKE' : 'LIKE';

        return $attribute . ' ' . $keyword . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' LIKE ?';
        }

        return '(' . \implode(' OR ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileContainsAll(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' LIKE ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileNotContains(string $attribute, array $values): string
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $this->addBinding('%' . $this->escapeLikeValue($values[0]) . '%');

            return $attribute . ' NOT LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $this->escapeLikeValue($value) . '%');
            $parts[] = $attribute . ' NOT LIKE ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * Escape LIKE metacharacters in user input before wrapping with wildcards.
     */
    private function escapeLikeValue(string $value): string
    {
        return \str_replace(['\\', '%', '_'], ['\\\\', '\\%', '\\_'], $value);
    }

    private function compileLogical(Query $query, string $operator): string
    {
        $parts = [];
        foreach ($query->getValues() as $subQuery) {
            /** @var Query $subQuery */
            $parts[] = $this->compileFilter($subQuery);
        }

        if ($parts === []) {
            return $operator === 'OR' ? '1 = 0' : '1 = 1';
        }

        return '(' . \implode(' ' . $operator . ' ', $parts) . ')';
    }

    private function compileExists(Query $query): string
    {
        $parts = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $parts[] = $this->resolveAndWrap($attr) . ' IS NOT NULL';
        }

        if ($parts === []) {
            return '1 = 1';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileNotExists(Query $query): string
    {
        $parts = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $parts[] = $this->resolveAndWrap($attr) . ' IS NULL';
        }

        if ($parts === []) {
            return '1 = 1';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileRaw(Query $query): string
    {
        $attribute = $query->getAttribute();

        if ($attribute === '') {
            return '1 = 1';
        }

        foreach ($query->getValues() as $binding) {
            $this->addBinding($binding);
        }

        return $attribute;
    }
}
