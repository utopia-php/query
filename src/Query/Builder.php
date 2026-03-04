<?php

namespace Utopia\Query;

use Closure;

class Builder implements Compiler
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

    private string $wrapChar = '`';

    private ?Closure $attributeResolver = null;

    /**
     * @var array<Closure>
     */
    private array $conditionProviders = [];

    /**
     * Set the collection/table name
     */
    public function from(string $table): static
    {
        $this->table = $table;

        return $this;
    }

    /**
     * Add a SELECT clause
     *
     * @param  array<string>  $columns
     */
    public function select(array $columns): static
    {
        $this->pendingQueries[] = Query::select($columns);

        return $this;
    }

    /**
     * Add filter queries
     *
     * @param  array<Query>  $queries
     */
    public function filter(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    /**
     * Add sort ascending
     */
    public function sortAsc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderAsc($attribute);

        return $this;
    }

    /**
     * Add sort descending
     */
    public function sortDesc(string $attribute): static
    {
        $this->pendingQueries[] = Query::orderDesc($attribute);

        return $this;
    }

    /**
     * Add sort random
     */
    public function sortRandom(): static
    {
        $this->pendingQueries[] = Query::orderRandom();

        return $this;
    }

    /**
     * Set LIMIT
     */
    public function limit(int $value): static
    {
        $this->pendingQueries[] = Query::limit($value);

        return $this;
    }

    /**
     * Set OFFSET
     */
    public function offset(int $value): static
    {
        $this->pendingQueries[] = Query::offset($value);

        return $this;
    }

    /**
     * Set cursor after
     */
    public function cursorAfter(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorAfter($value);

        return $this;
    }

    /**
     * Set cursor before
     */
    public function cursorBefore(mixed $value): static
    {
        $this->pendingQueries[] = Query::cursorBefore($value);

        return $this;
    }

    /**
     * Add multiple queries at once (batch mode)
     *
     * @param  array<Query>  $queries
     */
    public function queries(array $queries): static
    {
        foreach ($queries as $query) {
            $this->pendingQueries[] = $query;
        }

        return $this;
    }

    /**
     * Set the wrap character for identifiers
     */
    public function setWrapChar(string $char): static
    {
        $this->wrapChar = $char;

        return $this;
    }

    /**
     * Set an attribute resolver closure
     */
    public function setAttributeResolver(Closure $resolver): static
    {
        $this->attributeResolver = $resolver;

        return $this;
    }

    /**
     * Add a condition provider closure
     *
     * @param  Closure(string): array{0: string, 1: list<mixed>}  $provider
     */
    public function addConditionProvider(Closure $provider): static
    {
        $this->conditionProviders[] = $provider;

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

    public function union(Builder $other): static
    {
        $result = $other->build();
        $this->unions[] = [
            'type' => 'UNION',
            'query' => $result['query'],
            'bindings' => $result['bindings'],
        ];

        return $this;
    }

    public function unionAll(Builder $other): static
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
        $this->pendingQueries[] = Query::offset(($page - 1) * $perPage);

        return $this;
    }

    public function toRawSql(): string
    {
        $result = $this->build();
        $sql = $result['query'];

        foreach ($result['bindings'] as $binding) {
            if (\is_string($binding)) {
                $value = "'" . $binding . "'";
            } elseif (\is_int($binding) || \is_float($binding)) {
                $value = (string) $binding;
            } elseif (\is_bool($binding)) {
                $value = $binding ? '1' : '0';
            } else {
                $value = 'NULL';
            }
            $sql = \preg_replace('/\?/', $value, $sql, 1) ?? $sql;
        }

        return $sql;
    }

    /**
     * Build the query and bindings from accumulated state
     *
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
        $parts[] = 'FROM ' . $this->wrapIdentifier($this->table);

        // JOINS
        if (! empty($grouped['joins'])) {
            foreach ($grouped['joins'] as $joinQuery) {
                $parts[] = $this->compileJoin($joinQuery);
            }
        }

        // WHERE
        $whereClauses = [];

        // Compile filters
        foreach ($grouped['filters'] as $filter) {
            $whereClauses[] = $this->compileFilter($filter);
        }

        // Condition providers
        $providerBindings = [];
        foreach ($this->conditionProviders as $provider) {
            /** @var array{0: string, 1: list<mixed>} $result */
            $result = $provider($this->table);
            $whereClauses[] = $result[0];
            foreach ($result[1] as $binding) {
                $providerBindings[] = $binding;
            }
        }
        foreach ($providerBindings as $binding) {
            $this->addBinding($binding);
        }

        // Cursor
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

        // OFFSET
        if ($grouped['offset'] !== null) {
            $parts[] = 'OFFSET ?';
            $this->addBinding($grouped['offset']);
        }

        $sql = \implode(' ', $parts);

        // UNION
        foreach ($this->unions as $union) {
            $sql .= ' ' . $union['type'] . ' ' . $union['query'];
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
     * Get bindings from last build/compile
     *
     * @return list<mixed>
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    /**
     * Clear all accumulated state for reuse
     */
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
            Query::TYPE_ORDER_RANDOM => 'RAND()',
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

        return '_cursor ' . $operator . ' ?';
    }

    public function compileAggregate(Query $query): string
    {
        $func = \strtoupper($query->getMethod());
        $attr = $query->getAttribute();
        $col = $attr === '*' ? '*' : $this->resolveAndWrap($attr);
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

        $left = $this->resolveAndWrap($leftCol);
        $right = $this->resolveAndWrap($rightCol);

        return $type . ' ' . $table . ' ON ' . $left . ' ' . $operator . ' ' . $right;
    }

    // ── Protected (overridable) ──

    protected function resolveAttribute(string $attribute): string
    {
        if ($this->attributeResolver !== null) {
            /** @var string */
            return ($this->attributeResolver)($attribute);
        }

        return $attribute;
    }

    protected function wrapIdentifier(string $identifier): string
    {
        return $this->wrapChar . $identifier . $this->wrapChar;
    }

    protected function resolveAndWrap(string $attribute): string
    {
        return $this->wrapIdentifier($this->resolveAttribute($attribute));
    }

    // ── Private helpers ──

    private function addBinding(mixed $value): void
    {
        $this->bindings[] = $value;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileIn(string $attribute, array $values): string
    {
        $placeholders = \array_fill(0, \count($values), '?');
        foreach ($values as $value) {
            $this->addBinding($value);
        }

        return $attribute . ' IN (' . \implode(', ', $placeholders) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileNotIn(string $attribute, array $values): string
    {
        if (\count($values) === 1) {
            $this->addBinding($values[0]);

            return $attribute . ' != ?';
        }

        $placeholders = \array_fill(0, \count($values), '?');
        foreach ($values as $value) {
            $this->addBinding($value);
        }

        return $attribute . ' NOT IN (' . \implode(', ', $placeholders) . ')';
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
        /** @var string $val */
        $val = $values[0];
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
            $this->addBinding('%' . $values[0] . '%');

            return $attribute . ' LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $value . '%');
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
            $this->addBinding('%' . $value . '%');
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
            $this->addBinding('%' . $values[0] . '%');

            return $attribute . ' NOT LIKE ?';
        }

        $parts = [];
        foreach ($values as $value) {
            $this->addBinding('%' . $value . '%');
            $parts[] = $attribute . ' NOT LIKE ?';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileSearch(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);

        if ($not) {
            return 'NOT MATCH(' . $attribute . ') AGAINST(?)';
        }

        return 'MATCH(' . $attribute . ') AGAINST(?)';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' REGEXP ?';
    }

    private function compileLogical(Query $query, string $operator): string
    {
        $parts = [];
        foreach ($query->getValues() as $subQuery) {
            /** @var Query $subQuery */
            $parts[] = $this->compileFilter($subQuery);
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

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileNotExists(Query $query): string
    {
        $parts = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $parts[] = $this->resolveAndWrap($attr) . ' IS NULL';
        }

        return '(' . \implode(' AND ', $parts) . ')';
    }

    private function compileRaw(Query $query): string
    {
        foreach ($query->getValues() as $binding) {
            $this->addBinding($binding);
        }

        return $query->getAttribute();
    }
}
