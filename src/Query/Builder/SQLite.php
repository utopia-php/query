<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;

class SQLite extends SQL implements Json, ConditionalAggregates
{
    /** @var array<string, Condition> */
    protected array $jsonSets = [];

    protected function compileRandom(): string
    {
        return 'RANDOM()';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        throw new UnsupportedException('REGEXP is not natively supported in SQLite.');
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSearchExpr(string $attribute, array $values, bool $not): string
    {
        throw new UnsupportedException('Full-text search is not supported in the SQLite query builder.');
    }

    protected function compileConflictClause(): string
    {
        $wrappedKeys = \array_map(
            fn (string $key): string => $this->resolveAndWrap($key),
            $this->conflictKeys
        );

        $updates = [];
        foreach ($this->conflictUpdateColumns as $col) {
            $wrapped = $this->resolveAndWrap($col);
            if (isset($this->conflictRawSets[$col])) {
                $updates[] = $wrapped . ' = ' . $this->conflictRawSets[$col];
                foreach ($this->conflictRawSetBindings[$col] ?? [] as $binding) {
                    $this->addBinding($binding);
                }
            } else {
                $updates[] = $wrapped . ' = excluded.' . $wrapped;
            }
        }

        return 'ON CONFLICT (' . \implode(', ', $wrappedKeys) . ') DO UPDATE SET ' . \implode(', ', $updates);
    }

    public function insertOrIgnore(): BuildResult
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        foreach ($bindings as $binding) {
            $this->addBinding($binding);
        }

        $sql = \preg_replace('/^INSERT INTO/', 'INSERT OR IGNORE INTO', $sql, 1) ?? $sql;

        return new BuildResult($sql, $this->bindings);
    }

    public function setJsonAppend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            'json_group_array(value) FROM (SELECT value FROM json_each(IFNULL(' . $this->resolveAndWrap($column) . ', \'[]\')) UNION ALL SELECT value FROM json_each(?))',
            [\json_encode($values)],
        );

        return $this;
    }

    public function setJsonPrepend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            'json_group_array(value) FROM (SELECT value FROM json_each(?) UNION ALL SELECT value FROM json_each(IFNULL(' . $this->resolveAndWrap($column) . ', \'[]\')))',
            [\json_encode($values)],
        );

        return $this;
    }

    public function setJsonInsert(string $column, int $index, mixed $value): static
    {
        $this->jsonSets[$column] = new Condition(
            'json_insert(' . $this->resolveAndWrap($column) . ', \'$[' . $index . ']\', json(?))',
            [\json_encode($value)],
        );

        return $this;
    }

    public function setJsonRemove(string $column, mixed $value): static
    {
        $wrapped = $this->resolveAndWrap($column);
        $this->jsonSets[$column] = new Condition(
            '(SELECT json_group_array(value) FROM json_each(' . $wrapped . ') WHERE value != json(?))',
            [\json_encode($value)],
        );

        return $this;
    }

    public function setJsonIntersect(string $column, array $values): static
    {
        $wrapped = $this->resolveAndWrap($column);
        $this->setRaw($column, '(SELECT json_group_array(value) FROM json_each(IFNULL(' . $wrapped . ', \'[]\')) WHERE value IN (SELECT value FROM json_each(?)))', [\json_encode($values)]);

        return $this;
    }

    public function setJsonDiff(string $column, array $values): static
    {
        $wrapped = $this->resolveAndWrap($column);
        $this->setRaw($column, '(SELECT json_group_array(value) FROM json_each(IFNULL(' . $wrapped . ', \'[]\')) WHERE value NOT IN (SELECT value FROM json_each(?)))', [\json_encode($values)]);

        return $this;
    }

    public function setJsonUnique(string $column): static
    {
        $wrapped = $this->resolveAndWrap($column);
        $this->setRaw($column, '(SELECT json_group_array(DISTINCT value) FROM json_each(IFNULL(' . $wrapped . ', \'[]\')))');

        return $this;
    }

    public function update(): BuildResult
    {
        foreach ($this->jsonSets as $col => $condition) {
            $this->setRaw($col, $condition->expression, $condition->bindings);
        }

        $result = parent::update();
        $this->jsonSets = [];

        return $result;
    }

    public function countWhen(string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'COUNT(CASE WHEN ' . $condition . ' THEN 1 END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->selectRaw($expr, \array_values($bindings));
    }

    public function sumWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'SUM(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->selectRaw($expr, \array_values($bindings));
    }

    public function avgWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'AVG(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->selectRaw($expr, \array_values($bindings));
    }

    public function minWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MIN(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->selectRaw($expr, \array_values($bindings));
    }

    public function maxWhen(string $column, string $condition, string $alias = '', mixed ...$bindings): static
    {
        $expr = 'MAX(CASE WHEN ' . $condition . ' THEN ' . $this->resolveAndWrap($column) . ' END)';
        if ($alias !== '') {
            $expr .= ' AS ' . $this->quote($alias);
        }

        return $this->selectRaw($expr, \array_values($bindings));
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialDistance(Method $method, string $attribute, array $values): string
    {
        throw new UnsupportedException('Spatial distance queries are not supported in SQLite.');
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string
    {
        throw new UnsupportedException('Spatial predicates are not supported in SQLite.');
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialCoversPredicate(string $attribute, array $values, bool $not): string
    {
        throw new UnsupportedException('Spatial covers predicates are not supported in SQLite.');
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];
        $placeholders = [];
        foreach ((array) $arr as $item) {
            $this->addBinding(\json_encode($item));
            $placeholders[] = '?';
        }

        $conditions = \array_map(
            fn (string $p) => 'EXISTS (SELECT 1 FROM json_each(' . $attribute . ') WHERE json_each.value = json(' . $p . '))',
            $placeholders
        );

        $expr = '(' . \implode(' AND ', $conditions) . ')';

        return $not ? 'NOT ' . $expr : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileJsonOverlapsExpr(string $attribute, array $values): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];
        $placeholders = [];
        foreach ((array) $arr as $item) {
            $this->addBinding(\json_encode($item));
            $placeholders[] = '?';
        }

        $conditions = \array_map(
            fn (string $p) => 'EXISTS (SELECT 1 FROM json_each(' . $attribute . ') WHERE json_each.value = json(' . $p . '))',
            $placeholders
        );

        return '(' . \implode(' OR ', $conditions) . ')';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileJsonPathExpr(string $attribute, array $values): string
    {
        /** @var string $path */
        $path = $values[0];
        /** @var string $operator */
        $operator = $values[1];
        $value = $values[2];

        if (!\preg_match('/^[a-zA-Z0-9_.\[\]]+$/', $path)) {
            throw new ValidationException('Invalid JSON path: ' . $path);
        }

        $allowedOperators = ['=', '!=', '<', '>', '<=', '>=', '<>'];
        if (!\in_array($operator, $allowedOperators, true)) {
            throw new ValidationException('Invalid JSON path operator: ' . $operator);
        }

        $this->addBinding($value);

        return 'json_extract(' . $attribute . ', \'$.' . $path . '\') ' . $operator . ' ?';
    }

    public function reset(): static
    {
        parent::reset();
        $this->jsonSets = [];

        return $this;
    }
}
