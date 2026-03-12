<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\ConditionalAggregates;
use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\LateralJoins;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;

class MySQL extends SQL implements Json, Hints, ConditionalAggregates, LateralJoins
{
    /** @var list<string> */
    protected array $hints = [];

    protected string $updateJoinTable = '';

    protected string $updateJoinLeft = '';

    protected string $updateJoinRight = '';

    protected string $updateJoinAlias = '';

    protected string $deleteAlias = '';

    protected string $deleteUsingTable = '';

    protected string $deleteUsingLeft = '';

    protected string $deleteUsingRight = '';

    protected function compileRandom(): string
    {
        return 'RAND()';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' REGEXP ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSearchExpr(string $attribute, array $values, bool $not): string
    {
        /** @var string $term */
        $term = $values[0] ?? '';
        $exact = \str_ends_with($term, '"') && \str_starts_with($term, '"');

        $specialChars = '@,+,-,*,),(,<,>,~,"';
        $sanitized = \str_replace(\explode(',', $specialChars), ' ', $term);
        $sanitized = \preg_replace('/\s+/', ' ', $sanitized) ?? '';
        $sanitized = \trim($sanitized);

        if ($sanitized === '') {
            return $not ? '1 = 1' : '1 = 0';
        }

        if ($exact) {
            $sanitized = '"' . $sanitized . '"';
        } else {
            $sanitized .= '*';
        }

        $this->addBinding($sanitized);

        if ($not) {
            return 'NOT (MATCH(' . $attribute . ') AGAINST(? IN BOOLEAN MODE))';
        }

        return 'MATCH(' . $attribute . ') AGAINST(? IN BOOLEAN MODE)';
    }

    protected function compileConflictClause(): string
    {
        $updates = [];
        foreach ($this->conflictUpdateColumns as $col) {
            $wrapped = $this->resolveAndWrap($col);
            if (isset($this->conflictRawSets[$col])) {
                $updates[] = $wrapped . ' = ' . $this->conflictRawSets[$col];
                foreach ($this->conflictRawSetBindings[$col] ?? [] as $binding) {
                    $this->addBinding($binding);
                }
            } else {
                $updates[] = $wrapped . ' = VALUES(' . $wrapped . ')';
            }
        }

        return 'ON DUPLICATE KEY UPDATE ' . \implode(', ', $updates);
    }

    public function setJsonAppend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            'JSON_MERGE_PRESERVE(IFNULL(' . $this->resolveAndWrap($column) . ', JSON_ARRAY()), ?)',
            [\json_encode($values)],
        );

        return $this;
    }

    public function setJsonPrepend(string $column, array $values): static
    {
        $this->jsonSets[$column] = new Condition(
            'JSON_MERGE_PRESERVE(?, IFNULL(' . $this->resolveAndWrap($column) . ', JSON_ARRAY()))',
            [\json_encode($values)],
        );

        return $this;
    }

    public function setJsonInsert(string $column, int $index, mixed $value): static
    {
        $this->jsonSets[$column] = new Condition(
            'JSON_ARRAY_INSERT(' . $this->resolveAndWrap($column) . ', ?, ?)',
            ['$[' . $index . ']', $value],
        );

        return $this;
    }

    public function setJsonRemove(string $column, mixed $value): static
    {
        $this->jsonSets[$column] = new Condition(
            'JSON_REMOVE(' . $this->resolveAndWrap($column) . ', JSON_UNQUOTE(JSON_SEARCH(' . $this->resolveAndWrap($column) . ', \'one\', ?)))',
            [$value],
        );

        return $this;
    }

    public function setJsonIntersect(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT JSON_ARRAYAGG(val) FROM JSON_TABLE(' . $this->resolveAndWrap($column) . ', \'$[*]\' COLUMNS(val JSON PATH \'$\')) AS jt WHERE JSON_CONTAINS(?, val))', [\json_encode($values)]);

        return $this;
    }

    public function setJsonDiff(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT JSON_ARRAYAGG(val) FROM JSON_TABLE(' . $this->resolveAndWrap($column) . ', \'$[*]\' COLUMNS(val JSON PATH \'$\')) AS jt WHERE NOT JSON_CONTAINS(?, val))', [\json_encode($values)]);

        return $this;
    }

    public function setJsonUnique(string $column): static
    {
        $this->setRaw($column, '(SELECT JSON_ARRAYAGG(val) FROM (SELECT DISTINCT val FROM JSON_TABLE(' . $this->resolveAndWrap($column) . ', \'$[*]\' COLUMNS(val JSON PATH \'$\')) AS jt) AS dt)');

        return $this;
    }

    public function hint(string $hint): static
    {
        if (!\preg_match('/^[A-Za-z0-9_()= ,]+$/', $hint)) {
            throw new ValidationException('Invalid hint: ' . $hint);
        }

        $this->hints[] = $hint;

        return $this;
    }

    public function maxExecutionTime(int $ms): static
    {
        return $this->hint("MAX_EXECUTION_TIME({$ms})");
    }

    public function insertOrIgnore(): BuildResult
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        foreach ($bindings as $binding) {
            $this->addBinding($binding);
        }

        // Replace "INSERT INTO" with "INSERT IGNORE INTO"
        $sql = \preg_replace('/^INSERT INTO/', 'INSERT IGNORE INTO', $sql, 1) ?? $sql;

        return new BuildResult($sql, $this->bindings);
    }

    public function explain(bool $analyze = false, string $format = ''): BuildResult
    {
        $result = $this->build();
        $prefix = 'EXPLAIN';
        if ($analyze) {
            $prefix .= ' ANALYZE';
        }
        if ($format !== '') {
            $prefix .= ' FORMAT=' . \strtoupper($format);
        }

        return new BuildResult($prefix . ' ' . $result->query, $result->bindings, readOnly: true);
    }

    public function build(): BuildResult
    {
        $result = parent::build();

        if (! empty($this->hints)) {
            $hintStr = '/*+ ' . \implode(' ', $this->hints) . ' */';
            $query = \preg_replace('/^SELECT(\s+DISTINCT)?/', 'SELECT$1 ' . $hintStr, $result->query, 1);

            return new BuildResult($query ?? $result->query, $result->bindings, $result->readOnly);
        }

        return $result;
    }

    public function updateJoin(string $table, string $left, string $right, string $alias = ''): static
    {
        $this->updateJoinTable = $table;
        $this->updateJoinLeft = $left;
        $this->updateJoinRight = $right;
        $this->updateJoinAlias = $alias;

        return $this;
    }

    public function update(): BuildResult
    {
        foreach ($this->jsonSets as $col => $condition) {
            $this->setRaw($col, $condition->expression, $condition->bindings);
        }

        if ($this->updateJoinTable !== '') {
            $result = $this->buildUpdateJoin();
            $this->jsonSets = [];

            return $result;
        }

        $result = parent::update();
        $this->jsonSets = [];

        return $result;
    }

    private function buildUpdateJoin(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $joinTable = $this->quote($this->updateJoinTable);
        if ($this->updateJoinAlias !== '') {
            $joinTable .= ' AS ' . $this->quote($this->updateJoinAlias);
        }

        $sql = 'UPDATE ' . $this->quote($this->table)
            . ' JOIN ' . $joinTable
            . ' ON ' . $this->resolveAndWrap($this->updateJoinLeft) . ' = ' . $this->resolveAndWrap($this->updateJoinRight);

        $assignments = $this->compileAssignments();

        if (empty($assignments)) {
            throw new ValidationException('No assignments for UPDATE. Call set() or setRaw() before update().');
        }

        $sql .= ' SET ' . \implode(', ', $assignments);

        $parts = [$sql];
        $this->compileWhereClauses($parts);

        return new BuildResult(\implode(' ', $parts), $this->bindings);
    }

    public function deleteUsing(string $alias, string $table, string $left, string $right): static
    {
        $this->deleteAlias = $alias;
        $this->deleteUsingTable = $table;
        $this->deleteUsingLeft = $left;
        $this->deleteUsingRight = $right;

        return $this;
    }

    public function delete(): BuildResult
    {
        if ($this->deleteAlias !== '') {
            return $this->buildDeleteUsing();
        }

        return parent::delete();
    }

    private function buildDeleteUsing(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        $sql = 'DELETE ' . $this->quote($this->deleteAlias)
            . ' FROM ' . $this->quote($this->table) . ' AS ' . $this->quote($this->deleteAlias)
            . ' JOIN ' . $this->quote($this->deleteUsingTable)
            . ' ON ' . $this->resolveAndWrap($this->deleteUsingLeft) . ' = ' . $this->resolveAndWrap($this->deleteUsingRight);

        $parts = [$sql];
        $this->compileWhereClauses($parts);

        return new BuildResult(\implode(' ', $parts), $this->bindings);
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

    public function joinLateral(BaseBuilder $subquery, string $alias, JoinType $type = JoinType::Inner): static
    {
        $this->lateralJoins[] = new LateralJoin($subquery, $alias, $type);

        return $this;
    }

    public function leftJoinLateral(BaseBuilder $subquery, string $alias): static
    {
        return $this->joinLateral($subquery, $alias, JoinType::Left);
    }

    public function reset(): static
    {
        parent::reset();
        $this->hints = [];
        $this->jsonSets = [];
        $this->updateJoinTable = '';
        $this->updateJoinLeft = '';
        $this->updateJoinRight = '';
        $this->updateJoinAlias = '';
        $this->deleteAlias = '';
        $this->deleteUsingTable = '';
        $this->deleteUsingLeft = '';
        $this->deleteUsingRight = '';

        return $this;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialDistance(Method $method, string $attribute, array $values): string
    {
        /** @var array{0: string|array<mixed>, 1: float, 2: bool} $data */
        $data = $values[0];
        $wkt = \is_array($data[0]) ? $this->geometryToWkt($data[0]) : $data[0];
        $distance = $data[1];
        $meters = $data[2];

        $operator = match ($method) {
            Method::DistanceLessThan => '<',
            Method::DistanceGreaterThan => '>',
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            default => '<',
        };

        $this->addBinding($wkt);
        $this->addBinding($distance);

        if ($meters) {
            return 'ST_Distance(ST_SRID(' . $attribute . ', 4326), ' . $this->geomFromText(4326) . ', \'metre\') ' . $operator . ' ?';
        }

        return 'ST_Distance(ST_SRID(' . $attribute . ', 0), ' . $this->geomFromText(0) . ') ' . $operator . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string
    {
        /** @var array<mixed> $geometry */
        $geometry = $values[0];
        $wkt = $this->geometryToWkt($geometry);
        $this->addBinding($wkt);

        $expr = $function . '(' . $attribute . ', ' . $this->geomFromText(4326) . ')';

        return $not ? 'NOT ' . $expr : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSpatialCoversPredicate(string $attribute, array $values, bool $not): string
    {
        return $this->compileSpatialPredicate('ST_Contains', $attribute, $values, $not);
    }

    protected function geomFromText(int $srid): string
    {
        return "ST_GeomFromText(?, {$srid}, 'axis-order=long-lat')";
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string
    {
        $this->addBinding(\json_encode($values[0]));
        $expr = 'JSON_CONTAINS(' . $attribute . ', ?)';

        return $not ? 'NOT ' . $expr : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileJsonOverlapsExpr(string $attribute, array $values): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];
        $this->addBinding(\json_encode($arr));

        return 'JSON_OVERLAPS(' . $attribute . ', ?)';
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

        return 'JSON_EXTRACT(' . $attribute . ', \'$.' . $path . '\') ' . $operator . ' ?';
    }

}
