<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder\Feature\Hints;
use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;

class MySQL extends SQL implements Spatial, Json, Hints
{
    /** @var list<string> */
    protected array $hints = [];

    /** @var array<string, array{expression: string, bindings: list<mixed>}> */
    protected array $jsonSets = [];

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
    protected function compileSearch(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);

        if ($not) {
            return 'NOT (MATCH(' . $attribute . ') AGAINST(?))';
        }

        return 'MATCH(' . $attribute . ') AGAINST(?)';
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

    public function filterDistance(string $attribute, array $point, string $operator, float $distance, bool $meters = false): static
    {
        $wkt = 'POINT(' . (float) $point[0] . ' ' . (float) $point[1] . ')';
        $method = match ($operator) {
            '<' => Method::DistanceLessThan,
            '>' => Method::DistanceGreaterThan,
            '=' => Method::DistanceEqual,
            '!=' => Method::DistanceNotEqual,
            default => Method::DistanceLessThan,
        };

        $this->pendingQueries[] = new Query($method, $attribute, [[$wkt, $distance, $meters]]);

        return $this;
    }

    public function filterIntersects(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::intersects($attribute, $geometry);

        return $this;
    }

    public function filterNotIntersects(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notIntersects($attribute, $geometry);

        return $this;
    }

    public function filterCrosses(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::crosses($attribute, $geometry);

        return $this;
    }

    public function filterNotCrosses(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notCrosses($attribute, $geometry);

        return $this;
    }

    public function filterOverlaps(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::overlaps($attribute, $geometry);

        return $this;
    }

    public function filterNotOverlaps(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notOverlaps($attribute, $geometry);

        return $this;
    }

    public function filterTouches(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::touches($attribute, $geometry);

        return $this;
    }

    public function filterNotTouches(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notTouches($attribute, $geometry);

        return $this;
    }

    public function filterCovers(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::covers($attribute, $geometry);

        return $this;
    }

    public function filterNotCovers(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notCovers($attribute, $geometry);

        return $this;
    }

    public function filterSpatialEquals(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::spatialEquals($attribute, $geometry);

        return $this;
    }

    public function filterNotSpatialEquals(string $attribute, array $geometry): static
    {
        $this->pendingQueries[] = Query::notSpatialEquals($attribute, $geometry);

        return $this;
    }

    public function filterJsonContains(string $attribute, mixed $value): static
    {
        $this->pendingQueries[] = Query::jsonContains($attribute, $value);

        return $this;
    }

    public function filterJsonNotContains(string $attribute, mixed $value): static
    {
        $this->pendingQueries[] = Query::jsonNotContains($attribute, $value);

        return $this;
    }

    public function filterJsonOverlaps(string $attribute, array $values): static
    {
        $this->pendingQueries[] = Query::jsonOverlaps($attribute, $values);

        return $this;
    }

    public function filterJsonPath(string $attribute, string $path, string $operator, mixed $value): static
    {
        $this->pendingQueries[] = Query::jsonPath($attribute, $path, $operator, $value);

        return $this;
    }

    public function setJsonAppend(string $column, array $values): static
    {
        $this->jsonSets[$column] = [
            'expression' => 'JSON_MERGE_PRESERVE(IFNULL(' . $this->resolveAndWrap($column) . ', JSON_ARRAY()), ?)',
            'bindings' => [\json_encode($values)],
        ];

        return $this;
    }

    public function setJsonPrepend(string $column, array $values): static
    {
        $this->jsonSets[$column] = [
            'expression' => 'JSON_MERGE_PRESERVE(?, IFNULL(' . $this->resolveAndWrap($column) . ', JSON_ARRAY()))',
            'bindings' => [\json_encode($values)],
        ];

        return $this;
    }

    public function setJsonInsert(string $column, int $index, mixed $value): static
    {
        $this->jsonSets[$column] = [
            'expression' => 'JSON_ARRAY_INSERT(' . $this->resolveAndWrap($column) . ', ?, ?)',
            'bindings' => ['$[' . $index . ']', $value],
        ];

        return $this;
    }

    public function setJsonRemove(string $column, mixed $value): static
    {
        $this->jsonSets[$column] = [
            'expression' => 'JSON_REMOVE(' . $this->resolveAndWrap($column) . ', JSON_UNQUOTE(JSON_SEARCH(' . $this->resolveAndWrap($column) . ', \'one\', ?)))',
            'bindings' => [$value],
        ];

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

    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAndWrap($query->getAttribute());

        if ($method->isSpatial()) {
            return $this->compileSpatialFilter($method, $attribute, $query);
        }

        if ($method->isJson()) {
            return $this->compileJsonFilter($method, $attribute, $query);
        }

        return parent::compileFilter($query);
    }

    public function build(): BuildResult
    {
        $result = parent::build();

        if (! empty($this->hints)) {
            $hintStr = '/*+ ' . \implode(' ', $this->hints) . ' */';
            $query = \preg_replace('/^SELECT(\s+DISTINCT)?/', 'SELECT$1 ' . $hintStr, $result->query, 1);

            return new BuildResult($query ?? $result->query, $result->bindings);
        }

        return $result;
    }

    public function update(): BuildResult
    {
        // Apply JSON sets as rawSets before calling parent
        foreach ($this->jsonSets as $col => $data) {
            $this->setRaw($col, $data['expression'], $data['bindings']);
        }

        $result = parent::update();
        $this->jsonSets = [];

        return $result;
    }

    public function reset(): static
    {
        parent::reset();
        $this->hints = [];
        $this->jsonSets = [];

        return $this;
    }

    private function compileSpatialFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::DistanceLessThan,
            Method::DistanceGreaterThan,
            Method::DistanceEqual,
            Method::DistanceNotEqual => $this->compileSpatialDistance($method, $attribute, $values),
            Method::Intersects => $this->compileSpatialPredicate('ST_Intersects', $attribute, $values, false),
            Method::NotIntersects => $this->compileSpatialPredicate('ST_Intersects', $attribute, $values, true),
            Method::Crosses => $this->compileSpatialPredicate('ST_Crosses', $attribute, $values, false),
            Method::NotCrosses => $this->compileSpatialPredicate('ST_Crosses', $attribute, $values, true),
            Method::Overlaps => $this->compileSpatialPredicate('ST_Overlaps', $attribute, $values, false),
            Method::NotOverlaps => $this->compileSpatialPredicate('ST_Overlaps', $attribute, $values, true),
            Method::Touches => $this->compileSpatialPredicate('ST_Touches', $attribute, $values, false),
            Method::NotTouches => $this->compileSpatialPredicate('ST_Touches', $attribute, $values, true),
            Method::Covers => $this->compileSpatialPredicate('ST_Contains', $attribute, $values, false),
            Method::NotCovers => $this->compileSpatialPredicate('ST_Contains', $attribute, $values, true),
            Method::SpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, false),
            Method::NotSpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, true),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileSpatialDistance(Method $method, string $attribute, array $values): string
    {
        /** @var array{0: string, 1: float, 2: bool} $data */
        $data = $values[0];
        $wkt = $data[0];
        $distance = $data[1];
        $meters = $data[2];

        $operator = match ($method) {
            Method::DistanceLessThan => '<',
            Method::DistanceGreaterThan => '>',
            Method::DistanceEqual => '=',
            Method::DistanceNotEqual => '!=',
            default => '<',
        };

        if ($meters) {
            $this->addBinding($wkt);
            $this->addBinding($distance);

            return 'ST_Distance(ST_SRID(' . $attribute . ', 4326), ST_GeomFromText(?, 4326), \'metre\') ' . $operator . ' ?';
        }

        $this->addBinding($wkt);
        $this->addBinding($distance);

        return 'ST_Distance(' . $attribute . ', ST_GeomFromText(?)) ' . $operator . ' ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string
    {
        /** @var array<mixed> $geometry */
        $geometry = $values[0];
        $wkt = $this->geometryToWkt($geometry);
        $this->addBinding($wkt);

        $expr = $function . '(' . $attribute . ', ST_GeomFromText(?, 4326))';

        return $not ? 'NOT ' . $expr : $expr;
    }

    private function compileJsonFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::JsonContains => $this->compileJsonContains($attribute, $values, false),
            Method::JsonNotContains => $this->compileJsonContains($attribute, $values, true),
            Method::JsonOverlaps => $this->compileJsonOverlapsFilter($attribute, $values),
            Method::JsonPath => $this->compileJsonPathFilter($attribute, $values),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonContains(string $attribute, array $values, bool $not): string
    {
        $this->addBinding(\json_encode($values[0]));
        $expr = 'JSON_CONTAINS(' . $attribute . ', ?)';

        return $not ? 'NOT ' . $expr : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonOverlapsFilter(string $attribute, array $values): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];
        $this->addBinding(\json_encode($arr));

        return 'JSON_OVERLAPS(' . $attribute . ', ?)';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonPathFilter(string $attribute, array $values): string
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
