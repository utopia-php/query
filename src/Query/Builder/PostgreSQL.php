<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder\Feature\Json;
use Utopia\Query\Builder\Feature\LockingOf;
use Utopia\Query\Builder\Feature\Returning;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Builder\Feature\VectorSearch;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;

class PostgreSQL extends SQL implements Spatial, VectorSearch, Json, Returning, LockingOf
{
    protected string $wrapChar = '"';

    /** @var list<string> */
    protected array $returningColumns = [];

    /** @var array<string, array{expression: string, bindings: list<mixed>}> */
    protected array $jsonSets = [];

    /** @var ?array{attribute: string, vector: array<float>, metric: string} */
    protected ?array $vectorOrder = null;

    protected function compileRandom(): string
    {
        return 'RANDOM()';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' ~ ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSearch(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);

        if ($not) {
            return 'NOT (to_tsvector(' . $attribute . ') @@ plainto_tsquery(?))';
        }

        return 'to_tsvector(' . $attribute . ') @@ plainto_tsquery(?)';
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
                $updates[] = $wrapped . ' = EXCLUDED.' . $wrapped;
            }
        }

        return 'ON CONFLICT (' . \implode(', ', $wrappedKeys) . ') DO UPDATE SET ' . \implode(', ', $updates);
    }

    protected function shouldEmitOffset(?int $offset, ?int $limit): bool
    {
        return $offset !== null;
    }

    /**
     * @param  list<string>  $columns
     */
    public function returning(array $columns = ['*']): static
    {
        $this->returningColumns = $columns;

        return $this;
    }

    public function forUpdateOf(string $table): static
    {
        $this->lockMode = 'FOR UPDATE OF ' . $this->quote($table);

        return $this;
    }

    public function forShareOf(string $table): static
    {
        $this->lockMode = 'FOR SHARE OF ' . $this->quote($table);

        return $this;
    }

    public function insertOrIgnore(): BuildResult
    {
        $this->bindings = [];
        [$sql, $bindings] = $this->compileInsertBody();
        foreach ($bindings as $binding) {
            $this->addBinding($binding);
        }

        $sql .= ' ON CONFLICT DO NOTHING';

        return $this->appendReturning(new BuildResult($sql, $this->bindings));
    }

    public function insert(): BuildResult
    {
        $result = parent::insert();

        return $this->appendReturning($result);
    }

    public function update(): BuildResult
    {
        foreach ($this->jsonSets as $col => $data) {
            $this->setRaw($col, $data['expression'], $data['bindings']);
        }

        $result = parent::update();
        $this->jsonSets = [];

        return $this->appendReturning($result);
    }

    public function delete(): BuildResult
    {
        $result = parent::delete();

        return $this->appendReturning($result);
    }

    public function upsert(): BuildResult
    {
        $result = parent::upsert();

        return $this->appendReturning($result);
    }

    private function appendReturning(BuildResult $result): BuildResult
    {
        if (empty($this->returningColumns)) {
            return $result;
        }

        $columns = \array_map(
            fn (string $col): string => $col === '*' ? '*' : $this->resolveAndWrap($col),
            $this->returningColumns
        );

        return new BuildResult(
            $result->query . ' RETURNING ' . \implode(', ', $columns),
            $result->bindings
        );
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

    public function orderByVectorDistance(string $attribute, array $vector, string $metric = 'cosine'): static
    {
        $this->vectorOrder = [
            'attribute' => $attribute,
            'vector' => $vector,
            'metric' => $metric,
        ];

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
            'expression' => 'COALESCE(' . $this->resolveAndWrap($column) . ', \'[]\'::jsonb) || ?::jsonb',
            'bindings' => [\json_encode($values)],
        ];

        return $this;
    }

    public function setJsonPrepend(string $column, array $values): static
    {
        $this->jsonSets[$column] = [
            'expression' => '?::jsonb || COALESCE(' . $this->resolveAndWrap($column) . ', \'[]\'::jsonb)',
            'bindings' => [\json_encode($values)],
        ];

        return $this;
    }

    public function setJsonInsert(string $column, int $index, mixed $value): static
    {
        $this->jsonSets[$column] = [
            'expression' => 'jsonb_insert(' . $this->resolveAndWrap($column) . ', \'{' . $index . '}\', ?::jsonb)',
            'bindings' => [\json_encode($value)],
        ];

        return $this;
    }

    public function setJsonRemove(string $column, mixed $value): static
    {
        $this->jsonSets[$column] = [
            'expression' => $this->resolveAndWrap($column) . ' - ?',
            'bindings' => [\json_encode($value)],
        ];

        return $this;
    }

    public function setJsonIntersect(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT jsonb_agg(elem) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem WHERE elem <@ ?::jsonb)', [\json_encode($values)]);

        return $this;
    }

    public function setJsonDiff(string $column, array $values): static
    {
        $this->setRaw($column, '(SELECT COALESCE(jsonb_agg(elem), \'[]\'::jsonb) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem WHERE NOT elem <@ ?::jsonb)', [\json_encode($values)]);

        return $this;
    }

    public function setJsonUnique(string $column): static
    {
        $this->setRaw($column, '(SELECT jsonb_agg(DISTINCT elem) FROM jsonb_array_elements(' . $this->resolveAndWrap($column) . ') AS elem)');

        return $this;
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

        if ($method->isVector()) {
            return $this->compileVectorFilter($method, $attribute, $query);
        }

        return parent::compileFilter($query);
    }

    /**
     * @return array{expression: string, bindings: list<mixed>}|null
     */
    protected function compileVectorOrderExpr(): ?array
    {
        if ($this->vectorOrder === null) {
            return null;
        }

        $attr = $this->resolveAndWrap($this->vectorOrder['attribute']);
        $operator = match ($this->vectorOrder['metric']) {
            'cosine' => '<=>',
            'euclidean' => '<->',
            'dot' => '<#>',
            default => '<=>',
        };
        $vectorJson = \json_encode($this->vectorOrder['vector']);

        return [
            'expression' => '(' . $attr . ' ' . $operator . ' ?::vector) ASC',
            'bindings' => [$vectorJson],
        ];
    }

    public function reset(): static
    {
        parent::reset();
        $this->jsonSets = [];
        $this->vectorOrder = null;
        $this->returningColumns = [];

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
            Method::Covers => $this->compileSpatialPredicate('ST_Covers', $attribute, $values, false),
            Method::NotCovers => $this->compileSpatialPredicate('ST_Covers', $attribute, $values, true),
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

            return 'ST_Distance((' . $attribute . '::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) ' . $operator . ' ?';
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
            Method::JsonContains => $this->compileJsonContainsExpr($attribute, $values, false),
            Method::JsonNotContains => $this->compileJsonContainsExpr($attribute, $values, true),
            Method::JsonOverlaps => $this->compileJsonOverlapsExpr($attribute, $values),
            Method::JsonPath => $this->compileJsonPathExpr($attribute, $values),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonContainsExpr(string $attribute, array $values, bool $not): string
    {
        $this->addBinding(\json_encode($values[0]));
        $expr = $attribute . ' @> ?::jsonb';

        return $not ? 'NOT (' . $expr . ')' : $expr;
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonOverlapsExpr(string $attribute, array $values): string
    {
        /** @var array<mixed> $arr */
        $arr = $values[0];
        $this->addBinding(\json_encode($arr));

        return $attribute . ' ?| ARRAY(SELECT jsonb_array_elements_text(?::jsonb))';
    }

    /**
     * @param  array<mixed>  $values
     */
    private function compileJsonPathExpr(string $attribute, array $values): string
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

        return $attribute . '->>\''. $path . '\' ' . $operator . ' ?';
    }

    private function compileVectorFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();
        /** @var array<float> $vector */
        $vector = $values[0];

        $operator = match ($method) {
            Method::VectorCosine => '<=>',
            Method::VectorEuclidean => '<->',
            Method::VectorDot => '<#>',
            default => '<=>',
        };

        $this->addBinding(\json_encode($vector));

        return '(' . $attribute . ' ' . $operator . ' ?::vector)';
    }

}
