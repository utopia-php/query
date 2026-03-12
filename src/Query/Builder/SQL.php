<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\FullTextSearch;
use Utopia\Query\Builder\Feature\Locking;
use Utopia\Query\Builder\Feature\Spatial;
use Utopia\Query\Builder\Feature\Transactions;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\QuotesIdentifiers;
use Utopia\Query\Schema\ColumnType;

abstract class SQL extends BaseBuilder implements Locking, Transactions, Upsert, Spatial, FullTextSearch
{
    use QuotesIdentifiers;

    /** @var array<string, Condition> */
    protected array $jsonSets = [];

    public function forUpdate(): static
    {
        $this->lockMode = LockMode::ForUpdate;

        return $this;
    }

    public function forShare(): static
    {
        $this->lockMode = LockMode::ForShare;

        return $this;
    }

    public function forUpdateSkipLocked(): static
    {
        $this->lockMode = LockMode::ForUpdateSkipLocked;

        return $this;
    }

    public function forUpdateNoWait(): static
    {
        $this->lockMode = LockMode::ForUpdateNoWait;

        return $this;
    }

    public function forShareSkipLocked(): static
    {
        $this->lockMode = LockMode::ForShareSkipLocked;

        return $this;
    }

    public function forShareNoWait(): static
    {
        $this->lockMode = LockMode::ForShareNoWait;

        return $this;
    }

    public function begin(): BuildResult
    {
        return new BuildResult('BEGIN', []);
    }

    public function commit(): BuildResult
    {
        return new BuildResult('COMMIT', []);
    }

    public function rollback(): BuildResult
    {
        return new BuildResult('ROLLBACK', []);
    }

    public function savepoint(string $name): BuildResult
    {
        return new BuildResult('SAVEPOINT ' . $this->quote($name), []);
    }

    public function releaseSavepoint(string $name): BuildResult
    {
        return new BuildResult('RELEASE SAVEPOINT ' . $this->quote($name), []);
    }

    public function rollbackToSavepoint(string $name): BuildResult
    {
        return new BuildResult('ROLLBACK TO SAVEPOINT ' . $this->quote($name), []);
    }

    abstract protected function compileConflictClause(): string;

    public function upsert(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();
        $this->validateRows('upsert');
        $columns = $this->validateAndGetColumns();

        if (empty($this->conflictKeys)) {
            throw new ValidationException('No conflict keys specified. Call onConflict() before upsert().');
        }

        if (empty($this->conflictUpdateColumns)) {
            throw new ValidationException('No conflict update columns specified. Call onConflict() with update columns before upsert().');
        }

        $rowColumns = $columns;
        foreach ($this->conflictUpdateColumns as $col) {
            if (! \in_array($col, $rowColumns, true)) {
                throw new ValidationException("Conflict update column '{$col}' is not present in the row data.");
            }
        }

        $wrappedColumns = \array_map(fn (string $col): string => $this->resolveAndWrap($col), $columns);

        $rowPlaceholders = [];
        foreach ($this->pendingRows as $row) {
            $placeholders = [];
            foreach ($columns as $col) {
                $this->addBinding($row[$col] ?? null);
                if (isset($this->insertColumnExpressions[$col])) {
                    $placeholders[] = $this->insertColumnExpressions[$col];
                    foreach ($this->insertColumnExpressionBindings[$col] ?? [] as $extra) {
                        $this->addBinding($extra);
                    }
                } else {
                    $placeholders[] = '?';
                }
            }
            $rowPlaceholders[] = '(' . \implode(', ', $placeholders) . ')';
        }

        $tablePart = $this->quote($this->table);
        if ($this->insertAlias !== '') {
            $tablePart .= ' AS ' . $this->quote($this->insertAlias);
        }

        $sql = 'INSERT INTO ' . $tablePart
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' VALUES ' . \implode(', ', $rowPlaceholders);

        $sql .= ' ' . $this->compileConflictClause();

        return new BuildResult($sql, $this->bindings);
    }

    abstract public function insertOrIgnore(): BuildResult;

    public function upsertSelect(): BuildResult
    {
        $this->bindings = [];
        $this->validateTable();

        if ($this->insertSelectSource === null) {
            throw new ValidationException('No SELECT source specified. Call fromSelect() before upsertSelect().');
        }
        if (empty($this->insertSelectColumns)) {
            throw new ValidationException('No columns specified. Call fromSelect() with columns before upsertSelect().');
        }
        if (empty($this->conflictKeys)) {
            throw new ValidationException('No conflict keys specified. Call onConflict() before upsertSelect().');
        }
        if (empty($this->conflictUpdateColumns)) {
            throw new ValidationException('No conflict update columns specified. Call onConflict() with update columns before upsertSelect().');
        }

        $wrappedColumns = \array_map(
            fn (string $col): string => $this->resolveAndWrap($col),
            $this->insertSelectColumns
        );

        $sourceResult = $this->insertSelectSource->build();

        $sql = 'INSERT INTO ' . $this->quote($this->table)
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' ' . $sourceResult->query;

        foreach ($sourceResult->bindings as $binding) {
            $this->addBinding($binding);
        }

        $sql .= ' ' . $this->compileConflictClause();

        return new BuildResult($sql, $this->bindings);
    }

    /**
     * Convert a geometry array to WKT string.
     *
     * @param  array<mixed>  $geometry
     */
    protected function geometryToWkt(array $geometry): string
    {
        // Simple array of [lon, lat] -> POINT
        if (\count($geometry) === 2 && \is_numeric($geometry[0]) && \is_numeric($geometry[1])) {
            return 'POINT(' . (float) $geometry[0] . ' ' . (float) $geometry[1] . ')';
        }

        // Array of points -> check depth
        if (isset($geometry[0]) && \is_array($geometry[0])) {
            // Array of arrays of arrays -> POLYGON
            if (isset($geometry[0][0]) && \is_array($geometry[0][0])) {
                $rings = [];
                foreach ($geometry as $ring) {
                    /** @var array<array<float>> $ring */
                    $points = \array_map(fn (array $p): string => (float) $p[0] . ' ' . (float) $p[1], $ring);
                    $rings[] = '(' . \implode(', ', $points) . ')';
                }

                return 'POLYGON(' . \implode(', ', $rings) . ')';
            }

            // Single [lon, lat] pair wrapped in array -> POINT
            if (\count($geometry) === 1) {
                /** @var array<float> $point */
                $point = $geometry[0];

                return 'POINT(' . (float) $point[0] . ' ' . (float) $point[1] . ')';
            }

            // Array of [lon, lat] pairs -> LINESTRING
            /** @var array<array<float>> $geometry */
            $points = \array_map(fn (array $p): string => (float) $p[0] . ' ' . (float) $p[1], $geometry);

            return 'LINESTRING(' . \implode(', ', $points) . ')';
        }

        /** @var int|float|string $rawX */
        $rawX = $geometry[0] ?? 0;
        /** @var int|float|string $rawY */
        $rawY = $geometry[1] ?? 0;

        return 'POINT(' . (float) $rawX . ' ' . (float) $rawY . ')';
    }

    protected function getSpatialTypeFromWkt(string $wkt): string
    {
        $upper = \strtoupper(\trim($wkt));
        if (\str_starts_with($upper, 'POINT')) {
            return ColumnType::Point->value;
        }
        if (\str_starts_with($upper, 'LINESTRING')) {
            return ColumnType::Linestring->value;
        }
        if (\str_starts_with($upper, 'POLYGON')) {
            return ColumnType::Polygon->value;
        }

        return 'unknown';
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

    public function filterSearch(string $attribute, string $value): static
    {
        $this->pendingQueries[] = Query::search($attribute, $value);

        return $this;
    }

    public function filterNotSearch(string $attribute, string $value): static
    {
        $this->pendingQueries[] = Query::notSearch($attribute, $value);

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

    /**
     * @param  array<mixed>  $values
     */
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

    public function compileFilter(Query $query): string
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAndWrap($query->getAttribute());

        if ($method === Method::Search) {
            return $this->compileSearchExpr($attribute, $query->getValues(), false);
        }

        if ($method === Method::NotSearch) {
            return $this->compileSearchExpr($attribute, $query->getValues(), true);
        }

        if ($method->isSpatial()) {
            return $this->compileSpatialFilter($method, $attribute, $query);
        }

        $attrType = $query->getAttributeType();
        $isSpatialAttr = \in_array($attrType, [ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value], true);
        if ($isSpatialAttr) {
            $spatialMethod = match ($method) {
                Method::Equal => Method::SpatialEquals,
                Method::NotEqual => Method::NotSpatialEquals,
                Method::Contains => Method::Covers,
                Method::NotContains => Method::NotCovers,
                default => null,
            };
            if ($spatialMethod !== null) {
                return $this->compileSpatialFilter($spatialMethod, $attribute, $query);
            }
        }

        if ($method->isJson()) {
            return $this->compileJsonFilter($method, $attribute, $query);
        }

        if ($query->onArray() && \in_array($method, [Method::Contains, Method::ContainsAny, Method::NotContains, Method::ContainsAll], true)) {
            return $this->compileArrayFilter($method, $attribute, $query);
        }

        return parent::compileFilter($query);
    }

    protected function compileArrayFilter(Method $method, string $attribute, Query $query): string
    {
        $values = $query->getValues();

        return match ($method) {
            Method::Contains,
            Method::ContainsAny => $this->compileJsonOverlapsExpr($attribute, [$values]),
            Method::NotContains => 'NOT ' . $this->compileJsonOverlapsExpr($attribute, [$values]),
            Method::ContainsAll => $this->compileJsonContainsExpr($attribute, [$values], false),
            default => parent::compileFilter($query),
        };
    }

    protected function compileSpatialFilter(Method $method, string $attribute, Query $query): string
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
            Method::Covers => $this->compileSpatialCoversPredicate($attribute, $values, false),
            Method::NotCovers => $this->compileSpatialCoversPredicate($attribute, $values, true),
            Method::SpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, false),
            Method::NotSpatialEquals => $this->compileSpatialPredicate('ST_Equals', $attribute, $values, true),
            default => parent::compileFilter($query),
        };
    }

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialDistance(Method $method, string $attribute, array $values): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialPredicate(string $function, string $attribute, array $values, bool $not): string;

    /**
     * Compile covers/not-covers spatial predicate. MySQL uses ST_Contains, PostgreSQL uses ST_Covers.
     *
     * @param  array<mixed>  $values
     */
    abstract protected function compileSpatialCoversPredicate(string $attribute, array $values, bool $not): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileSearchExpr(string $attribute, array $values, bool $not): string;

    protected function compileJsonFilter(Method $method, string $attribute, Query $query): string
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
    abstract protected function compileJsonContainsExpr(string $attribute, array $values, bool $not): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileJsonOverlapsExpr(string $attribute, array $values): string;

    /**
     * @param  array<mixed>  $values
     */
    abstract protected function compileJsonPathExpr(string $attribute, array $values): string;
}
