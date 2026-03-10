<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\Locking;
use Utopia\Query\Builder\Feature\Transactions;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\QuotesIdentifiers;

abstract class SQL extends BaseBuilder implements Locking, Transactions, Upsert
{
    use QuotesIdentifiers;

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
            $tablePart .= ' AS ' . $this->insertAlias;
        }

        $sql = 'INSERT INTO ' . $tablePart
            . ' (' . \implode(', ', $wrappedColumns) . ')'
            . ' VALUES ' . \implode(', ', $rowPlaceholders);

        $sql .= ' ' . $this->compileConflictClause();

        return new BuildResult($sql, $this->bindings);
    }

    abstract public function insertOrIgnore(): BuildResult;

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
}
