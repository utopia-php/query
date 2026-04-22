<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Builder\Plan;
use Utopia\Query\Exception\ValidationException;

trait Upsert
{
    #[\Override]
    public function upsert(): Plan
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
        foreach ($this->rows as $row) {
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

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }

    #[\Override]
    public function upsertSelect(): Plan
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

        $this->addBindings($sourceResult->bindings);

        $sql .= ' ' . $this->compileConflictClause();

        return new Plan($sql, $this->bindings, executor: $this->executor);
    }
}
