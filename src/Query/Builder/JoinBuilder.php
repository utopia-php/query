<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Exception\ValidationException;

class JoinBuilder
{
    private const ALLOWED_OPERATORS = ['=', '!=', '<', '>', '<=', '>=', '<>'];

    /** @var list<array{left: string, operator: string, right: string}> */
    private array $ons = [];

    /** @var list<array{expression: string, bindings: list<mixed>}> */
    private array $wheres = [];

    /**
     * Add an ON condition to the join.
     *
     * Note: $left and $right should be raw column identifiers (e.g. "users.id").
     * The parent builder's compileJoinWithBuilder already calls resolveAndWrap on these values.
     */
    public function on(string $left, string $right, string $operator = '='): static
    {
        if (!\in_array($operator, self::ALLOWED_OPERATORS, true)) {
            throw new ValidationException('Invalid join operator: ' . $operator);
        }

        $this->ons[] = ['left' => $left, 'operator' => $operator, 'right' => $right];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function onRaw(string $expression, array $bindings = []): static
    {
        $this->wheres[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    /**
     * Add a WHERE condition to the join.
     *
     * Note: $column is used as-is in the SQL expression. The caller is responsible
     * for ensuring it is a safe, pre-validated column identifier.
     */
    public function where(string $column, string $operator, mixed $value): static
    {
        if (!\preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $column)) {
            throw new ValidationException('Invalid column name: ' . $column);
        }

        if (!\in_array($operator, self::ALLOWED_OPERATORS, true)) {
            throw new ValidationException('Invalid join operator: ' . $operator);
        }

        $this->wheres[] = ['expression' => $column . ' ' . $operator . ' ?', 'bindings' => [$value]];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function whereRaw(string $expression, array $bindings = []): static
    {
        $this->wheres[] = ['expression' => $expression, 'bindings' => $bindings];

        return $this;
    }

    /** @return list<array{left: string, operator: string, right: string}> */
    public function getOns(): array
    {
        return $this->ons;
    }

    /** @return list<array{expression: string, bindings: list<mixed>}> */
    public function getWheres(): array
    {
        return $this->wheres;
    }
}
