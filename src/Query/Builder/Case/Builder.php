<?php

namespace Utopia\Query\Builder\Case;

use Utopia\Query\Exception\ValidationException;

class Builder
{
    /** @var list<array{condition: string, result: string, conditionBindings: list<mixed>, resultBindings: list<mixed>}> */
    private array $whens = [];

    private ?string $elseResult = null;

    /** @var list<mixed> */
    private array $elseBindings = [];

    private string $alias = '';

    /**
     * @param  list<mixed>  $conditionBindings
     * @param  list<mixed>  $resultBindings
     */
    public function when(string $condition, string $result, array $conditionBindings = [], array $resultBindings = []): static
    {
        $this->whens[] = [
            'condition' => $condition,
            'result' => $result,
            'conditionBindings' => $conditionBindings,
            'resultBindings' => $resultBindings,
        ];

        return $this;
    }

    /**
     * @param  list<mixed>  $bindings
     */
    public function elseResult(string $result, array $bindings = []): static
    {
        $this->elseResult = $result;
        $this->elseBindings = $bindings;

        return $this;
    }

    /**
     * Set the alias for this CASE expression.
     *
     * The alias is used as-is in the generated SQL (e.g. `CASE ... END AS alias`).
     * The caller must pass a pre-quoted identifier if quoting is required, since
     * Case\Builder does not have access to the builder's quote() method.
     */
    public function alias(string $alias): static
    {
        $this->alias = $alias;

        return $this;
    }

    public function build(): Expression
    {
        if (empty($this->whens)) {
            throw new ValidationException('CASE expression requires at least one WHEN clause.');
        }

        $sql = 'CASE';
        $bindings = [];

        foreach ($this->whens as $when) {
            $sql .= ' WHEN ' . $when['condition'] . ' THEN ' . $when['result'];
            foreach ($when['conditionBindings'] as $binding) {
                $bindings[] = $binding;
            }
            foreach ($when['resultBindings'] as $binding) {
                $bindings[] = $binding;
            }
        }

        if ($this->elseResult !== null) {
            $sql .= ' ELSE ' . $this->elseResult;
            foreach ($this->elseBindings as $binding) {
                $bindings[] = $binding;
            }
        }

        $sql .= ' END';

        if ($this->alias !== '') {
            $sql .= ' AS ' . $this->alias;
        }

        return new Expression($sql, $bindings);
    }
}
