<?php

namespace Utopia\Query\Schema;

class ForeignKey
{
    public string $column;

    public string $refTable = '';

    public string $refColumn = '';

    public string $onDelete = '';

    public string $onUpdate = '';

    public function __construct(string $column)
    {
        $this->column = $column;
    }

    public function references(string $column): static
    {
        $this->refColumn = $column;

        return $this;
    }

    public function on(string $table): static
    {
        $this->refTable = $table;

        return $this;
    }

    private const ALLOWED_ACTIONS = ['CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION'];

    public function onDelete(string $action): static
    {
        $action = \strtoupper($action);
        if (!\in_array($action, self::ALLOWED_ACTIONS, true)) {
            throw new \InvalidArgumentException('Invalid foreign key action: ' . $action);
        }

        $this->onDelete = $action;

        return $this;
    }

    public function onUpdate(string $action): static
    {
        $action = \strtoupper($action);
        if (!\in_array($action, self::ALLOWED_ACTIONS, true)) {
            throw new \InvalidArgumentException('Invalid foreign key action: ' . $action);
        }

        $this->onUpdate = $action;

        return $this;
    }
}
