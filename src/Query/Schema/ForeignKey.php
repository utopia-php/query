<?php

namespace Utopia\Query\Schema;

class ForeignKey
{
    public string $column;

    public string $refTable = '';

    public string $refColumn = '';

    public ?ForeignKeyAction $onDelete = null;

    public ?ForeignKeyAction $onUpdate = null;

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

    public function onDelete(ForeignKeyAction|string $action): static
    {
        if (\is_string($action)) {
            $action = ForeignKeyAction::from(\strtoupper($action));
        }

        $this->onDelete = $action;

        return $this;
    }

    public function onUpdate(ForeignKeyAction|string $action): static
    {
        if (\is_string($action)) {
            $action = ForeignKeyAction::from(\strtoupper($action));
        }

        $this->onUpdate = $action;

        return $this;
    }
}
