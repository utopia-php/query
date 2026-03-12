<?php

namespace Utopia\Query\Schema;

class ForeignKey
{
    public private(set) string $refTable = '';

    public private(set) string $refColumn = '';

    public private(set) ?ForeignKeyAction $onDelete = null;

    public private(set) ?ForeignKeyAction $onUpdate = null;

    public function __construct(
        public readonly string $column,
    ) {
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

    public function onDelete(ForeignKeyAction $action): static
    {
        $this->onDelete = $action;

        return $this;
    }

    public function onUpdate(ForeignKeyAction $action): static
    {
        $this->onUpdate = $action;

        return $this;
    }
}
