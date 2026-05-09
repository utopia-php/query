<?php

namespace Utopia\Query\Schema\Forwarder;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

/**
 * Forwarders that delegate SQLite-specific calls back to the parent Table.
 * Used by {@see Column\SQLite} and {@see ForeignKey\SQLite}.
 */
trait SQLite
{
    public function foreignKey(string $column): ForeignKey\SQLite
    {
        return $this->table->foreignKey($column);
    }

    public function addForeignKey(string $column): ForeignKey\SQLite
    {
        return $this->table->addForeignKey($column);
    }

    public function dropForeignKey(string $name): Table\SQLite
    {
        return $this->table->dropForeignKey($name);
    }

}
