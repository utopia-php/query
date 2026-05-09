<?php

namespace Utopia\Query\Schema\Forwarder;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

/**
 * Forwarders that delegate SQLite-specific calls back to the parent Table.
 * Used by {@see Column\SQLite} and {@see ForeignKey\SQLite}.
 *
 * Note: SQLite ALTER TABLE does not support FK add/drop, so only the inline
 * `foreignKey()` (used at CREATE time) is forwarded — `addForeignKey()` and
 * `dropForeignKey()` are intentionally omitted.
 */
trait SQLite
{
    public function foreignKey(string $column): ForeignKey\SQLite
    {
        return $this->table->foreignKey($column);
    }
}
