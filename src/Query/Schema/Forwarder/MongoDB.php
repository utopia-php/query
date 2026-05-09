<?php

namespace Utopia\Query\Schema\Forwarder;

use Utopia\Query\Schema\Column;

/**
 * Forwarders that delegate MongoDB-specific calls back to the parent Table.
 * Used by {@see Column\MongoDB}. (MongoDB has no ForeignKey type.)
 *
 */
trait MongoDB
{
    public function vector(string $name, int $dimensions): Column\MongoDB
    {
        return $this->table->vector($name, $dimensions);
    }
}
