<?php

namespace Utopia\Query\Schema\ForeignKey;

use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Forwarder;
use Utopia\Query\Schema\Table;

/**
 * @property Table\SQLite $table
 */
class SQLite extends ForeignKey
{
    use Forwarder\SQLite;

    /**
     * @param  list<string>  $columns
     */
    public function primary(array $columns): Table\SQLite
    {
        return $this->table->primary($columns);
    }

    public function check(string $name, string $expression): Table\SQLite
    {
        return $this->table->check($name, $expression);
    }
}
