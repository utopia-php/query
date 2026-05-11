<?php

namespace Utopia\Query\Schema\Forwarder;

use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\Table;

/**
 * Forwarders that delegate ClickHouse-specific calls back to the parent Table.
 * Used by {@see Column\ClickHouse}. (ClickHouse has no ForeignKey type.)
 *
 */
trait ClickHouse
{
    public function vector(string $name, int $dimensions): Column\ClickHouse
    {
        return $this->table->vector($name, $dimensions);
    }

    public function engine(Engine $engine, string ...$args): Table\ClickHouse
    {
        return $this->table->engine($engine, ...$args);
    }

    /**
     * @param  list<string>  $columns
     */
    public function orderBy(array $columns): Table\ClickHouse
    {
        return $this->table->orderBy($columns);
    }

    /**
     * @param  array<string, string|int|float|bool>  $settings
     */
    public function settings(array $settings): Table\ClickHouse
    {
        return $this->table->settings($settings);
    }

    public function partitionBy(string $expression): Table\ClickHouse
    {
        return $this->table->partitionBy($expression);
    }

}
