<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

/**
 * @extends Table<Column\MongoDB, ForeignKey>
 */
class MongoDB extends Table
{
    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\MongoDB
    {
        return new Column\MongoDB($this, $name, $type, $length, $precision);
    }

    /**
     * @return Column\MongoDB
     */
    public function vector(string $name, int $dimensions): Column
    {
        $col = $this->newColumn($name, ColumnType::Vector)->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }
}
