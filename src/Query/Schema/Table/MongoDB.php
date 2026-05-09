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

    public function vector(string $name, int $dimensions): Column\MongoDB
    {
        $col = $this->newColumn($name, ColumnType::Vector);
        $col->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }
}
