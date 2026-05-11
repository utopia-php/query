<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

/**
 * @extends Table<Column\MySQL, ForeignKey\MySQL>
 */
class MySQL extends Table
{
    use Trait\Checks;
    use Trait\CompositePrimary;
    /** @use Trait\ForeignKeys<ForeignKey\MySQL> */
    use Trait\ForeignKeys;
    use Trait\FulltextSpatialIndex;
    use Trait\StandardPartitioning;

    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\MySQL
    {
        return new Column\MySQL($this, $name, $type, $length, $precision);
    }

    #[\Override]
    protected function newForeignKey(string $column): ForeignKey\MySQL
    {
        return new ForeignKey\MySQL($this, $column);
    }
}
