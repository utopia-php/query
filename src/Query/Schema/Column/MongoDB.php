<?php

namespace Utopia\Query\Schema\Column;

use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\Forwarder;
use Utopia\Query\Schema\Table;

/**
 * @property Table\MongoDB $table
 */
class MongoDB extends Column
{
    use Forwarder\MongoDB;
}
