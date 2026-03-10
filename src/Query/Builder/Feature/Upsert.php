<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\BuildResult;

interface Upsert
{
    public function upsert(): BuildResult;

    public function insertOrIgnore(): BuildResult;
}
