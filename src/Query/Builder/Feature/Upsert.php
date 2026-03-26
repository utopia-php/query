<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\Plan;

interface Upsert
{
    public function upsert(): Plan;

    public function insertOrIgnore(): Plan;

    public function upsertSelect(): Plan;
}
