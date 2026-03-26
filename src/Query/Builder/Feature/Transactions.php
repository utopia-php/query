<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\Plan;

interface Transactions
{
    public function begin(): Plan;

    public function commit(): Plan;

    public function rollback(): Plan;

    public function savepoint(string $name): Plan;

    public function releaseSavepoint(string $name): Plan;

    public function rollbackToSavepoint(string $name): Plan;
}
