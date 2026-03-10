<?php

namespace Utopia\Query\Builder\Feature;

use Utopia\Query\Builder\BuildResult;

interface Transactions
{
    public function begin(): BuildResult;

    public function commit(): BuildResult;

    public function rollback(): BuildResult;

    public function savepoint(string $name): BuildResult;

    public function releaseSavepoint(string $name): BuildResult;

    public function rollbackToSavepoint(string $name): BuildResult;
}
