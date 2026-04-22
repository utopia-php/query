<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Builder\Plan;

trait Transactions
{
    #[\Override]
    public function begin(): Plan
    {
        return new Plan('BEGIN', [], executor: $this->executor);
    }

    #[\Override]
    public function commit(): Plan
    {
        return new Plan('COMMIT', [], executor: $this->executor);
    }

    #[\Override]
    public function rollback(): Plan
    {
        return new Plan('ROLLBACK', [], executor: $this->executor);
    }

    #[\Override]
    public function savepoint(string $name): Plan
    {
        return new Plan('SAVEPOINT ' . $this->quote($name), [], executor: $this->executor);
    }

    #[\Override]
    public function releaseSavepoint(string $name): Plan
    {
        return new Plan('RELEASE SAVEPOINT ' . $this->quote($name), [], executor: $this->executor);
    }

    #[\Override]
    public function rollbackToSavepoint(string $name): Plan
    {
        return new Plan('ROLLBACK TO SAVEPOINT ' . $this->quote($name), [], executor: $this->executor);
    }
}
