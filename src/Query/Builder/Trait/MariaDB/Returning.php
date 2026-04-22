<?php

namespace Utopia\Query\Builder\Trait\MariaDB;

trait Returning
{
    /**
     * @param  list<string>  $columns
     */
    #[\Override]
    public function returning(array $columns = ['*']): static
    {
        $this->returningColumns = $columns;

        return $this;
    }
}
