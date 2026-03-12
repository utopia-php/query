<?php

namespace Utopia\Query\Builder\Feature;

interface Returning
{
    /**
     * @param  list<string>  $columns
     */
    public function returning(array $columns = ['*']): static;
}
