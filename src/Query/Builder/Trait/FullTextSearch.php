<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Query;

trait FullTextSearch
{
    #[\Override]
    public function filterSearch(string $attribute, string $value): static
    {
        $this->pendingQueries[] = Query::search($attribute, $value);

        return $this;
    }

    #[\Override]
    public function filterNotSearch(string $attribute, string $value): static
    {
        $this->pendingQueries[] = Query::notSearch($attribute, $value);

        return $this;
    }
}
