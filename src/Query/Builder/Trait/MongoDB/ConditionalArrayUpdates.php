<?php

namespace Utopia\Query\Builder\Trait\MongoDB;

trait ConditionalArrayUpdates
{
    /**
     * @param  array<string, mixed>  $condition
     */
    #[\Override]
    public function arrayFilter(string $identifier, array $condition): static
    {
        $this->arrayFilters[] = [$identifier => $condition];

        return $this;
    }
}
