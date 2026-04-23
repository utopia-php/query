<?php

namespace Utopia\Query\Builder\Trait;

use Utopia\Query\Exception\UnsupportedException;

trait GroupByModifiers
{
    protected ?string $groupByModifier = null;

    #[\Override]
    public function withRollup(): static
    {
        throw new UnsupportedException('WITH ROLLUP is not supported by this dialect.');
    }

    #[\Override]
    public function withCube(): static
    {
        throw new UnsupportedException('WITH CUBE is not supported by this dialect.');
    }

    #[\Override]
    public function withTotals(): static
    {
        throw new UnsupportedException('WITH TOTALS is not supported by this dialect.');
    }

    protected function resetGroupByModifier(): void
    {
        $this->groupByModifier = null;
    }
}
