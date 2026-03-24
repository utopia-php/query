<?php

namespace Utopia\Query\Builder\Feature;

interface GroupByModifiers
{
    /**
     * Add a grand total row to GROUP BY results (no intermediate subtotals).
     */
    public function withTotals(): static;

    /**
     * Add hierarchical subtotal rows for each grouping level, plus a grand total.
     */
    public function withRollup(): static;

    /**
     * Add subtotal rows for every combination of grouping columns, plus a grand total.
     */
    public function withCube(): static;
}
