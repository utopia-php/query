<?php

namespace Utopia\Query\Schema\Feature\ClickHouse;

use Utopia\Query\Builder;
use Utopia\Query\Builder\Statement;

interface MaterializedViews
{
    /**
     * Emit `CREATE MATERIALIZED VIEW [IF NOT EXISTS] \`name\` TO \`target\` AS <body>`.
     *
     * Accepts either a {@see Builder} (whose `build()` SQL is inlined and whose
     * bindings ride along on the returned Statement) or a raw SQL string for
     * bodies that do not yet round-trip through the builder.
     *
     * @security When `$body` is a string, its contents are inlined verbatim
     *           into the DDL with no escaping or validation. Pass only SQL
     *           that the caller fully controls — never a value derived from
     *           an untrusted source. Prefer the {@see Builder} overload when
     *           any part of the body is parameterised.
     */
    public function createMaterializedView(string $name, string $targetTable, Builder|string $body, bool $ifNotExists = true): Statement;

    public function dropMaterializedView(string $name, bool $ifExists = true): Statement;
}
