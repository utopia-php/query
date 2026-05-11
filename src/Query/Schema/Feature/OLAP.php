<?php

namespace Utopia\Query\Schema\Feature;

/**
 * Marker for dialects that expose OLAP-shaped column and table modifiers
 * (`LowCardinality`, `FixedString`, column-level `CODEC`, `SAMPLE BY`).
 *
 * The modifier methods live on the dialect's `Table` / `Column` subclasses
 * (e.g. {@see \Utopia\Query\Schema\Table\ClickHouse},
 * {@see \Utopia\Query\Schema\Column\ClickHouse}) and the corresponding
 * `Forwarder` trait, so callers can only chain them when the underlying
 * dialect supports them — the methods aren't reachable from non-OLAP
 * dialects at the type level.
 */
interface OLAP
{
}
