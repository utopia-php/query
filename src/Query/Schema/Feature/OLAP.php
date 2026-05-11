<?php

namespace Utopia\Query\Schema\Feature;

/**
 * Marker for dialects that expose OLAP-shaped column and table modifiers
 * (`LowCardinality`, `FixedString`, column-level `CODEC`, `SAMPLE BY`).
 *
 * Unlike sibling `Feature/*` interfaces — which declare schema-level method
 * signatures because their operations are emitted as standalone statements —
 * OLAP modifiers are intrinsic to a dialect's column/table builder shape and
 * cannot be expressed as `Schema` methods. They live on the dialect's
 * `Table` / `Column` subclasses (e.g. {@see \Utopia\Query\Schema\Table\ClickHouse},
 * {@see \Utopia\Query\Schema\Column\ClickHouse}) and the corresponding
 * `Forwarder` trait, so callers can only chain them when the underlying
 * dialect supports them — the methods aren't reachable from non-OLAP
 * dialects at the type level.
 *
 * Non-OLAP dialects therefore have nothing to handle or throw from: the
 * cross-dialect `ColumnType` enum carries no OLAP-only cases, and the
 * `compileColumnType()` implementations on `MySQL` / `PostgreSQL` / `SQLite` /
 * `MongoDB` are byte-identical to their pre-OLAP form.
 */
interface OLAP
{
}
