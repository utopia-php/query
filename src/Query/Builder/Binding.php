<?php

namespace Utopia\Query\Builder;

/**
 * A single parameter binding annotated with optional name + type metadata.
 *
 * The base `Builder` keeps `$bindings` as a `list<mixed>` and emits `?`
 * placeholders, which positional-protocol drivers consume directly.
 *
 * Dialects that need typed named placeholders — ClickHouse HTTP, where
 * parameters are passed as `{name:Type}` query-string params — keep a
 * parallel `list<Binding>` so they can rewrite `?` to `{name:Type}` and
 * publish `Statement::$namedBindings` without disturbing the positional
 * path used by every other dialect.
 */
readonly class Binding
{
    public function __construct(
        public mixed $value,
        public ?string $name = null,
        public ?string $type = null,
        public ?string $column = null,
    ) {
    }

    public function withName(string $name): self
    {
        return new self($this->value, $name, $this->type, $this->column);
    }

    public function withType(string $type): self
    {
        return new self($this->value, $this->name, $type, $this->column);
    }
}
