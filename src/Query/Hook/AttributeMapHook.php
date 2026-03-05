<?php

namespace Utopia\Query\Hook;

readonly class AttributeMapHook implements AttributeHook
{
    /** @param array<string, string> $map */
    public function __construct(public array $map)
    {
    }

    public function resolve(string $attribute): string
    {
        return $this->map[$attribute] ?? $attribute;
    }
}
