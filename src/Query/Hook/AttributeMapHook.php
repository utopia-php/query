<?php

namespace Utopia\Query\Hook;

class AttributeMapHook implements AttributeHook
{
    /** @param array<string, string> $map */
    public function __construct(protected array $map)
    {
    }

    public function resolve(string $attribute): string
    {
        return $this->map[$attribute] ?? $attribute;
    }
}
