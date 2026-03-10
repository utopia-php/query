<?php

namespace Utopia\Query;

trait QuotesIdentifiers
{
    protected string $wrapChar = '`';

    protected function quote(string $identifier): string
    {
        $segments = \explode('.', $identifier);
        $wrapped = \array_map(fn (string $segment): string => $segment === '*'
            ? '*'
            : $this->wrapChar . \str_replace($this->wrapChar, $this->wrapChar . $this->wrapChar, $segment) . $this->wrapChar, $segments);

        return \implode('.', $wrapped);
    }
}
