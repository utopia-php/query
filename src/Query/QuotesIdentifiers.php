<?php

namespace Utopia\Query;

use Utopia\Query\Exception\ValidationException;

trait QuotesIdentifiers
{
    protected string $wrapChar = '`';

    protected function quote(string $identifier): string
    {
        if ($identifier === '*') {
            return '*';
        }

        if (\preg_match('/[\x00-\x1f\x7f]/', $identifier) === 1) {
            throw new ValidationException('Identifier contains control character');
        }

        if (!\str_contains($identifier, '.')) {
            return $this->wrapChar
                . \str_replace($this->wrapChar, $this->wrapChar . $this->wrapChar, $identifier)
                . $this->wrapChar;
        }

        $segments = \explode('.', $identifier);
        $lastIndex = \count($segments) - 1;
        $wrapped = [];

        foreach ($segments as $index => $segment) {
            if ($segment === '*' && $index === $lastIndex) {
                $wrapped[] = '*';
                continue;
            }

            $wrapped[] = $this->wrapChar
                . \str_replace($this->wrapChar, $this->wrapChar . $this->wrapChar, $segment)
                . $this->wrapChar;
        }

        return \implode('.', $wrapped);
    }
}
