<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;

class SQL extends BaseBuilder
{
    private string $wrapChar = '`';

    public function setWrapChar(string $char): static
    {
        $this->wrapChar = $char;

        return $this;
    }

    protected function wrapIdentifier(string $identifier): string
    {
        return $this->wrapChar . $identifier . $this->wrapChar;
    }

    protected function compileRandom(): string
    {
        return 'RAND()';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' REGEXP ?';
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function compileSearch(string $attribute, array $values, bool $not): string
    {
        $this->addBinding($values[0]);

        if ($not) {
            return 'NOT (MATCH(' . $attribute . ') AGAINST(?))';
        }

        return 'MATCH(' . $attribute . ') AGAINST(?)';
    }
}
