<?php

namespace Utopia\Query\Hook;

use Utopia\Query\Hook;

interface AttributeHook extends Hook
{
    public function resolve(string $attribute): string;
}
