<?php

namespace Utopia\Query\Builder\Feature;

interface FullTextSearch
{
    public function filterSearch(string $attribute, string $value): static;

    public function filterNotSearch(string $attribute, string $value): static;
}
