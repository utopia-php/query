<?php

namespace Utopia\Query\Builder\Feature;

interface VectorSearch
{
    /**
     * Order results by vector distance (nearest first).
     *
     * @param  array<float>  $vector  The query vector
     * @param  string  $metric  Distance metric: 'cosine', 'euclidean', 'dot'
     */
    public function orderByVectorDistance(string $attribute, array $vector, string $metric = 'cosine'): static;
}
