<?php

namespace Utopia\Query\Schema\ClickHouse;

use Utopia\Query\Exception\ValidationException;

readonly class SkipIndex
{
    /**
     * @param  list<string>  $columns
     * @param  list<string|int|float>  $algorithmArgs  Args for parameterized algorithms
     *                                                  (e.g. [3] for set(3),
     *                                                  [0.01] for bloom_filter(0.01),
     *                                                  [4, 1024, 3, 0] for ngrambf_v1(n, size_bytes, hashes, seed))
     */
    public function __construct(
        public string $name,
        public array $columns,
        public SkipIndexAlgorithm $algorithm,
        public array $algorithmArgs = [],
        public int $granularity = 1,
    ) {
        if (! \preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $name)) {
            throw new ValidationException('Invalid skip index name: ' . $name);
        }
        if ($columns === []) {
            throw new ValidationException('Skip index requires at least one column.');
        }
        if ($granularity < 1) {
            throw new ValidationException('Skip index granularity must be >= 1.');
        }
    }
}
