<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Exception\ValidationException;

readonly class Index
{
    /**
     * @param  string[]  $columns
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     * @param  array<string, string>  $collations  Column-specific collations (column name => collation)
     * @param  list<string>  $rawColumns  Raw SQL expressions appended to the column list (bypass quoting)
     */
    public function __construct(
        public string $name,
        public array $columns,
        public IndexType $type = IndexType::Index,
        public array $lengths = [],
        public array $orders = [],
        public string $method = '',
        public string $operatorClass = '',
        public array $collations = [],
        public array $rawColumns = [],
    ) {
        if ($method !== '' && ! \preg_match('/^[A-Za-z0-9_]+$/', $method)) {
            throw new ValidationException('Invalid index method: ' . $method);
        }
        if ($operatorClass !== '' && ! \preg_match('/^[A-Za-z0-9_.]+$/', $operatorClass)) {
            throw new ValidationException('Invalid operator class: ' . $operatorClass);
        }
        foreach ($collations as $collation) {
            if (! \preg_match('/^[A-Za-z0-9_]+$/', $collation)) {
                throw new ValidationException('Invalid collation: ' . $collation);
            }
        }
    }
}
