<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Exception\ValidationException;

readonly class Index
{
    /**
     * @param  string[]  $columns
     * @param  array<string, int>  $lengths
     * @param  array<string, string>  $orders
     */
    public function __construct(
        public string $name,
        public array $columns,
        public string $type = 'index',
        public array $lengths = [],
        public array $orders = [],
        public string $method = '',
        public string $operatorClass = '',
    ) {
        if ($method !== '' && ! \preg_match('/^[A-Za-z0-9_]+$/', $method)) {
            throw new ValidationException('Invalid index method: ' . $method);
        }
        if ($operatorClass !== '' && ! \preg_match('/^[A-Za-z0-9_.]+$/', $operatorClass)) {
            throw new ValidationException('Invalid operator class: ' . $operatorClass);
        }
    }
}
