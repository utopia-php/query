<?php

namespace Utopia\Query\Hook;

use Utopia\Query\Condition;

class PermissionFilterHook implements FilterHook
{
    /**
     * @param  list<string>  $roles
     */
    public function __construct(
        protected string $namespace,
        protected array $roles,
        protected string $type = 'read',
        protected string $documentColumn = '_uid',
    ) {
    }

    public function filter(string $table): Condition
    {
        if (empty($this->roles)) {
            return new Condition('1 = 0');
        }

        $placeholders = implode(', ', array_fill(0, count($this->roles), '?'));

        return new Condition(
            "{$this->documentColumn} IN (SELECT DISTINCT _document FROM {$this->namespace}_{$table}_perms WHERE _permission IN ({$placeholders}) AND _type = ?)",
            [...$this->roles, $this->type],
        );
    }
}
