<?php

namespace Utopia\Query\AST;

readonly class SelectStatement
{
    /**
     * @param Expr[] $columns
     * @param JoinClause[] $joins
     * @param Expr[] $groupBy
     * @param OrderByItem[] $orderBy
     * @param CteDefinition[] $ctes
     * @param WindowDefinition[] $windows
     */
    public function __construct(
        public array $columns = [],
        public TableRef|SubquerySource|null $from = null,
        public array $joins = [],
        public ?Expr $where = null,
        public array $groupBy = [],
        public ?Expr $having = null,
        public array $orderBy = [],
        public ?Expr $limit = null,
        public ?Expr $offset = null,
        public bool $distinct = false,
        public array $ctes = [],
        public array $windows = [],
    ) {
    }

    /**
     * Create a copy with modified properties.
     *
     * Uses false as default for nullable properties to distinguish
     * "not passed" from "explicitly set to null".
     *
     * @param Expr[]|null $columns
     * @param JoinClause[]|null $joins
     * @param Expr[]|null $groupBy
     * @param OrderByItem[]|null $orderBy
     * @param CteDefinition[]|null $ctes
     * @param WindowDefinition[]|null $windows
     */
    public function with(
        ?array $columns = null,
        TableRef|SubquerySource|null|false $from = false,
        ?array $joins = null,
        Expr|null|false $where = false,
        ?array $groupBy = null,
        Expr|null|false $having = false,
        ?array $orderBy = null,
        Expr|null|false $limit = false,
        Expr|null|false $offset = false,
        ?bool $distinct = null,
        ?array $ctes = null,
        ?array $windows = null,
    ): self {
        return new self(
            columns: $columns ?? $this->columns,
            from: $from === false ? $this->from : $from,
            joins: $joins ?? $this->joins,
            where: $where === false ? $this->where : $where,
            groupBy: $groupBy ?? $this->groupBy,
            having: $having === false ? $this->having : $having,
            orderBy: $orderBy ?? $this->orderBy,
            limit: $limit === false ? $this->limit : $limit,
            offset: $offset === false ? $this->offset : $offset,
            distinct: $distinct ?? $this->distinct,
            ctes: $ctes ?? $this->ctes,
            windows: $windows ?? $this->windows,
        );
    }
}
