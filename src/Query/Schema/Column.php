<?php

namespace Utopia\Query\Schema;

class Column
{
    public private(set) bool $isNullable = false;

    public private(set) mixed $default = null;

    public private(set) bool $hasDefault = false;

    public private(set) bool $isUnsigned = false;

    public private(set) bool $isUnique = false;

    public private(set) bool $isPrimary = false;

    public private(set) bool $isAutoIncrement = false;

    public private(set) ?string $after = null;

    public private(set) ?string $comment = null;

    /** @var string[] */
    public private(set) array $enumValues = [];

    public private(set) ?int $srid = null;

    public private(set) ?int $dimensions = null;

    public private(set) bool $isModify = false;

    public private(set) ?string $collation = null;

    public function __construct(
        public string $name,
        public ColumnType $type,
        public ?int $length = null,
        public ?int $precision = null,
    ) {
    }

    public function nullable(): static
    {
        $this->isNullable = true;

        return $this;
    }

    public function default(mixed $value): static
    {
        $this->default = $value;
        $this->hasDefault = true;

        return $this;
    }

    public function unsigned(): static
    {
        $this->isUnsigned = true;

        return $this;
    }

    public function unique(): static
    {
        $this->isUnique = true;

        return $this;
    }

    public function primary(): static
    {
        $this->isPrimary = true;

        return $this;
    }

    public function after(string $column): static
    {
        $this->after = $column;

        return $this;
    }

    public function autoIncrement(): static
    {
        $this->isAutoIncrement = true;

        return $this;
    }

    public function comment(string $comment): static
    {
        $this->comment = $comment;

        return $this;
    }

    public function collation(string $collation): static
    {
        $this->collation = $collation;

        return $this;
    }

    /**
     * @param  string[]  $values
     */
    public function enum(array $values): static
    {
        $this->enumValues = $values;

        return $this;
    }

    public function srid(int $srid): static
    {
        $this->srid = $srid;

        return $this;
    }

    public function dimensions(int $dimensions): static
    {
        $this->dimensions = $dimensions;

        return $this;
    }

    public function modify(): static
    {
        $this->isModify = true;

        return $this;
    }
}
