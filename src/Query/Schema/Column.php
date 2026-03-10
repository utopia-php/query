<?php

namespace Utopia\Query\Schema;

class Column
{
    public bool $isNullable = false;

    public mixed $default = null;

    public bool $hasDefault = false;

    public bool $isUnsigned = false;

    public bool $isUnique = false;

    public bool $isPrimary = false;

    public bool $isAutoIncrement = false;

    public ?string $after = null;

    public ?string $comment = null;

    /** @var string[] */
    public array $enumValues = [];

    public ?int $srid = null;

    public ?int $dimensions = null;

    public bool $isModify = false;

    public ?string $collation = null;

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
}
