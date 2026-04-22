<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Exception\ValidationException;

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

    public private(set) ?string $checkExpression = null;

    public private(set) ?string $generatedExpression = null;

    /**
     * Null when {@see generatedAs()} has not been called.
     * True = STORED, false = VIRTUAL.
     */
    public private(set) ?bool $generatedStored = null;

    public private(set) ?string $ttl = null;

    public private(set) ?string $userTypeName = null;

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

    /**
     * Attach a column-level CHECK constraint.
     *
     * The expression is emitted verbatim inside `CHECK (...)` and must come from
     * trusted (developer-controlled) source — never from untrusted input.
     */
    public function check(string $expression): static
    {
        $this->checkExpression = $expression;

        return $this;
    }

    /**
     * Mark the column as a generated column computed from the given expression.
     *
     * The expression is emitted verbatim inside `GENERATED ALWAYS AS (...)` and
     * must come from trusted (developer-controlled) source — never from untrusted
     * input.
     */
    public function generatedAs(string $expression): static
    {
        $this->generatedExpression = $expression;

        return $this;
    }

    /**
     * Mark a generated column as STORED. Mutually exclusive with {@see virtual()}.
     */
    public function stored(): static
    {
        $this->generatedStored = true;

        return $this;
    }

    /**
     * Mark a generated column as VIRTUAL. Mutually exclusive with {@see stored()}.
     */
    public function virtual(): static
    {
        $this->generatedStored = false;

        return $this;
    }

    /**
     * Attach a column-level TTL expression (ClickHouse only).
     *
     * Emitted verbatim as `TTL <expression>` inline with the column
     * definition. Other dialects throw UnsupportedException when compiling
     * the column.
     *
     * @throws ValidationException if the expression is empty or contains a semicolon.
     */
    public function ttl(string $expression): static
    {
        $trimmed = \trim($expression);

        if ($trimmed === '') {
            throw new ValidationException('TTL expression must not be empty.');
        }

        if (\str_contains($trimmed, ';')) {
            throw new ValidationException('TTL expression must not contain ";".');
        }

        $this->ttl = $trimmed;

        return $this;
    }

    /**
     * Reference a user-defined type (e.g. a PostgreSQL enum type created via CREATE TYPE).
     *
     * The column's emitted type will be the quoted identifier, overriding the mapping
     * implied by its ColumnType. Only supported by dialects that implement user-defined
     * types (currently PostgreSQL); other dialects throw UnsupportedException when
     * compiling the column.
     *
     * @throws ValidationException if $name is not a valid identifier.
     */
    public function userType(string $name): static
    {
        if (! \preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $name)) {
            throw new ValidationException('Invalid user-defined type name: ' . $name);
        }

        $this->userTypeName = $name;

        return $this;
    }
}
