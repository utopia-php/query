<?php

namespace Utopia\Query\Schema\Table;

use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Schema\ClickHouse\Engine;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Table;

/**
 * @extends Table<Column\ClickHouse, ForeignKey>
 */
class ClickHouse extends Table
{
    use Trait\CompositePrimary;

    #[\Override]
    protected function newColumn(string $name, ColumnType $type, ?int $length = null, ?int $precision = null): Column\ClickHouse
    {
        return new Column\ClickHouse($this, $name, $type, $length, $precision);
    }

    public function vector(string $name, int $dimensions): Column\ClickHouse
    {
        $col = $this->newColumn($name, ColumnType::Vector);
        $col->dimensions($dimensions);
        $this->columns[] = $col;

        return $col;
    }

    /**
     * Select the table engine. Engine-specific arguments are validated against
     * the engine variant:
     * - CollapsingMergeTree requires exactly one sign column.
     * - ReplicatedMergeTree requires a zookeeper path and replica name.
     *
     * @throws ValidationException if required engine arguments are missing.
     */
    public function engine(Engine $engine, string ...$args): static
    {
        if ($engine === Engine::CollapsingMergeTree && ! isset($args[0])) {
            throw new ValidationException('CollapsingMergeTree requires a sign column.');
        }

        if ($engine === Engine::ReplicatedMergeTree && (! isset($args[0]) || ! isset($args[1]))) {
            throw new ValidationException('ReplicatedMergeTree requires zookeeper_path and replica_name.');
        }

        $this->engine = $engine;
        $this->engineArgs = \array_values($args);

        return $this;
    }

    /**
     * Set the ORDER BY clause. When unset, ClickHouse falls back to the
     * primary key columns.
     *
     * @param  list<string>  $columns
     *
     * @throws ValidationException if any column name is not a valid identifier.
     */
    public function orderBy(array $columns): static
    {
        foreach ($columns as $column) {
            if (! \preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                throw new ValidationException('Invalid column name in ORDER BY: ' . $column);
            }
        }

        $this->orderBy = $columns;

        return $this;
    }

    /**
     * Attach a table-level TTL expression.
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
     * Set table-level engine SETTINGS.
     *
     * Compiled as `SETTINGS k=v, ...` after the TTL clause. Booleans become
     * `1` / `0`, ints/floats are stringified, strings are passed through after
     * a conservative character allow-list check.
     *
     * Calling this method replaces previously-set settings.
     *
     * @param  array<string, string|int|float|bool>  $settings
     *
     * @throws ValidationException if any key is not a valid identifier or any
     *                             string value contains characters outside the
     *                             allow-list.
     */
    public function settings(array $settings): static
    {
        $sanitized = [];

        foreach ($settings as $key => $value) {
            if (! \preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $key)) {
                throw new ValidationException('Invalid setting name: ' . $key);
            }

            if (\is_bool($value)) {
                $sanitized[$key] = $value ? '1' : '0';
            } elseif (\is_int($value)) {
                $sanitized[$key] = (string) $value;
            } elseif (\is_float($value)) {
                $sanitized[$key] = \rtrim(\rtrim(\sprintf('%F', $value), '0'), '.');
            } elseif (\is_string($value)) {
                if (! \preg_match('/^[A-Za-z0-9_.\-+\/]+$/', $value)) {
                    throw new ValidationException(
                        'Invalid setting value for ' . $key . ': must match [A-Za-z0-9_.\-+/]+'
                    );
                }
                $sanitized[$key] = $value;
            } else {
                throw new ValidationException(
                    'Setting value for ' . $key . ' must be string, int, float, or bool.'
                );
            }
        }

        $this->settings = $sanitized;

        return $this;
    }

    /**
     * Partition the table by an expression. ClickHouse uses a single expression
     * (no Range/List/Hash distinction in the DDL) — the expression itself
     * determines the partition shape (e.g. `toYYYYMM(event_date)`).
     */
    public function partitionBy(string $expression): static
    {
        $this->partitionExpression = $expression;

        return $this;
    }
}
