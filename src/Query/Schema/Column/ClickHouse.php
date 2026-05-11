<?php

namespace Utopia\Query\Schema\Column;

use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\Forwarder;
use Utopia\Query\Schema\Table;

/**
 * @extends Column<Table\ClickHouse>
 */
class ClickHouse extends Column
{
    use Forwarder\ClickHouse;

    public protected(set) bool $isLowCardinality = false;

    /** @var list<string> Column-level CODEC clauses, e.g. ['Delta(4)', 'LZ4'] */
    public protected(set) array $codecs = [];

    /**
     * @param  list<string>  $columns
     *
     * @phpstan-return ($columns is array{} ? static : Table\ClickHouse)
     */
    public function primary(array $columns = []): static|Table
    {
        if ($columns === []) {
            $this->isPrimary = true;

            return $this;
        }

        return $this->table->primary($columns);
    }

    /**
     * Wrap the column type in `LowCardinality(...)`.
     *
     * Suitable for string columns with a small number of distinct values
     * (status enums, type discriminators, country codes). `Nullable` is
     * applied outside `LowCardinality` to match ClickHouse's required
     * wrapping order: `Nullable(LowCardinality(String))`.
     */
    public function lowCardinality(): static
    {
        $this->isLowCardinality = true;

        return $this;
    }

    /**
     * Append a column-level CODEC clause.
     *
     * Multiple calls accumulate and emit `CODEC(c1, c2, ...)`. Pass either
     * a bare codec name (`->codec('LZ4')`) or one with arguments
     * (`->codec('Delta(4)')`, `->codec('ZSTD(3)')`). The codec string is
     * emitted verbatim and must come from a trusted source.
     *
     * @throws ValidationException if the codec string is empty or contains
     *                             a semicolon.
     */
    public function codec(string $codec): static
    {
        $trimmed = \trim($codec);

        if ($trimmed === '') {
            throw new ValidationException('CODEC expression must not be empty.');
        }

        if (\str_contains($trimmed, ';')) {
            throw new ValidationException('CODEC expression must not contain ";".');
        }

        $this->codecs[] = $trimmed;

        return $this;
    }
}
