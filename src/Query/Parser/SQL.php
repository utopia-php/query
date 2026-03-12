<?php

namespace Utopia\Query\Parser;

use Utopia\Query\Parser;
use Utopia\Query\Type;

/**
 * Abstract SQL Parser
 *
 * Provides shared SQL classification logic used by all wire protocol parsers.
 * Subclasses implement parse() for their specific wire protocol format.
 *
 * Performance: Uses byte-level checks and simple string operations (no regex).
 * Designed to run on every packet with sub-microsecond overhead.
 */
abstract class SQL implements Parser
{
    /**
     * Read keywords lookup (uppercase)
     *
     * @var array<string, true>
     */
    private const READ_KEYWORDS = [
        'SELECT' => true,
        'SHOW' => true,
        'DESCRIBE' => true,
        'DESC' => true,
        'EXPLAIN' => true,
        'TABLE' => true,
        'VALUES' => true,
    ];

    /**
     * Write keywords lookup (uppercase)
     *
     * @var array<string, true>
     */
    private const WRITE_KEYWORDS = [
        'INSERT' => true,
        'UPDATE' => true,
        'DELETE' => true,
        'CREATE' => true,
        'DROP' => true,
        'ALTER' => true,
        'TRUNCATE' => true,
        'GRANT' => true,
        'REVOKE' => true,
        'LOCK' => true,
        'CALL' => true,
        'DO' => true,
    ];

    /**
     * Transaction-begin keywords (uppercase)
     *
     * @var array<string, true>
     */
    private const TRANSACTION_BEGIN_KEYWORDS = [
        'BEGIN' => true,
        'START' => true,
    ];

    /**
     * Transaction-end keywords (uppercase)
     *
     * @var array<string, true>
     */
    private const TRANSACTION_END_KEYWORDS = [
        'COMMIT' => true,
        'ROLLBACK' => true,
    ];

    /**
     * Other transaction keywords (uppercase)
     *
     * @var array<string, true>
     */
    private const TRANSACTION_KEYWORDS = [
        'SAVEPOINT' => true,
        'RELEASE' => true,
        'SET' => true,
    ];

    /**
     * Classify a SQL query string by its leading keyword
     *
     * Handles:
     * - Leading whitespace (spaces, tabs, newlines)
     * - SQL comments: line comments (--) and block comments
     * - Mixed case keywords
     * - COPY ... TO (read) vs COPY ... FROM (write)
     * - CTE: WITH ... SELECT (read) vs WITH ... INSERT/UPDATE/DELETE (write)
     */
    public function classifySQL(string $query): Type
    {
        $keyword = $this->extractKeyword($query);

        if ($keyword === '') {
            return Type::Unknown;
        }

        // Fast hash-based lookup
        if (isset(self::READ_KEYWORDS[$keyword])) {
            return Type::Read;
        }

        if (isset(self::WRITE_KEYWORDS[$keyword])) {
            return Type::Write;
        }

        if (isset(self::TRANSACTION_BEGIN_KEYWORDS[$keyword])) {
            return Type::TransactionBegin;
        }

        if (isset(self::TRANSACTION_END_KEYWORDS[$keyword])) {
            return Type::TransactionEnd;
        }

        if (isset(self::TRANSACTION_KEYWORDS[$keyword])) {
            return Type::Transaction;
        }

        // COPY requires directional analysis: COPY ... TO = read, COPY ... FROM = write
        if ($keyword === 'COPY') {
            return $this->classifyCopy($query);
        }

        // WITH (CTE): look at the final statement keyword
        if ($keyword === 'WITH') {
            return $this->classifyCTE($query);
        }

        return Type::Unknown;
    }

    /**
     * Extract the first SQL keyword from a query string
     *
     * Skips leading whitespace and SQL comments efficiently.
     * Returns the keyword in uppercase for classification.
     */
    public function extractKeyword(string $query): string
    {
        $len = \strlen($query);
        $pos = 0;

        // Skip leading whitespace and comments
        while ($pos < $len) {
            $c = $query[$pos];

            // Skip whitespace
            if ($c === ' ' || $c === "\t" || $c === "\n" || $c === "\r" || $c === "\f") {
                $pos++;

                continue;
            }

            // Skip line comments: -- ...
            if ($c === '-' && ($pos + 1) < $len && $query[$pos + 1] === '-') {
                $pos += 2;
                while ($pos < $len && $query[$pos] !== "\n") {
                    $pos++;
                }

                continue;
            }

            // Skip block comments: /* ... */
            if ($c === '/' && ($pos + 1) < $len && $query[$pos + 1] === '*') {
                $pos += 2;
                while ($pos < ($len - 1)) {
                    if ($query[$pos] === '*' && $query[$pos + 1] === '/') {
                        $pos += 2;

                        break;
                    }
                    $pos++;
                }

                continue;
            }

            break;
        }

        if ($pos >= $len) {
            return '';
        }

        // Read keyword until whitespace, '(', ';', or end
        $start = $pos;
        while ($pos < $len) {
            $c = $query[$pos];
            if ($c === ' ' || $c === "\t" || $c === "\n" || $c === "\r" || $c === '(' || $c === ';') {
                break;
            }
            $pos++;
        }

        if ($pos === $start) {
            return '';
        }

        return \strtoupper(\substr($query, $start, $pos - $start));
    }

    /**
     * Classify COPY statement direction
     *
     * COPY ... TO stdout/file = READ (export)
     * COPY ... FROM stdin/file = WRITE (import)
     * Default to WRITE for safety
     */
    private function classifyCopy(string $query): Type
    {
        $toPos = \stripos($query, ' TO ');
        $fromPos = \stripos($query, ' FROM ');

        if ($toPos !== false && ($fromPos === false || $toPos < $fromPos)) {
            return Type::Read;
        }

        return Type::Write;
    }

    /**
     * Classify CTE (WITH ... AS (...) SELECT/INSERT/UPDATE/DELETE ...)
     *
     * After the CTE definitions, the first read/write keyword at
     * parenthesis depth 0 is the main statement.
     * Default to READ since most CTEs are used with SELECT.
     */
    private function classifyCTE(string $query): Type
    {
        $len = \strlen($query);
        $pos = 0;
        $depth = 0;
        $seenParen = false;

        while ($pos < $len) {
            $c = $query[$pos];

            if ($c === '(') {
                $depth++;
                $seenParen = true;
                $pos++;

                continue;
            }

            if ($c === ')') {
                $depth--;
                $pos++;

                continue;
            }

            // Only look for keywords at depth 0, after we've seen at least one CTE block
            if ($depth === 0 && $seenParen && ($c >= 'A' && $c <= 'Z' || $c >= 'a' && $c <= 'z')) {
                $wordStart = $pos;
                while ($pos < $len) {
                    $ch = $query[$pos];
                    if (($ch >= 'A' && $ch <= 'Z') || ($ch >= 'a' && $ch <= 'z') || ($ch >= '0' && $ch <= '9') || $ch === '_') {
                        $pos++;
                    } else {
                        break;
                    }
                }
                $word = \strtoupper(\substr($query, $wordStart, $pos - $wordStart));

                if (isset(self::READ_KEYWORDS[$word])) {
                    return Type::Read;
                }

                if (isset(self::WRITE_KEYWORDS[$word])) {
                    return Type::Write;
                }

                continue;
            }

            $pos++;
        }

        return Type::Read;
    }
}
