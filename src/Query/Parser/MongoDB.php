<?php

namespace Utopia\Query\Parser;

use Utopia\Query\Parser;
use Utopia\Query\Type;

/**
 * MongoDB Wire Protocol Parser
 *
 * Parses MongoDB OP_MSG (opcode 2013) messages and classifies commands.
 *
 * OP_MSG format:
 * - Bytes 0-3: Message length (little-endian int32)
 * - Bytes 4-7: Request ID
 * - Bytes 8-11: Response To
 * - Bytes 12-15: Opcode (2013 = OP_MSG)
 * - Bytes 16-19: Flag bits
 * - Byte 20: Section kind (0 = body)
 * - Bytes 21+: BSON document
 *
 * The first key in the BSON body document is the command name.
 * Classification is based on the command name:
 * - Read: find, aggregate, count, distinct, listCollections, etc.
 * - Write: insert, update, delete, create, drop, createIndexes, etc.
 * - TransactionBegin: startTransaction flag present
 * - TransactionEnd: commitTransaction or abortTransaction command
 */
class MongoDB implements Parser
{
    /**
     * Read command names (lowercase)
     *
     * @var array<string, true>
     */
    private const READ_COMMANDS = [
        'find' => true,
        'aggregate' => true,
        'count' => true,
        'distinct' => true,
        'listCollections' => true,
        'listDatabases' => true,
        'listIndexes' => true,
        'dbStats' => true,
        'collStats' => true,
        'explain' => true,
        'getMore' => true,
        'serverStatus' => true,
        'buildInfo' => true,
        'connectionStatus' => true,
        'ping' => true,
        'isMaster' => true,
        'ismaster' => true,
        'hello' => true,
    ];

    /**
     * Write command names (lowercase)
     *
     * @var array<string, true>
     */
    private const WRITE_COMMANDS = [
        'insert' => true,
        'update' => true,
        'delete' => true,
        'findAndModify' => true,
        'create' => true,
        'drop' => true,
        'createIndexes' => true,
        'dropIndexes' => true,
        'dropDatabase' => true,
        'renameCollection' => true,
    ];

    /**
     * MongoDB OP_MSG opcode
     */
    private const OP_MSG = 2013;

    /**
     * Minimum OP_MSG size: header (16) + flags (4) + section kind (1) + min BSON doc (5)
     */
    private const MIN_MSG_SIZE = 26;

    public function parse(string $data): Type
    {
        $len = \strlen($data);
        if ($len < self::MIN_MSG_SIZE) {
            return Type::Unknown;
        }

        // Verify opcode is OP_MSG (2013)
        $opcode = \unpack('V', $data, 12)[1];
        if ($opcode !== self::OP_MSG) {
            return Type::Unknown;
        }

        // Byte 20: section kind (0 = body)
        if (\ord($data[20]) !== 0) {
            return Type::Unknown;
        }

        // BSON document starts at byte 21
        // BSON doc: 4-byte length, then elements
        // Each element: type byte, cstring name, value
        $bsonOffset = 21;

        if ($bsonOffset + 4 > $len) {
            return Type::Unknown;
        }

        // Check for startTransaction flag in the document
        if ($this->hasBsonKey($data, $bsonOffset, 'startTransaction')) {
            return Type::TransactionBegin;
        }

        // Extract the first key name (the command name)
        $commandName = $this->extractFirstBsonKey($data, $bsonOffset);

        if ($commandName === null) {
            return Type::Unknown;
        }

        // Transaction end commands
        if ($commandName === 'commitTransaction' || $commandName === 'abortTransaction') {
            return Type::TransactionEnd;
        }

        // Read commands
        if (isset(self::READ_COMMANDS[$commandName])) {
            return Type::Read;
        }

        // Write commands
        if (isset(self::WRITE_COMMANDS[$commandName])) {
            return Type::Write;
        }

        return Type::Unknown;
    }

    /**
     * Not applicable — MongoDB does not use SQL
     */
    public function classifySQL(string $query): Type
    {
        return Type::Unknown;
    }

    /**
     * Not applicable — MongoDB does not use SQL
     */
    public function extractKeyword(string $query): string
    {
        return '';
    }

    /**
     * Extract the first key name from a BSON document
     *
     * BSON element: type byte (1) + cstring key + value
     * We only need the key name, not the value.
     */
    private function extractFirstBsonKey(string $data, int $bsonOffset): ?string
    {
        $len = \strlen($data);

        // Skip BSON document length (4 bytes)
        $pos = $bsonOffset + 4;

        if ($pos >= $len) {
            return null;
        }

        // Type byte (0x00 = end of document)
        $type = \ord($data[$pos]);
        if ($type === 0x00) {
            return null;
        }

        $pos++;

        // Read cstring key (null-terminated)
        $keyStart = $pos;
        while ($pos < $len && $data[$pos] !== "\x00") {
            $pos++;
        }

        if ($pos >= $len) {
            return null;
        }

        return \substr($data, $keyStart, $pos - $keyStart);
    }

    /**
     * Check if a BSON document contains a specific key
     *
     * Scans through BSON elements looking for the key name.
     * Skips values based on BSON type to advance through elements.
     */
    private function hasBsonKey(string $data, int $bsonOffset, string $targetKey): bool
    {
        $len = \strlen($data);

        if ($bsonOffset + 4 > $len) {
            return false;
        }

        $docLen = \unpack('V', $data, $bsonOffset)[1];
        $docEnd = $bsonOffset + $docLen;
        if ($docEnd > $len) {
            $docEnd = $len;
        }

        $pos = $bsonOffset + 4;

        while ($pos < $docEnd) {
            $type = \ord($data[$pos]);
            if ($type === 0x00) {
                break;
            }
            $pos++;

            // Read key name (cstring)
            $keyStart = $pos;
            while ($pos < $docEnd && $data[$pos] !== "\x00") {
                $pos++;
            }

            if ($pos >= $docEnd) {
                break;
            }

            $key = \substr($data, $keyStart, $pos - $keyStart);
            $pos++; // skip null terminator

            if ($key === $targetKey) {
                return true;
            }

            // Skip value based on type
            $pos = $this->skipBsonValue($data, $pos, $type, $docEnd);
            if ($pos === false) {
                break;
            }
        }

        return false;
    }

    /**
     * Skip a BSON value to advance past it
     *
     * @return int|false New position after the value, or false on error
     */
    private function skipBsonValue(string $data, int $pos, int $type, int $limit): int|false
    {
        return match ($type) {
            0x01 => $pos + 8,                    // double (8 bytes)
            0x02, 0x0D, 0x0E => $this->skipBsonString($data, $pos, $limit), // string, JavaScript, Symbol
            0x03, 0x04 => $this->skipBsonDocument($data, $pos, $limit),     // document, array
            0x05 => $this->skipBsonBinary($data, $pos, $limit),             // binary
            0x06, 0x0A => $pos,                  // undefined, null (0 bytes)
            0x07 => $pos + 12,                   // ObjectId (12 bytes)
            0x08 => $pos + 1,                    // boolean (1 byte)
            0x09, 0x11, 0x12 => $pos + 8,        // datetime, timestamp, int64
            0x0B => $this->skipBsonRegex($data, $pos, $limit), // regex (2 cstrings)
            0x0C => $this->skipBsonDbPointer($data, $pos, $limit), // DBPointer
            0x10 => $pos + 4,                    // int32 (4 bytes)
            0x13 => $pos + 16,                   // decimal128 (16 bytes)
            0xFF, 0x7F => $pos,                  // min/max key (0 bytes)
            default => false,
        };
    }

    private function skipBsonString(string $data, int $pos, int $limit): int|false
    {
        if ($pos + 4 > $limit) {
            return false;
        }
        $strLen = \unpack('V', $data, $pos)[1];

        return $pos + 4 + $strLen;
    }

    private function skipBsonDocument(string $data, int $pos, int $limit): int|false
    {
        if ($pos + 4 > $limit) {
            return false;
        }
        $docLen = \unpack('V', $data, $pos)[1];

        return $pos + $docLen;
    }

    private function skipBsonBinary(string $data, int $pos, int $limit): int|false
    {
        if ($pos + 4 > $limit) {
            return false;
        }
        $binLen = \unpack('V', $data, $pos)[1];

        return $pos + 4 + 1 + $binLen; // length + subtype byte + data
    }

    private function skipBsonRegex(string $data, int $pos, int $limit): int|false
    {
        // Two cstrings: pattern + options
        while ($pos < $limit && $data[$pos] !== "\x00") {
            $pos++;
        }
        $pos++; // skip null
        while ($pos < $limit && $data[$pos] !== "\x00") {
            $pos++;
        }

        return $pos + 1;
    }

    private function skipBsonDbPointer(string $data, int $pos, int $limit): int|false
    {
        // string (4-byte len + data) + 12-byte ObjectId
        $newPos = $this->skipBsonString($data, $pos, $limit);
        if ($newPos === false) {
            return false;
        }

        return $newPos + 12;
    }
}
