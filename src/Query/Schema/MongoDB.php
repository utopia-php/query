<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder;
use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Schema;

class MongoDB extends Schema
{
    protected function quote(string $identifier): string
    {
        return $identifier;
    }

    protected function compileColumnType(Column $column): string
    {
        return match ($column->type) {
            ColumnType::String, ColumnType::Varchar, ColumnType::Relationship => 'string',
            ColumnType::Text, ColumnType::MediumText, ColumnType::LongText => 'string',
            ColumnType::Integer, ColumnType::BigInteger, ColumnType::Id => 'int',
            ColumnType::Float, ColumnType::Double => 'double',
            ColumnType::Boolean => 'bool',
            ColumnType::Datetime, ColumnType::Timestamp => 'date',
            ColumnType::Json, ColumnType::Object => 'object',
            ColumnType::Binary => 'binData',
            ColumnType::Enum => 'string',
            ColumnType::Point => 'object',
            ColumnType::Linestring, ColumnType::Polygon => 'object',
            ColumnType::Uuid7 => 'string',
            ColumnType::Vector => 'array',
        };
    }

    protected function compileAutoIncrement(): string
    {
        return '';
    }

    /**
     * @param callable(Blueprint): void $definition
     */
    public function create(string $table, callable $definition, bool $ifNotExists = false): BuildResult
    {
        $blueprint = new Blueprint();
        $definition($blueprint);

        $properties = [];
        $required = [];

        foreach ($blueprint->columns as $column) {
            $bsonType = $this->compileColumnType($column);

            $prop = ['bsonType' => $bsonType];

            if ($column->comment !== null) {
                $prop['description'] = $column->comment;
            }

            if ($column->type === ColumnType::Enum && ! empty($column->enumValues)) {
                $prop['enum'] = $column->enumValues;
            }

            $properties[$column->name] = $prop;

            if (! $column->isNullable && ! $column->hasDefault) {
                $required[] = $column->name;
            }
        }

        $validator = [];
        if (! empty($properties)) {
            $schema = [
                'bsonType' => 'object',
                'properties' => $properties,
            ];
            if (! empty($required)) {
                $schema['required'] = $required;
            }
            $validator = ['$jsonSchema' => $schema];
        }

        $command = [
            'command' => 'createCollection',
            'collection' => $table,
        ];

        if (! empty($validator)) {
            $command['validator'] = $validator;
        }

        return new BuildResult(
            \json_encode($command, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            []
        );
    }

    /**
     * @param callable(Blueprint): void $definition
     */
    public function alter(string $table, callable $definition): BuildResult
    {
        $blueprint = new Blueprint();
        $definition($blueprint);

        if (! empty($blueprint->dropColumns) || ! empty($blueprint->renameColumns)) {
            throw new UnsupportedException('MongoDB does not support dropping or renaming columns via schema. Use $unset/$rename update operators.');
        }

        $properties = [];
        $required = [];

        foreach ($blueprint->columns as $column) {
            $bsonType = $this->compileColumnType($column);
            $prop = ['bsonType' => $bsonType];

            if ($column->comment !== null) {
                $prop['description'] = $column->comment;
            }

            $properties[$column->name] = $prop;

            if (! $column->isNullable && ! $column->hasDefault) {
                $required[] = $column->name;
            }
        }

        $validator = [];
        if (! empty($properties)) {
            $schema = [
                'bsonType' => 'object',
                'properties' => $properties,
            ];
            if (! empty($required)) {
                $schema['required'] = $required;
            }
            $validator = ['$jsonSchema' => $schema];
        }

        $command = [
            'command' => 'collMod',
            'collection' => $table,
        ];

        if (! empty($validator)) {
            $command['validator'] = $validator;
        }

        return new BuildResult(
            \json_encode($command, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            []
        );
    }

    public function drop(string $table): BuildResult
    {
        return new BuildResult(
            \json_encode(['command' => 'drop', 'collection' => $table], JSON_THROW_ON_ERROR),
            []
        );
    }

    public function dropIfExists(string $table): BuildResult
    {
        return $this->drop($table);
    }

    public function rename(string $from, string $to): BuildResult
    {
        return new BuildResult(
            \json_encode([
                'command' => 'renameCollection',
                'from' => $from,
                'to' => $to,
            ], JSON_THROW_ON_ERROR),
            []
        );
    }

    public function truncate(string $table): BuildResult
    {
        return new BuildResult(
            \json_encode([
                'command' => 'deleteMany',
                'collection' => $table,
                'filter' => new \stdClass(),
            ], JSON_THROW_ON_ERROR),
            []
        );
    }

    /**
     * @param string[] $columns
     * @param array<string, int> $lengths
     * @param array<string, string> $orders
     * @param array<string, string> $collations
     * @param list<string> $rawColumns
     */
    public function createIndex(
        string $table,
        string $name,
        array $columns,
        bool $unique = false,
        string $type = '',
        string $method = '',
        string $operatorClass = '',
        array $lengths = [],
        array $orders = [],
        array $collations = [],
        array $rawColumns = [],
    ): BuildResult {
        $keys = [];
        foreach ($columns as $col) {
            $direction = 1;
            if (isset($orders[$col])) {
                $direction = \strtolower($orders[$col]) === 'desc' ? -1 : 1;
            }
            $keys[$col] = $direction;
        }

        $index = [
            'key' => $keys,
            'name' => $name,
        ];

        if ($unique) {
            $index['unique'] = true;
        }

        $command = [
            'command' => 'createIndex',
            'collection' => $table,
            'index' => $index,
        ];

        return new BuildResult(
            \json_encode($command, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            []
        );
    }

    public function dropIndex(string $table, string $name): BuildResult
    {
        return new BuildResult(
            \json_encode([
                'command' => 'dropIndex',
                'collection' => $table,
                'index' => $name,
            ], JSON_THROW_ON_ERROR),
            []
        );
    }

    public function createView(string $name, Builder $query): BuildResult
    {
        $result = $query->build();

        /** @var array<string, mixed>|null $op */
        $op = \json_decode($result->query, true);
        if ($op === null) {
            throw new UnsupportedException('Cannot parse query for MongoDB view creation.');
        }

        $command = [
            'command' => 'createView',
            'view' => $name,
            'source' => $op['collection'] ?? '',
            'pipeline' => $op['pipeline'] ?? [],
        ];

        return new BuildResult(
            \json_encode($command, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $result->bindings
        );
    }

    public function createDatabase(string $name): BuildResult
    {
        return new BuildResult(
            \json_encode(['command' => 'createDatabase', 'database' => $name], JSON_THROW_ON_ERROR),
            []
        );
    }

    public function dropDatabase(string $name): BuildResult
    {
        return new BuildResult(
            \json_encode(['command' => 'dropDatabase', 'database' => $name], JSON_THROW_ON_ERROR),
            []
        );
    }

    public function analyzeTable(string $table): BuildResult
    {
        return new BuildResult(
            \json_encode(['command' => 'collStats', 'collection' => $table], JSON_THROW_ON_ERROR),
            []
        );
    }
}
