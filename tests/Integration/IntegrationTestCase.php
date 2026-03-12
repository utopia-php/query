<?php

namespace Tests\Integration;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\BuildResult;

abstract class IntegrationTestCase extends TestCase
{
    protected ?PDO $mysql = null;

    protected ?PDO $postgres = null;

    protected ?ClickHouseClient $clickhouse = null;

    protected ?MongoDBClient $mongoClient = null;

    /** @var list<string> */
    private array $mysqlCleanup = [];

    /** @var list<string> */
    private array $postgresCleanup = [];

    /** @var list<string> */
    private array $clickhouseCleanup = [];

    /** @var list<string> */
    private array $mongoCleanup = [];

    protected function connectMysql(): PDO
    {
        if ($this->mysql === null) {
            $this->mysql = new PDO(
                'mysql:host=127.0.0.1;port=13306;dbname=query_test',
                'root',
                'test',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        }

        return $this->mysql;
    }

    protected function connectPostgres(): PDO
    {
        if ($this->postgres === null) {
            $this->postgres = new PDO(
                'pgsql:host=127.0.0.1;port=15432;dbname=query_test',
                'postgres',
                'test',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
        }

        return $this->postgres;
    }

    protected function connectClickhouse(): ClickHouseClient
    {
        if ($this->clickhouse === null) {
            $this->clickhouse = new ClickHouseClient();
        }

        return $this->clickhouse;
    }

    protected function connectMongoDB(): MongoDBClient
    {
        if ($this->mongoClient === null) {
            $this->mongoClient = new MongoDBClient();
        }

        return $this->mongoClient;
    }

    /**
     * @return list<array<string, mixed>>
     */
    protected function executeOnMongoDB(BuildResult $result): array
    {
        $mongo = $this->connectMongoDB();

        return $mongo->execute($result->query, $result->bindings);
    }

    protected function trackMongoCollection(string $collection): void
    {
        $this->mongoCleanup[] = $collection;
    }

    /**
     * @return list<array<string, mixed>>
     */
    protected function executeOnMysql(BuildResult $result): array
    {
        $pdo = $this->connectMysql();
        $stmt = $pdo->prepare($result->query);

        foreach ($result->bindings as $i => $value) {
            $type = match (true) {
                is_bool($value) => PDO::PARAM_BOOL,
                is_int($value) => PDO::PARAM_INT,
                $value === null => PDO::PARAM_NULL,
                default => PDO::PARAM_STR,
            };
            $stmt->bindValue($i + 1, $value, $type);
        }
        $stmt->execute();

        /** @var list<array<string, mixed>> */
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * @return list<array<string, mixed>>
     */
    protected function executeOnPostgres(BuildResult $result): array
    {
        $pdo = $this->connectPostgres();
        $stmt = $pdo->prepare($result->query);

        foreach ($result->bindings as $i => $value) {
            $type = match (true) {
                is_bool($value) => PDO::PARAM_BOOL,
                is_int($value) => PDO::PARAM_INT,
                $value === null => PDO::PARAM_NULL,
                default => PDO::PARAM_STR,
            };
            $stmt->bindValue($i + 1, $value, $type);
        }
        $stmt->execute();

        /** @var list<array<string, mixed>> */
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * @return list<array<string, mixed>>
     */
    protected function executeOnClickhouse(BuildResult $result): array
    {
        $ch = $this->connectClickhouse();

        return $ch->execute($result->query, $result->bindings);
    }

    protected function mysqlStatement(string $sql): void
    {
        $this->connectMysql()->prepare($sql)->execute();
    }

    protected function postgresStatement(string $sql): void
    {
        $this->connectPostgres()->prepare($sql)->execute();
    }

    protected function clickhouseStatement(string $sql): void
    {
        $this->connectClickhouse()->statement($sql);
    }

    protected function trackMysqlTable(string $table): void
    {
        $this->mysqlCleanup[] = $table;
    }

    protected function trackPostgresTable(string $table): void
    {
        $this->postgresCleanup[] = $table;
    }

    protected function trackClickhouseTable(string $table): void
    {
        $this->clickhouseCleanup[] = $table;
    }

    protected function tearDown(): void
    {
        $this->mysql?->exec('SET FOREIGN_KEY_CHECKS = 0');
        foreach ($this->mysqlCleanup as $table) {
            $stmt = $this->mysql?->prepare("DROP TABLE IF EXISTS `{$table}`");
            if ($stmt !== null && $stmt !== false) {
                $stmt->execute();
            }
        }
        $this->mysql?->exec('SET FOREIGN_KEY_CHECKS = 1');

        foreach ($this->postgresCleanup as $table) {
            $stmt = $this->postgres?->prepare("DROP TABLE IF EXISTS \"{$table}\" CASCADE");
            if ($stmt !== null && $stmt !== false) {
                $stmt->execute();
            }
        }

        foreach ($this->clickhouseCleanup as $table) {
            $this->clickhouse?->statement("DROP TABLE IF EXISTS `{$table}`");
        }

        foreach ($this->mongoCleanup as $collection) {
            $this->mongoClient?->dropCollection($collection);
        }

        $this->mysqlCleanup = [];
        $this->postgresCleanup = [];
        $this->clickhouseCleanup = [];
        $this->mongoCleanup = [];
    }
}
