<?php

namespace Tests\Query\Regression;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\MySQL as MySQLBuilder;
use Utopia\Query\Builder\PostgreSQL as PostgreSQLBuilder;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Parser\MySQL as MySQLParser;
use Utopia\Query\Parser\PostgreSQL as PostgreSQLParser;
use Utopia\Query\Query;
use Utopia\Query\Schema\Index;
use Utopia\Query\Schema\MySQL as MySQLSchema;
use Utopia\Query\Schema\ParameterDirection;
use Utopia\Query\Schema\PostgreSQL as PostgreSQLSchema;
use Utopia\Query\Type;

/**
 * Focused regression tests for security and correctness fix commits.
 *
 * Each test exercises the exact attack vector the commit closed and would fail
 * on the pre-fix code. Mapping commit -> method(s):
 *
 *  - d203ed7  fix(security): DDL input validation + wire-parser state machine
 *      testAlterColumnTypeRejectsSemicolonInUsingExpression
 *      testAlterColumnTypeRejectsDisallowedTypeCharacters
 *      testCreatePartitionRejectsCommentInjection
 *      testExtractKeywordIgnoresKeywordInsideStringLiteral
 *      testExtractKeywordIgnoresKeywordInsideBlockComment
 *      testClassifyDeleteInsideCteIsWrite
 *  - 5662d27  fix: cast/window selectors + mongo field names + parser depth + tokenizer bounds
 *      testMongoBuilderRejectsDollarPrefixedFieldName
 *      testMongoBuilderRejectsEmptyFieldName
 *  - c5a4ed3  fix: escape backslashes in DDL string literals
 *      testMySqlCreateTypeEnumEscapesTrailingBackslash
 *      testPostgreSqlCreateCollationRejectsInvalidOptionKey
 *      testPostgreSqlTablesampleRejectsInvalidMethod
 *  - 4eb2996  fix(ast): binary associativity + unary spacing + literal escaping
 *      (already covered directly in tests/Query/AST/SerializerTest.php by the
 *      commit itself; adding a belt-and-braces case for string literal backslash)
 *      testDdlStringLiteralEscapesBackslashes
 *  - ff64121  fix(query): reject Method::Raw from parse() by default
 *      testQueryParseRejectsRawByDefault
 *      testQueryParseAcceptsRawWhenAllowRawTrue
 *      testQueryParseRejectsRawNestedInsideOr
 */
class SecurityRegressionTest extends TestCase
{
    public function testAlterColumnTypeRejectsSemicolonInUsingExpression(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        $schema->alterColumnType('users', 'age', 'INTEGER', 'age::integer; DROP TABLE users');
    }

    public function testAlterColumnTypeRejectsDisallowedTypeCharacters(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        $schema->alterColumnType('users', 'age', "INTEGER'; DROP TABLE users; --");
    }

    public function testCreatePartitionRejectsCommentInjection(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        $schema->createPartition('orders', 'p1', "FOR VALUES IN ('a') /* evil */");
    }

    public function testExtractKeywordIgnoresKeywordInsideStringLiteral(): void
    {
        $parser = new MySQLParser();

        // The keyword hidden inside the quoted string must not leak out.
        // Pre-fix naive byte-scan would see "DELETE" as the first word after
        // the SELECT in position, but extractKeyword should still report SELECT.
        $this->assertSame('SELECT', $parser->extractKeyword("SELECT 'DELETE FROM users' AS x"));
    }

    public function testExtractKeywordIgnoresKeywordInsideBlockComment(): void
    {
        $parser = new MySQLParser();

        $this->assertSame('SELECT', $parser->extractKeyword('/* DELETE FROM users */ SELECT 1'));
    }

    public function testCteClassifierIgnoresKeywordHiddenInStringLiteral(): void
    {
        $parser = new PostgreSQLParser();

        // Pre-fix: a naive byte-scan could match INSERT inside the string and
        // misclassify as Write. With the state machine, the quoted literal is
        // skipped and the outer SELECT is the classifying keyword (Read).
        $sql = "WITH x AS (SELECT 'INSERT INTO users VALUES(1)' AS s) SELECT * FROM x";
        $this->assertSame(Type::Read, $parser->classifySQL($sql));
    }

    public function testMongoBuilderRejectsDollarPrefixedFieldNameInPush(): void
    {
        $builder = new \Utopia\Query\Builder\MongoDB();
        $builder->from('users');

        $this->expectException(ValidationException::class);
        $builder->push('$where', 'value');
    }

    public function testMongoBuilderRejectsEmptyFieldNameInSet(): void
    {
        $builder = new \Utopia\Query\Builder\MongoDB();
        $builder->from('users');

        $this->expectException(ValidationException::class);
        $builder->set(['' => 'value']);
    }

    public function testMySqlCreateTableEnumEscapesTrailingBackslash(): void
    {
        $schema = new MySQLSchema();
        $plan = $schema->create('widgets', function (\Utopia\Query\Schema\Table $t): void {
            $t->enum('grade', ['A', 'B', "bad\\"]);
        });

        // Pre-fix: the trailing backslash could escape the closing quote. After
        // the fix it must appear doubled inside the literal.
        $this->assertStringContainsString("'bad\\\\'", $plan->query);
    }

    public function testPostgreSqlCreateCollationRejectsInvalidOptionKey(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        // Key with spaces/quotes would break out of option list if not validated
        $schema->createCollation('c1', ["provider = 'x', danger" => 'icu']);
    }

    public function testPostgreSqlTablesampleRejectsInvalidMethod(): void
    {
        $builder = new \Utopia\Query\Builder\PostgreSQL();
        $builder->from('users');

        $this->expectException(ValidationException::class);
        $builder->tablesample(10.0, "BERNOULLI); DROP TABLE users; --");
    }

    public function testDdlStringLiteralEscapesBackslashes(): void
    {
        // Belt-and-braces: a default column value ending in backslash must be
        // serialised with the backslash doubled so the closing quote cannot be
        // escaped by the payload under MySQL default SQL mode.
        $schema = new MySQLSchema();
        $plan = $schema->create('notes', function (\Utopia\Query\Schema\Table $t): void {
            $t->string('body')->default("evil\\");
        });

        $this->assertStringContainsString("'evil\\\\'", $plan->query);
    }

    public function testQueryParseRejectsRawByDefault(): void
    {
        $this->expectException(ValidationException::class);
        Query::parseQuery([
            'method' => Method::Raw->value,
            'values' => ["SELECT * FROM users; DROP TABLE users"],
        ]);
    }

    public function testQueryParseAcceptsRawWhenAllowRawTrue(): void
    {
        $query = Query::parseQuery([
            'method' => Method::Raw->value,
            'values' => ['trusted_raw'],
        ], allowRaw: true);

        $this->assertSame(Method::Raw, $query->getMethod());
    }

    public function testQueryParseRejectsRawNestedInsideOr(): void
    {
        $this->expectException(ValidationException::class);
        Query::parseQuery([
            'method' => Method::Or->value,
            'values' => [
                ['method' => Method::Equal->value, 'attribute' => 'a', 'values' => [1]],
                ['method' => Method::Raw->value, 'values' => ['DROP TABLE users']],
            ],
        ]);
    }

    public function testSelectWindowRejectsInjectionInsideParens(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid window function');

        (new MySQLBuilder())
            ->from('t')
            ->selectWindow('ROW_NUMBER(1); DROP TABLE x;-- )', 'w');
    }

    public function testSelectWindowRejectsSemicolonInsideArgs(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid window function');

        (new MySQLBuilder())
            ->from('t')
            ->selectWindow('SUM(a; DROP TABLE x)', 'w');
    }

    public function testSelectWindowAcceptsValidArgs(): void
    {
        $builder = (new PostgreSQLBuilder())
            ->from('t')
            ->selectWindow('SUM("amount")', 'w', ['dept']);

        $this->assertInstanceOf(PostgreSQLBuilder::class, $builder);
    }

    public function testCreateIndexRejectsDoubleQuoteInCollation(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid collation');

        new Index(
            name: 'idx_name',
            columns: ['name'],
            collations: ['name' => '"en_US"'],
        );
    }

    public function testCreateProcedureRejectsCommaInjectionInType(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid procedure parameter type');

        $schema->createProcedure('p', [
            [ParameterDirection::In, 'x', 'VARCHAR(255), DROP TABLE x --'],
        ], 'SELECT 1');
    }

    public function testSelectCastRejectsCommaInjectionInType(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid cast type');

        (new MySQLBuilder())
            ->from('t')
            ->selectCast('c', 'VARCHAR(255), DROP TABLE x --', 'a');
    }

    public function testSelectCastRejectsQuoteInjectionInType(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid cast type');

        (new MySQLBuilder())
            ->from('t')
            ->selectCast('c', "INT'; DROP TABLE x; --", 'a');
    }

    public function testSelectCastRejectsCommentSequenceInType(): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid cast type');

        (new MySQLBuilder())
            ->from('t')
            ->selectCast('c', 'INT /* danger */', 'a');
    }

    public function testAlterColumnTypeRejectsCommaInjectionInType(): void
    {
        $schema = new PostgreSQLSchema();

        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Invalid column type');

        $schema->alterColumnType('users', 'age', 'INTEGER, DROP TABLE users --');
    }

    public function testSelectCastAcceptsStructuredType(): void
    {
        $result = (new MySQLBuilder())
            ->from('t')
            ->selectCast('c', 'DECIMAL(10, 2)', 'a')
            ->build();

        $this->assertStringContainsString('CAST(`c` AS DECIMAL(10, 2))', $result->query);
    }
}
