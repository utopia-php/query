# Implementation Plan: SQL Tokenizer & AST

**Status:** In Progress
**Created:** 2026-03-24
**Description:** Add a tokenizer, parser, AST node hierarchy, serializer, visitor pattern, and Builder integration for SQL SELECT queries. Supports per-dialect tokenization/serialization for MySQL, PostgreSQL, ClickHouse, SQLite, and MariaDB. Enables round-trip: SQL string → tokens → AST → modify/validate → SQL string, plus AST ↔ Builder conversion.

## Phases

### Phase 1: Token Types & Base Tokenizer
- **Status:** [ ] Pending
- **What:** Define `TokenType` enum, `Token` readonly class, and a base `Tokenizer` class that lexes standard SQL into tokens. Handles keywords, identifiers (unquoted/quoted), literals (string/int/float/null/bool), operators, punctuation, placeholders, comments, and whitespace.
- **Tests:** Tokenize simple SELECT, WHERE, JOIN queries. Verify token types, values, and positions. Edge cases: nested quotes, escaped characters, multi-character operators, comments.
- **Files:**
  - `src/Query/Tokenizer/TokenType.php`
  - `src/Query/Tokenizer/Token.php`
  - `src/Query/Tokenizer/Tokenizer.php`
  - `tests/Query/Tokenizer/TokenizerTest.php`

### Phase 2: AST Node Hierarchy
- **Status:** [ ] Pending
- **What:** Define typed readonly classes for the AST. `Expr` interface for all expressions. Node types: `SelectStatement`, `ColumnRef`, `Literal`, `BinaryExpr`, `UnaryExpr`, `FunctionCall`, `InExpr`, `BetweenExpr`, `LikeExpr`, `IsNullExpr`, `CaseExpr`, `SubqueryExpr`, `Star`, `Placeholder`, `CastExpr`, `AliasedExpr`, `FromClause`, `TableRef`, `WhereClause`, `JoinClause`, `OrderByClause`, `OrderByItem`, `GroupByClause`, `HavingClause`, `LimitClause`, `OffsetClause`.
- **Tests:** Construct each node type, verify properties, verify immutability.
- **Files:**
  - `src/Query/AST/Expr.php` (interface)
  - `src/Query/AST/Node/*.php` (one per node type)
  - `src/Query/AST/SelectStatement.php`
  - `tests/Query/AST/NodeTest.php`
- **Depends on:** Phase 1 (for context, not code dependency)

### Phase 3: Base SQL Parser (Tokens → AST)
- **Status:** [ ] Pending
- **What:** A recursive-descent parser that converts a token stream into `SelectStatement` AST. Handles: column lists, FROM clause, WHERE expressions, JOINs, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET. Expression parsing with operator precedence for WHERE/HAVING conditions (AND/OR/NOT, comparisons, arithmetic, function calls, IN, BETWEEN, LIKE, IS NULL, CASE).
- **Tests:** Parse basic SELECT, SELECT with WHERE, SELECT with JOINs, SELECT with aggregations and GROUP BY/HAVING, SELECT with subqueries, complex nested expressions, operator precedence.
- **Files:**
  - `src/Query/AST/Parser.php`
  - `tests/Query/AST/ParserTest.php`
- **Depends on:** Phase 1, Phase 2

### Phase 4: Base SQL Serializer (AST → SQL)
- **Status:** [ ] Pending
- **What:** A serializer that converts AST nodes back to a SQL string. Handles proper quoting, parenthesization, and formatting. Produces parameterized output (preserving placeholders).
- **Tests:** Round-trip tests: parse SQL → AST → serialize → compare to normalized original. Test all clause types, expression types, and edge cases.
- **Files:**
  - `src/Query/AST/Serializer.php`
  - `tests/Query/AST/SerializerTest.php`
- **Depends on:** Phase 2, Phase 3

### Phase 5: Dialect-Specific Tokenizers & Serializers
- **Status:** [ ] Pending
- **What:** Dialect-specific subclasses for MySQL (backtick quoting, MySQL keywords/functions, hints), PostgreSQL (double-quote quoting, `::` cast, `@>` JSONB operators, `<=>/<->/<#>` vector ops), ClickHouse (backtick quoting, ClickHouse functions like `countIf`, PREWHERE, FINAL, SAMPLE, SETTINGS), SQLite (minimal overrides), MariaDB (extends MySQL).
- **Tests:** Parse and round-trip dialect-specific SQL for each dialect. Verify correct quoting and operator handling.
- **Files:**
  - `src/Query/Tokenizer/MySQL.php`
  - `src/Query/Tokenizer/PostgreSQL.php`
  - `src/Query/Tokenizer/ClickHouse.php`
  - `src/Query/Tokenizer/SQLite.php`
  - `src/Query/Tokenizer/MariaDB.php`
  - `src/Query/AST/Serializer/MySQL.php`
  - `src/Query/AST/Serializer/PostgreSQL.php`
  - `src/Query/AST/Serializer/ClickHouse.php`
  - `src/Query/AST/Serializer/SQLite.php`
  - `src/Query/AST/Serializer/MariaDB.php`
  - `tests/Query/Tokenizer/MySQLTest.php`
  - `tests/Query/Tokenizer/PostgreSQLTest.php`
  - `tests/Query/Tokenizer/ClickHouseTest.php`
  - `tests/Query/AST/Serializer/MySQLTest.php`
  - `tests/Query/AST/Serializer/PostgreSQLTest.php`
  - `tests/Query/AST/Serializer/ClickHouseTest.php`
- **Depends on:** Phase 1, Phase 4

### Phase 6: Visitor Pattern for AST Modification & Validation
- **Status:** [ ] Pending
- **What:** A `Visitor` interface with `visit(Expr $node): Expr` method for transforming AST nodes. A `Walker` that traverses the AST and applies visitors. Built-in visitors: `TableRenamer` (rename tables), `ColumnValidator` (validate column names against allow-list), `FilterInjector` (inject WHERE conditions like tenant filters).
- **Tests:** Apply each visitor to AST nodes, verify transformations. Test composition of multiple visitors. Test validation errors.
- **Files:**
  - `src/Query/AST/Visitor.php` (interface)
  - `src/Query/AST/Walker.php`
  - `src/Query/AST/Visitor/TableRenamer.php`
  - `src/Query/AST/Visitor/ColumnValidator.php`
  - `src/Query/AST/Visitor/FilterInjector.php`
  - `tests/Query/AST/VisitorTest.php`
- **Depends on:** Phase 2

### Phase 7: Builder ↔ AST Integration
- **Status:** [ ] Pending
- **What:** Add `toAst(): SelectStatement` method to the base `Builder` class and `fromAst(SelectStatement $ast): static` factory method. Each dialect builder serializes its state to AST nodes and can reconstruct from AST. Enables: parse SQL → AST → Builder → modify with fluent API → build().
- **Tests:** Builder → AST → serialize matches Builder → build(). Parse SQL → AST → Builder → build() produces equivalent SQL. Round-trip for each dialect.
- **Files:**
  - Modified: `src/Query/Builder.php`
  - Modified: `src/Query/Builder/SQL.php`
  - Modified: `src/Query/Builder/MySQL.php`
  - Modified: `src/Query/Builder/PostgreSQL.php`
  - Modified: `src/Query/Builder/ClickHouse.php`
  - Modified: `src/Query/Builder/SQLite.php`
  - Modified: `src/Query/Builder/MariaDB.php`
  - `tests/Query/AST/BuilderIntegrationTest.php`
- **Depends on:** Phase 3, Phase 4, Phase 5, Phase 6

## Progress Log

<!-- Updated as phases complete -->
