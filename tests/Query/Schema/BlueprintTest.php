<?php

namespace Tests\Query\Schema;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Schema\Blueprint;
use Utopia\Query\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKey;
use Utopia\Query\Schema\Index;
use Utopia\Query\Schema\RenameColumn;

class BlueprintTest extends TestCase
{
    // ── columns (public private(set)) ──────────────────────────

    public function testColumnsPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->columns);
    }

    public function testColumnsPropertyPopulatedByString(): void
    {
        $bp = new Blueprint();
        $col = $bp->string('name');

        $this->assertCount(1, $bp->columns);
        $this->assertSame($col, $bp->columns[0]);
        $this->assertSame('name', $bp->columns[0]->name);
        $this->assertSame(ColumnType::String, $bp->columns[0]->type);
    }

    public function testColumnsPropertyPopulatedByMultipleMethods(): void
    {
        $bp = new Blueprint();
        $bp->integer('age');
        $bp->boolean('active');
        $bp->text('bio');

        $this->assertCount(3, $bp->columns);
        $this->assertSame('age', $bp->columns[0]->name);
        $this->assertSame('active', $bp->columns[1]->name);
        $this->assertSame('bio', $bp->columns[2]->name);
    }

    public function testColumnsPropertyNotWritableExternally(): void
    {
        $bp = new Blueprint();

        $this->expectException(\Error::class);
        /** @phpstan-ignore-next-line */
        $bp->columns = [new Column('x', ColumnType::String)];
    }

    public function testColumnsPopulatedById(): void
    {
        $bp = new Blueprint();
        $bp->id('pk');

        $this->assertCount(1, $bp->columns);
        $this->assertSame('pk', $bp->columns[0]->name);
        $this->assertTrue($bp->columns[0]->isPrimary);
        $this->assertTrue($bp->columns[0]->isAutoIncrement);
        $this->assertTrue($bp->columns[0]->isUnsigned);
    }

    public function testColumnsPopulatedByAddColumn(): void
    {
        $bp = new Blueprint();
        $bp->addColumn('score', ColumnType::Integer);

        $this->assertCount(1, $bp->columns);
        $this->assertSame('score', $bp->columns[0]->name);
    }

    public function testColumnsPopulatedByModifyColumn(): void
    {
        $bp = new Blueprint();
        $bp->modifyColumn('score', 'integer');

        $this->assertCount(1, $bp->columns);
        $this->assertTrue($bp->columns[0]->isModify);
    }

    // ── indexes (public private(set)) ──────────────────────────

    public function testIndexesPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->indexes);
    }

    public function testIndexesPopulatedByIndex(): void
    {
        $bp = new Blueprint();
        $bp->index(['email']);

        $this->assertCount(1, $bp->indexes);
        $this->assertInstanceOf(Index::class, $bp->indexes[0]);
        $this->assertSame('idx_email', $bp->indexes[0]->name);
    }

    public function testIndexesPopulatedByUniqueIndex(): void
    {
        $bp = new Blueprint();
        $bp->uniqueIndex(['email']);

        $this->assertCount(1, $bp->indexes);
        $this->assertSame('uniq_email', $bp->indexes[0]->name);
    }

    public function testIndexesPopulatedByFulltextIndex(): void
    {
        $bp = new Blueprint();
        $bp->fulltextIndex(['body']);

        $this->assertCount(1, $bp->indexes);
        $this->assertSame('ft_body', $bp->indexes[0]->name);
    }

    public function testIndexesPopulatedBySpatialIndex(): void
    {
        $bp = new Blueprint();
        $bp->spatialIndex(['location']);

        $this->assertCount(1, $bp->indexes);
        $this->assertSame('sp_location', $bp->indexes[0]->name);
    }

    public function testIndexesPopulatedByAddIndex(): void
    {
        $bp = new Blueprint();
        $bp->addIndex('my_idx', ['col1', 'col2']);

        $this->assertCount(1, $bp->indexes);
        $this->assertSame('my_idx', $bp->indexes[0]->name);
        $this->assertSame(['col1', 'col2'], $bp->indexes[0]->columns);
    }

    public function testIndexesPropertyNotWritableExternally(): void
    {
        $bp = new Blueprint();

        $this->expectException(\Error::class);
        /** @phpstan-ignore-next-line */
        $bp->indexes = [];
    }

    // ── foreignKeys (public private(set)) ──────────────────────

    public function testForeignKeysPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->foreignKeys);
    }

    public function testForeignKeysPopulatedByForeignKey(): void
    {
        $bp = new Blueprint();
        $bp->foreignKey('user_id')->references('id')->on('users');

        $this->assertCount(1, $bp->foreignKeys);
        $this->assertInstanceOf(ForeignKey::class, $bp->foreignKeys[0]);
        $this->assertSame('user_id', $bp->foreignKeys[0]->column);
    }

    public function testForeignKeysPopulatedByAddForeignKey(): void
    {
        $bp = new Blueprint();
        $bp->addForeignKey('order_id')->references('id')->on('orders');

        $this->assertCount(1, $bp->foreignKeys);
        $this->assertSame('order_id', $bp->foreignKeys[0]->column);
    }

    public function testForeignKeysPropertyNotWritableExternally(): void
    {
        $bp = new Blueprint();

        $this->expectException(\Error::class);
        /** @phpstan-ignore-next-line */
        $bp->foreignKeys = [];
    }

    // ── dropColumns (public private(set)) ──────────────────────

    public function testDropColumnsPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->dropColumns);
    }

    public function testDropColumnsPopulatedByDropColumn(): void
    {
        $bp = new Blueprint();
        $bp->dropColumn('old_field');

        $this->assertCount(1, $bp->dropColumns);
        $this->assertSame('old_field', $bp->dropColumns[0]);
    }

    public function testDropColumnsMultiple(): void
    {
        $bp = new Blueprint();
        $bp->dropColumn('a');
        $bp->dropColumn('b');
        $bp->dropColumn('c');

        $this->assertCount(3, $bp->dropColumns);
        $this->assertSame(['a', 'b', 'c'], $bp->dropColumns);
    }

    // ── renameColumns (public private(set)) ────────────────────

    public function testRenameColumnsPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->renameColumns);
    }

    public function testRenameColumnsPopulatedByRenameColumn(): void
    {
        $bp = new Blueprint();
        $bp->renameColumn('old', 'new');

        $this->assertCount(1, $bp->renameColumns);
        $this->assertInstanceOf(RenameColumn::class, $bp->renameColumns[0]);
        $this->assertSame('old', $bp->renameColumns[0]->from);
        $this->assertSame('new', $bp->renameColumns[0]->to);
    }

    // ── dropIndexes (public private(set)) ──────────────────────

    public function testDropIndexesPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->dropIndexes);
    }

    public function testDropIndexesPopulatedByDropIndex(): void
    {
        $bp = new Blueprint();
        $bp->dropIndex('idx_old');

        $this->assertCount(1, $bp->dropIndexes);
        $this->assertSame('idx_old', $bp->dropIndexes[0]);
    }

    // ── dropForeignKeys (public private(set)) ──────────────────

    public function testDropForeignKeysPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->dropForeignKeys);
    }

    public function testDropForeignKeysPopulatedByDropForeignKey(): void
    {
        $bp = new Blueprint();
        $bp->dropForeignKey('fk_user');

        $this->assertCount(1, $bp->dropForeignKeys);
        $this->assertSame('fk_user', $bp->dropForeignKeys[0]);
    }

    // ── rawColumnDefs (public private(set)) ────────────────────

    public function testRawColumnDefsPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->rawColumnDefs);
    }

    public function testRawColumnDefsPopulatedByRawColumn(): void
    {
        $bp = new Blueprint();
        $bp->rawColumn('`my_col` VARCHAR(100) NOT NULL');

        $this->assertCount(1, $bp->rawColumnDefs);
        $this->assertSame('`my_col` VARCHAR(100) NOT NULL', $bp->rawColumnDefs[0]);
    }

    // ── rawIndexDefs (public private(set)) ─────────────────────

    public function testRawIndexDefsPropertyIsReadable(): void
    {
        $bp = new Blueprint();
        $this->assertSame([], $bp->rawIndexDefs);
    }

    public function testRawIndexDefsPopulatedByRawIndex(): void
    {
        $bp = new Blueprint();
        $bp->rawIndex('INDEX `idx_custom` (`col1`)');

        $this->assertCount(1, $bp->rawIndexDefs);
        $this->assertSame('INDEX `idx_custom` (`col1`)', $bp->rawIndexDefs[0]);
    }

    // ── Combined / integration ─────────────────────────────────

    public function testAllPropertiesStartEmpty(): void
    {
        $bp = new Blueprint();

        $this->assertSame([], $bp->columns);
        $this->assertSame([], $bp->indexes);
        $this->assertSame([], $bp->foreignKeys);
        $this->assertSame([], $bp->dropColumns);
        $this->assertSame([], $bp->renameColumns);
        $this->assertSame([], $bp->dropIndexes);
        $this->assertSame([], $bp->dropForeignKeys);
        $this->assertSame([], $bp->rawColumnDefs);
        $this->assertSame([], $bp->rawIndexDefs);
    }

    public function testMultiplePropertiesPopulatedTogether(): void
    {
        $bp = new Blueprint();
        $bp->string('name');
        $bp->integer('age');
        $bp->index(['name']);
        $bp->foreignKey('team_id')->references('id')->on('teams');
        $bp->rawColumn('`extra` TEXT');
        $bp->rawIndex('INDEX `idx_extra` (`extra`)');

        $this->assertCount(2, $bp->columns);
        $this->assertCount(1, $bp->indexes);
        $this->assertCount(1, $bp->foreignKeys);
        $this->assertCount(1, $bp->rawColumnDefs);
        $this->assertCount(1, $bp->rawIndexDefs);
    }

    public function testAlterOperationsPopulateCorrectProperties(): void
    {
        $bp = new Blueprint();
        $bp->modifyColumn('score', ColumnType::BigInteger);
        $bp->renameColumn('old_name', 'new_name');
        $bp->dropColumn('obsolete');
        $bp->dropIndex('idx_dead');
        $bp->dropForeignKey('fk_dead');

        $this->assertCount(1, $bp->columns);
        $this->assertTrue($bp->columns[0]->isModify);
        $this->assertCount(1, $bp->renameColumns);
        $this->assertCount(1, $bp->dropColumns);
        $this->assertCount(1, $bp->dropIndexes);
        $this->assertCount(1, $bp->dropForeignKeys);
    }

    public function testColumnTypeVariants(): void
    {
        $bp = new Blueprint();
        $bp->text('body');
        $bp->mediumText('summary');
        $bp->longText('content');
        $bp->bigInteger('count');
        $bp->float('price');
        $bp->boolean('active');
        $bp->datetime('created_at', 3);
        $bp->timestamp('updated_at', 6);
        $bp->json('meta');
        $bp->binary('data');
        $bp->enum('status', ['draft', 'published']);
        $bp->point('location');
        $bp->linestring('route');
        $bp->polygon('area');
        $bp->vector('embedding', 768);

        $this->assertCount(15, $bp->columns);
        $this->assertSame(ColumnType::Text, $bp->columns[0]->type);
        $this->assertSame(ColumnType::MediumText, $bp->columns[1]->type);
        $this->assertSame(ColumnType::LongText, $bp->columns[2]->type);
        $this->assertSame(ColumnType::BigInteger, $bp->columns[3]->type);
        $this->assertSame(ColumnType::Float, $bp->columns[4]->type);
        $this->assertSame(ColumnType::Boolean, $bp->columns[5]->type);
        $this->assertSame(ColumnType::Datetime, $bp->columns[6]->type);
        $this->assertSame(ColumnType::Timestamp, $bp->columns[7]->type);
        $this->assertSame(ColumnType::Json, $bp->columns[8]->type);
        $this->assertSame(ColumnType::Binary, $bp->columns[9]->type);
        $this->assertSame(ColumnType::Enum, $bp->columns[10]->type);
        $this->assertSame(ColumnType::Point, $bp->columns[11]->type);
        $this->assertSame(ColumnType::Linestring, $bp->columns[12]->type);
        $this->assertSame(ColumnType::Polygon, $bp->columns[13]->type);
        $this->assertSame(ColumnType::Vector, $bp->columns[14]->type);
    }

    public function testTimestampsHelperAddsTwoColumns(): void
    {
        $bp = new Blueprint();
        $bp->timestamps(6);

        $this->assertCount(2, $bp->columns);
        $this->assertSame('created_at', $bp->columns[0]->name);
        $this->assertSame('updated_at', $bp->columns[1]->name);
        $this->assertSame(ColumnType::Datetime, $bp->columns[0]->type);
        $this->assertSame(ColumnType::Datetime, $bp->columns[1]->type);
    }
}
