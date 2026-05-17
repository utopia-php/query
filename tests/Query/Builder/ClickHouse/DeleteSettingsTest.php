<?php

namespace Tests\Query\Builder\ClickHouse;

use PHPUnit\Framework\TestCase;
use Tests\Query\AssertsBindingCount;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Query;

class DeleteSettingsTest extends TestCase
{
    use AssertsBindingCount;

    public function testDeleteWithoutSettingsEmitsAlterTableDelete(): void
    {
        $result = (new Builder())
            ->from('audit_log')
            ->filter([Query::lessThan('time', '2024-01-01 00:00:00')])
            ->delete();

        $this->assertBindingCount($result);
        $this->assertSame(
            'ALTER TABLE `audit_log` DELETE WHERE `time` < ?',
            $result->query
        );
        $this->assertSame(['2024-01-01 00:00:00'], $result->bindings);
    }

    public function testDeleteWithAsyncCleanupSetting(): void
    {
        $result = (new Builder())
            ->from('audit_log')
            ->settings(['lightweight_deletes_sync' => '0'])
            ->filter([Query::lessThan('time', '2024-01-01 00:00:00')])
            ->delete();

        $this->assertBindingCount($result);
        $this->assertSame(
            'ALTER TABLE `audit_log` DELETE WHERE `time` < ? SETTINGS lightweight_deletes_sync=0',
            $result->query
        );
        $this->assertSame(['2024-01-01 00:00:00'], $result->bindings);
    }

    public function testDeleteWithMultipleSettings(): void
    {
        $result = (new Builder())
            ->from('audit_log')
            ->settings([
                'lightweight_deletes_sync' => '0',
                'mutations_sync' => '0',
            ])
            ->filter([Query::lessThan('time', '2024-01-01 00:00:00')])
            ->delete();

        $this->assertSame(
            'ALTER TABLE `audit_log` DELETE WHERE `time` < ? SETTINGS lightweight_deletes_sync=0, mutations_sync=0',
            $result->query
        );
    }

    public function testDeleteWithHintAlsoEmitsSettings(): void
    {
        $result = (new Builder())
            ->from('audit_log')
            ->hint('lightweight_deletes_sync=0')
            ->filter([Query::lessThan('time', '2024-01-01 00:00:00')])
            ->delete();

        $this->assertSame(
            'ALTER TABLE `audit_log` DELETE WHERE `time` < ? SETTINGS lightweight_deletes_sync=0',
            $result->query
        );
    }

    public function testDeleteWithCompoundWhereAndSettings(): void
    {
        $result = (new Builder())
            ->from('audit_log')
            ->settings(['lightweight_deletes_sync' => '0'])
            ->filter([
                Query::lessThan('time', '2024-01-01 00:00:00'),
                Query::equal('tenant', ['acme']),
            ])
            ->delete();

        $this->assertSame(
            'ALTER TABLE `audit_log` DELETE WHERE `time` < ? AND `tenant` IN (?) SETTINGS lightweight_deletes_sync=0',
            $result->query
        );
        $this->assertSame(['2024-01-01 00:00:00', 'acme'], $result->bindings);
    }

    public function testSelectStillEmitsSettingsAfterDeleteFeatureAdded(): void
    {
        $result = (new Builder())
            ->from('events')
            ->settings(['max_threads' => '4'])
            ->build();

        $this->assertSame(
            'SELECT * FROM `events` SETTINGS max_threads=4',
            $result->query
        );
    }
}
