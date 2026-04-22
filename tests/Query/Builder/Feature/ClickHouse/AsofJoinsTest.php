<?php

namespace Tests\Query\Builder\Feature\ClickHouse;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Builder\ClickHouse as Builder;
use Utopia\Query\Query;

class AsofJoinsTest extends TestCase
{
    public function testAsofJoinEmitsAsofJoinWithQualifiedColumns(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofJoin('quotes', 'trades.timestamp', 'quotes.timestamp')
            ->build();

        $this->assertStringContainsString(
            'ASOF JOIN `quotes` ON `trades`.`timestamp` = `quotes`.`timestamp`',
            $result->query,
        );
    }

    public function testAsofJoinWithAliasUsesAliasInOnClause(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofJoin('quotes', 'trades.timestamp', 'q.timestamp', 'q')
            ->build();

        $this->assertStringContainsString(
            'ASOF JOIN `quotes` AS `q` ON `trades`.`timestamp` = `q`.`timestamp`',
            $result->query,
        );
    }

    public function testAsofLeftJoinEmitsAsofLeftJoin(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofLeftJoin('quotes', 'trades.timestamp', 'quotes.timestamp')
            ->build();

        $this->assertStringContainsString('ASOF LEFT JOIN `quotes`', $result->query);
    }

    public function testAsofJoinWithEmptyAliasSkipsAsClause(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofJoin('quotes', 'trades.timestamp', 'quotes.timestamp', '')
            ->build();

        $this->assertStringNotContainsString('AS ``', $result->query);
    }

    public function testAsofJoinPrecedesWhereClause(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofJoin('quotes', 'trades.timestamp', 'quotes.timestamp')
            ->filter([Query::equal('trades.symbol', ['AAPL'])])
            ->build();

        $this->assertLessThan(\strpos($result->query, 'WHERE'), \strpos($result->query, 'ASOF JOIN'));
        $this->assertSame(['AAPL'], $result->bindings);
    }

    public function testAsofJoinDoesNotAddBindings(): void
    {
        $result = (new Builder())
            ->from('trades')
            ->asofJoin('quotes', 'trades.timestamp', 'quotes.timestamp')
            ->build();

        $this->assertSame([], $result->bindings);
    }
}
