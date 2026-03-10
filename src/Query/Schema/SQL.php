<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\BuildResult;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\QuotesIdentifiers;
use Utopia\Query\Schema;
use Utopia\Query\Schema\Feature\ForeignKeys;
use Utopia\Query\Schema\Feature\Procedures;
use Utopia\Query\Schema\Feature\Triggers;

abstract class SQL extends Schema implements ForeignKeys, Procedures, Triggers
{
    use QuotesIdentifiers;

    public function addForeignKey(
        string $table,
        string $name,
        string $column,
        string $refTable,
        string $refColumn,
        ForeignKeyAction|string $onDelete = '',
        ForeignKeyAction|string $onUpdate = '',
    ): BuildResult {
        $onDeleteAction = $this->resolveForeignKeyAction($onDelete);
        $onUpdateAction = $this->resolveForeignKeyAction($onUpdate);

        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ADD CONSTRAINT ' . $this->quote($name)
            . ' FOREIGN KEY (' . $this->quote($column) . ')'
            . ' REFERENCES ' . $this->quote($refTable)
            . ' (' . $this->quote($refColumn) . ')';

        if ($onDeleteAction !== null) {
            $sql .= ' ON DELETE ' . $onDeleteAction->value;
        }
        if ($onUpdateAction !== null) {
            $sql .= ' ON UPDATE ' . $onUpdateAction->value;
        }

        return new BuildResult($sql, []);
    }

    public function dropForeignKey(string $table, string $name): BuildResult
    {
        return new BuildResult(
            'ALTER TABLE ' . $this->quote($table)
            . ' DROP FOREIGN KEY ' . $this->quote($name),
            []
        );
    }

    /**
     * Validate and compile a procedure parameter list.
     *
     * @param  list<array{0: ParameterDirection|string, 1: string, 2: string}>  $params
     * @return list<string>
     */
    protected function compileProcedureParams(array $params): array
    {
        $paramList = [];
        foreach ($params as $param) {
            if ($param[0] instanceof ParameterDirection) {
                $direction = $param[0]->value;
            } else {
                $direction = \strtoupper($param[0]);
                ParameterDirection::from($direction);
            }

            $name = $this->quote($param[1]);

            if (! \preg_match('/^[A-Za-z0-9_() ,]+$/', $param[2])) {
                throw new ValidationException('Invalid procedure parameter type: ' . $param[2]);
            }

            $paramList[] = $direction . ' ' . $name . ' ' . $param[2];
        }

        return $paramList;
    }

    /**
     * @param  list<array{0: ParameterDirection|string, 1: string, 2: string}>  $params
     */
    public function createProcedure(string $name, array $params, string $body): BuildResult
    {
        $paramList = $this->compileProcedureParams($params);

        $sql = 'CREATE PROCEDURE ' . $this->quote($name)
            . '(' . \implode(', ', $paramList) . ')'
            . ' BEGIN ' . $body . ' END';

        return new BuildResult($sql, []);
    }

    public function dropProcedure(string $name): BuildResult
    {
        return new BuildResult('DROP PROCEDURE ' . $this->quote($name), []);
    }

    public function createTrigger(
        string $name,
        string $table,
        TriggerTiming|string $timing,
        TriggerEvent|string $event,
        string $body,
    ): BuildResult {
        if ($timing instanceof TriggerTiming) {
            $timingValue = $timing->value;
        } else {
            $timingValue = \strtoupper($timing);
            TriggerTiming::from($timingValue);
        }

        if ($event instanceof TriggerEvent) {
            $eventValue = $event->value;
        } else {
            $eventValue = \strtoupper($event);
            TriggerEvent::from($eventValue);
        }

        $sql = 'CREATE TRIGGER ' . $this->quote($name)
            . ' ' . $timingValue . ' ' . $eventValue
            . ' ON ' . $this->quote($table)
            . ' FOR EACH ROW BEGIN ' . $body . ' END';

        return new BuildResult($sql, []);
    }

    public function dropTrigger(string $name): BuildResult
    {
        return new BuildResult('DROP TRIGGER ' . $this->quote($name), []);
    }

    private function resolveForeignKeyAction(ForeignKeyAction|string $action): ?ForeignKeyAction
    {
        if ($action instanceof ForeignKeyAction) {
            return $action;
        }

        if ($action === '') {
            return null;
        }

        return ForeignKeyAction::from(\strtoupper($action));
    }
}
