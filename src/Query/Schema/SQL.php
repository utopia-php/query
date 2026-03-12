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
        ?ForeignKeyAction $onDelete = null,
        ?ForeignKeyAction $onUpdate = null,
    ): BuildResult {
        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ADD CONSTRAINT ' . $this->quote($name)
            . ' FOREIGN KEY (' . $this->quote($column) . ')'
            . ' REFERENCES ' . $this->quote($refTable)
            . ' (' . $this->quote($refColumn) . ')';

        if ($onDelete !== null) {
            $sql .= ' ON DELETE ' . $onDelete->toSql();
        }
        if ($onUpdate !== null) {
            $sql .= ' ON UPDATE ' . $onUpdate->toSql();
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
     * @param  list<array{0: ParameterDirection, 1: string, 2: string}>  $params
     * @return list<string>
     */
    protected function compileProcedureParams(array $params): array
    {
        $paramList = [];
        foreach ($params as $param) {
            $direction = $param[0]->value;
            $name = $this->quote($param[1]);

            if (! \preg_match('/^[A-Za-z0-9_() ,]+$/', $param[2])) {
                throw new ValidationException('Invalid procedure parameter type: ' . $param[2]);
            }

            $paramList[] = $direction . ' ' . $name . ' ' . $param[2];
        }

        return $paramList;
    }

    /**
     * @param  list<array{0: ParameterDirection, 1: string, 2: string}>  $params
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
        TriggerTiming $timing,
        TriggerEvent $event,
        string $body,
    ): BuildResult {
        $sql = 'CREATE TRIGGER ' . $this->quote($name)
            . ' ' . $timing->value . ' ' . $event->value
            . ' ON ' . $this->quote($table)
            . ' FOR EACH ROW BEGIN ' . $body . ' END';

        return new BuildResult($sql, []);
    }

    public function dropTrigger(string $name): BuildResult
    {
        return new BuildResult('DROP TRIGGER ' . $this->quote($name), []);
    }
}
