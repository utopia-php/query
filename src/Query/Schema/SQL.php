<?php

namespace Utopia\Query\Schema;

use Utopia\Query\Builder\Plan;
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
    ): Plan {
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

        return new Plan($sql, [], executor: $this->executor);
    }

    public function dropForeignKey(string $table, string $name): Plan
    {
        return new Plan(
            'ALTER TABLE ' . $this->quote($table)
            . ' DROP FOREIGN KEY ' . $this->quote($name),
            [],
            executor: $this->executor,
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
     * Create a stored procedure.
     *
     * $body is emitted verbatim into the generated DDL and must come from
     * trusted (developer-controlled) source — never from untrusted input.
     *
     * @param  list<array{0: ParameterDirection, 1: string, 2: string}>  $params
     */
    public function createProcedure(string $name, array $params, string $body): Plan
    {
        $paramList = $this->compileProcedureParams($params);

        $sql = 'CREATE PROCEDURE ' . $this->quote($name)
            . '(' . \implode(', ', $paramList) . ')'
            . ' BEGIN ' . $body . ' END';

        return new Plan($sql, [], executor: $this->executor);
    }

    public function dropProcedure(string $name): Plan
    {
        return new Plan('DROP PROCEDURE ' . $this->quote($name), [], executor: $this->executor);
    }

    /**
     * Create a trigger.
     *
     * $body is emitted verbatim into the generated DDL and must come from
     * trusted (developer-controlled) source — never from untrusted input.
     */
    public function createTrigger(
        string $name,
        string $table,
        TriggerTiming $timing,
        TriggerEvent $event,
        string $body,
    ): Plan {
        $sql = 'CREATE TRIGGER ' . $this->quote($name)
            . ' ' . $timing->value . ' ' . $event->value
            . ' ON ' . $this->quote($table)
            . ' FOR EACH ROW BEGIN ' . $body . ' END';

        return new Plan($sql, [], executor: $this->executor);
    }

    public function dropTrigger(string $name): Plan
    {
        return new Plan('DROP TRIGGER ' . $this->quote($name), [], executor: $this->executor);
    }
}
