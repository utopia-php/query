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

    private const ALLOWED_FK_ACTIONS = ['CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION'];

    public function addForeignKey(
        string $table,
        string $name,
        string $column,
        string $refTable,
        string $refColumn,
        string $onDelete = '',
        string $onUpdate = '',
    ): BuildResult {
        if ($onDelete !== '') {
            $onDelete = \strtoupper($onDelete);
            if (! \in_array($onDelete, self::ALLOWED_FK_ACTIONS, true)) {
                throw new ValidationException('Invalid foreign key action: ' . $onDelete);
            }
        }
        if ($onUpdate !== '') {
            $onUpdate = \strtoupper($onUpdate);
            if (! \in_array($onUpdate, self::ALLOWED_FK_ACTIONS, true)) {
                throw new ValidationException('Invalid foreign key action: ' . $onUpdate);
            }
        }

        $sql = 'ALTER TABLE ' . $this->quote($table)
            . ' ADD CONSTRAINT ' . $this->quote($name)
            . ' FOREIGN KEY (' . $this->quote($column) . ')'
            . ' REFERENCES ' . $this->quote($refTable)
            . ' (' . $this->quote($refColumn) . ')';

        if ($onDelete !== '') {
            $sql .= ' ON DELETE ' . $onDelete;
        }
        if ($onUpdate !== '') {
            $sql .= ' ON UPDATE ' . $onUpdate;
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
     * @param  list<array{0: string, 1: string, 2: string}>  $params
     * @return list<string>
     */
    protected function compileProcedureParams(array $params): array
    {
        $paramList = [];
        foreach ($params as $param) {
            $direction = \strtoupper($param[0]);
            if (! \in_array($direction, ['IN', 'OUT', 'INOUT'], true)) {
                throw new ValidationException('Invalid procedure parameter direction: ' . $param[0]);
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
     * @param  list<array{0: string, 1: string, 2: string}>  $params
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
        string $timing,
        string $event,
        string $body,
    ): BuildResult {
        $timing = \strtoupper($timing);
        $event = \strtoupper($event);

        if (!\in_array($timing, ['BEFORE', 'AFTER', 'INSTEAD OF'], true)) {
            throw new \Utopia\Query\Exception\ValidationException('Invalid trigger timing: ' . $timing);
        }
        if (!\in_array($event, ['INSERT', 'UPDATE', 'DELETE'], true)) {
            throw new \Utopia\Query\Exception\ValidationException('Invalid trigger event: ' . $event);
        }

        $sql = 'CREATE TRIGGER ' . $this->quote($name)
            . ' ' . $timing . ' ' . $event
            . ' ON ' . $this->quote($table)
            . ' FOR EACH ROW BEGIN ' . $body . ' END';

        return new BuildResult($sql, []);
    }

    public function dropTrigger(string $name): BuildResult
    {
        return new BuildResult('DROP TRIGGER ' . $this->quote($name), []);
    }
}
