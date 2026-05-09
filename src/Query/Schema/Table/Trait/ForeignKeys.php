<?php

namespace Utopia\Query\Schema\Table\Trait;

use Utopia\Query\Schema\ForeignKey;

trait ForeignKeys
{
    /**
     * Declare a foreign key. The behaviour is identical for create and alter
     * contexts — the dialect compiler switches between `FOREIGN KEY (...)` (in
     * a CREATE TABLE column list) and `ADD FOREIGN KEY (...)` (in an ALTER
     * TABLE clause) when emitting the statement. {@see addForeignKey()} is
     * an alias for use in alter chains; both register the same FK exactly once.
     */
    public function foreignKey(string $column): ForeignKey
    {
        $fk = $this->newForeignKey($column);
        $this->foreignKeys[] = $fk;

        return $fk;
    }

    /**
     * Alias of {@see foreignKey()}, for symmetry with the other `add*`/`drop*`
     * alter helpers. Returns the same registered {@see ForeignKey}; calling
     * both methods for the same column registers the FK twice.
     */
    public function addForeignKey(string $column): ForeignKey
    {
        return $this->foreignKey($column);
    }

    public function dropForeignKey(string $name): static
    {
        $this->dropForeignKeys[] = $name;

        return $this;
    }
}
