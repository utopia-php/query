<?php

namespace Utopia\Query;

/**
 * Wire Protocol Query Parser
 *
 * Classifies database wire protocol messages as Read, Write, Transaction, or Unknown
 * to enable routing queries to appropriate primary/replica backends.
 */
interface Parser
{
    /**
     * Parse a raw wire protocol message and classify the query
     *
     * @param  string  $data  Raw protocol message bytes
     * @return Type Classification result
     */
    public function parse(string $data): Type;
}
