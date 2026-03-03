<?php

namespace Tests\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Query\Query;

class VectorQueryTest extends TestCase
{
    public function testVectorDot(): void
    {
        $vector = [0.1, 0.2, 0.3];
        $query = Query::vectorDot('embedding', $vector);
        $this->assertEquals(Query::TYPE_VECTOR_DOT, $query->getMethod());
        $this->assertEquals([$vector], $query->getValues());
    }

    public function testVectorCosine(): void
    {
        $vector = [0.1, 0.2, 0.3];
        $query = Query::vectorCosine('embedding', $vector);
        $this->assertEquals(Query::TYPE_VECTOR_COSINE, $query->getMethod());
    }

    public function testVectorEuclidean(): void
    {
        $vector = [0.1, 0.2, 0.3];
        $query = Query::vectorEuclidean('embedding', $vector);
        $this->assertEquals(Query::TYPE_VECTOR_EUCLIDEAN, $query->getMethod());
    }
}
