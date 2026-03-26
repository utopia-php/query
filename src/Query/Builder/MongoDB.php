<?php

namespace Utopia\Query\Builder;

use Utopia\Query\Builder as BaseBuilder;
use Utopia\Query\Builder\Feature\FullTextSearch;
use Utopia\Query\Builder\Feature\MongoDB\ArrayPushModifiers;
use Utopia\Query\Builder\Feature\MongoDB\AtlasSearch;
use Utopia\Query\Builder\Feature\MongoDB\ConditionalArrayUpdates;
use Utopia\Query\Builder\Feature\MongoDB\FieldUpdates;
use Utopia\Query\Builder\Feature\MongoDB\PipelineStages;
use Utopia\Query\Builder\Feature\TableSampling;
use Utopia\Query\Builder\Feature\Upsert;
use Utopia\Query\Exception\UnsupportedException;
use Utopia\Query\Exception\ValidationException;
use Utopia\Query\Method;
use Utopia\Query\Query;

class MongoDB extends BaseBuilder implements
    Upsert,
    FullTextSearch,
    TableSampling,
    FieldUpdates,
    ArrayPushModifiers,
    ConditionalArrayUpdates,
    PipelineStages,
    AtlasSearch
{
    /** @var array<string, mixed> */
    protected array $pushOps = [];

    /** @var array<string, mixed> */
    protected array $pullOps = [];

    /** @var array<string, mixed> */
    protected array $addToSetOps = [];

    /** @var array<string, int|float> */
    protected array $incOps = [];

    /** @var list<string> */
    protected array $unsetOps = [];

    protected ?string $textSearchTerm = null;

    protected ?float $sampleSize = null;

    /** @var array<string, string> */
    protected array $renameOps = [];

    /** @var array<string, int|float> */
    protected array $mulOps = [];

    /** @var array<string, int> */
    protected array $popOps = [];

    /** @var array<string, array<mixed>> */
    protected array $pullAllOps = [];

    /** @var array<string, mixed> */
    protected array $minOps = [];

    /** @var array<string, mixed> */
    protected array $maxOps = [];

    /** @var array<string, array<string, string>> */
    protected array $currentDateOps = [];

    /** @var array<string, array<string, mixed>> */
    protected array $pushEachOps = [];

    /** @var list<array<string, mixed>> */
    protected array $arrayFilters = [];

    /** @var array<string, mixed>|null */
    protected ?array $bucketStage = null;

    /** @var array<string, mixed>|null */
    protected ?array $bucketAutoStage = null;

    /** @var array<string, array<string, mixed>>|null */
    protected ?array $facetStages = null;

    /** @var array<string, mixed>|null */
    protected ?array $graphLookupStage = null;

    /** @var array<string, mixed>|null */
    protected ?array $mergeStage = null;

    /** @var array<string, mixed>|null */
    protected ?array $outStage = null;

    protected ?string $replaceRootExpr = null;

    /** @var array<string, mixed>|null */
    protected ?array $searchStage = null;

    /** @var array<string, mixed>|null */
    protected ?array $searchMetaStage = null;

    /** @var array<string, mixed>|null */
    protected ?array $vectorSearchStage = null;

    /** @var string|array<string, int>|null */
    protected string|array|null $indexHint = null;

    protected function quote(string $identifier): string
    {
        return $identifier;
    }

    protected function compileRandom(): string
    {
        return '$rand';
    }

    /**
     * @param array<mixed> $values
     */
    protected function compileRegex(string $attribute, array $values): string
    {
        $this->addBinding($values[0]);

        return $attribute . ' REGEX ?';
    }

    public function push(string $field, mixed $value): static
    {
        $this->pushOps[$field] = $value;

        return $this;
    }

    public function pull(string $field, mixed $value): static
    {
        $this->pullOps[$field] = $value;

        return $this;
    }

    public function addToSet(string $field, mixed $value): static
    {
        $this->addToSetOps[$field] = $value;

        return $this;
    }

    public function increment(string $field, int|float $amount = 1): static
    {
        $this->incOps[$field] = $amount;

        return $this;
    }

    public function unsetFields(string ...$fields): static
    {
        foreach ($fields as $field) {
            $this->unsetOps[] = $field;
        }

        return $this;
    }

    public function filterSearch(string $attribute, string $value): static
    {
        $this->textSearchTerm = $value;

        return $this;
    }

    public function filterNotSearch(string $attribute, string $value): static
    {
        throw new UnsupportedException('MongoDB does not support negated full-text search.');
    }

    public function tablesample(float $percent, string $method = 'BERNOULLI'): static
    {
        $this->sampleSize = $percent;

        return $this;
    }

    public function rename(string $oldField, string $newField): static
    {
        $this->renameOps[$oldField] = $newField;

        return $this;
    }

    public function multiply(string $field, int|float $factor): static
    {
        $this->mulOps[$field] = $factor;

        return $this;
    }

    public function popFirst(string $field): static
    {
        $this->popOps[$field] = -1;

        return $this;
    }

    public function popLast(string $field): static
    {
        $this->popOps[$field] = 1;

        return $this;
    }

    public function pullAll(string $field, array $values): static
    {
        $this->pullAllOps[$field] = $values;

        return $this;
    }

    public function updateMin(string $field, mixed $value): static
    {
        $this->minOps[$field] = $value;

        return $this;
    }

    public function updateMax(string $field, mixed $value): static
    {
        $this->maxOps[$field] = $value;

        return $this;
    }

    public function currentDate(string $field, string $type = 'date'): static
    {
        $this->currentDateOps[$field] = ['$type' => $type];

        return $this;
    }

    public function pushEach(string $field, array $values, ?int $position = null, ?int $slice = null, ?array $sort = null): static
    {
        $modifier = ['values' => $values];
        if ($position !== null) {
            $modifier['position'] = $position;
        }
        if ($slice !== null) {
            $modifier['slice'] = $slice;
        }
        if ($sort !== null) {
            $modifier['sort'] = $sort;
        }
        $this->pushEachOps[$field] = $modifier;

        return $this;
    }

    public function arrayFilter(string $identifier, array $condition): static
    {
        $this->arrayFilters[] = [$identifier => $condition];

        return $this;
    }

    public function bucket(string $groupBy, array $boundaries, ?string $defaultBucket = null, array $output = []): static
    {
        $stage = [
            'groupBy' => '$' . $groupBy,
            'boundaries' => $boundaries,
        ];
        if ($defaultBucket !== null) {
            $stage['default'] = $defaultBucket;
        }
        if (! empty($output)) {
            $stage['output'] = $output;
        }
        $this->bucketStage = $stage;

        return $this;
    }

    public function bucketAuto(string $groupBy, int $buckets, array $output = []): static
    {
        $stage = [
            'groupBy' => '$' . $groupBy,
            'buckets' => $buckets,
        ];
        if (! empty($output)) {
            $stage['output'] = $output;
        }
        $this->bucketAutoStage = $stage;

        return $this;
    }

    public function facet(array $facets): static
    {
        $this->facetStages = [];
        foreach ($facets as $name => $builder) {
            $result = $builder->build();
            /** @var array<string, mixed>|null $subOp */
            $subOp = \json_decode($result->query, true);
            if ($subOp === null) {
                throw new UnsupportedException('Cannot parse facet query for MongoDB.');
            }
            $this->facetStages[$name] = [
                'pipeline' => $this->operationToPipeline($subOp),
                'bindings' => $result->bindings,
            ];
        }

        return $this;
    }

    public function graphLookup(string $from, mixed $startWith, string $connectFromField, string $connectToField, string $as, ?int $maxDepth = null, ?string $depthField = null): static
    {
        $stage = [
            'from' => $from,
            'startWith' => '$' . $startWith,
            'connectFromField' => $connectFromField,
            'connectToField' => $connectToField,
            'as' => $as,
        ];
        if ($maxDepth !== null) {
            $stage['maxDepth'] = $maxDepth;
        }
        if ($depthField !== null) {
            $stage['depthField'] = $depthField;
        }
        $this->graphLookupStage = $stage;

        return $this;
    }

    public function mergeIntoCollection(string $collection, ?array $on = null, ?array $whenMatched = null, ?array $whenNotMatched = null): static
    {
        $stage = ['into' => $collection];
        if ($on !== null) {
            $stage['on'] = $on;
        }
        if ($whenMatched !== null) {
            $stage['whenMatched'] = $whenMatched;
        }
        if ($whenNotMatched !== null) {
            $stage['whenNotMatched'] = $whenNotMatched;
        }
        $this->mergeStage = $stage;

        return $this;
    }

    public function outputToCollection(string $collection, ?string $database = null): static
    {
        if ($database !== null) {
            $this->outStage = ['db' => $database, 'coll' => $collection];
        } else {
            $this->outStage = ['coll' => $collection];
        }

        return $this;
    }

    public function replaceRoot(string $newRootExpression): static
    {
        $this->replaceRootExpr = $newRootExpression;

        return $this;
    }

    public function search(array $searchDefinition, ?string $index = null): static
    {
        $stage = $searchDefinition;
        if ($index !== null) {
            $stage['index'] = $index;
        }
        $this->searchStage = $stage;

        return $this;
    }

    public function searchMeta(array $searchDefinition, ?string $index = null): static
    {
        $stage = $searchDefinition;
        if ($index !== null) {
            $stage['index'] = $index;
        }
        $this->searchMetaStage = $stage;

        return $this;
    }

    public function vectorSearch(string $path, array $queryVector, int $numCandidates, int $limit, ?string $index = null, ?array $filter = null): static
    {
        $stage = [
            'path' => $path,
            'queryVector' => $queryVector,
            'numCandidates' => $numCandidates,
            'limit' => $limit,
        ];
        if ($index !== null) {
            $stage['index'] = $index;
        }
        if ($filter !== null) {
            $stage['filter'] = $filter;
        }
        $this->vectorSearchStage = $stage;

        return $this;
    }

    /**
     * @param string|array<string, int> $hint
     */
    public function hint(string|array $hint): static
    {
        $this->indexHint = $hint;

        return $this;
    }

    public function reset(): static
    {
        parent::reset();
        $this->pushOps = [];
        $this->pullOps = [];
        $this->addToSetOps = [];
        $this->incOps = [];
        $this->unsetOps = [];
        $this->textSearchTerm = null;
        $this->sampleSize = null;
        $this->renameOps = [];
        $this->mulOps = [];
        $this->popOps = [];
        $this->pullAllOps = [];
        $this->minOps = [];
        $this->maxOps = [];
        $this->currentDateOps = [];
        $this->pushEachOps = [];
        $this->arrayFilters = [];
        $this->bucketStage = null;
        $this->bucketAutoStage = null;
        $this->facetStages = null;
        $this->graphLookupStage = null;
        $this->mergeStage = null;
        $this->outStage = null;
        $this->replaceRootExpr = null;
        $this->searchStage = null;
        $this->searchMetaStage = null;
        $this->vectorSearchStage = null;
        $this->indexHint = null;

        return $this;
    }

    public function build(): Plan
    {
        $this->bindings = [];

        foreach ($this->beforeBuildCallbacks as $callback) {
            $callback($this);
        }

        $this->validateTable();

        $grouped = Query::groupByType($this->pendingQueries);

        if ($this->needsAggregation($grouped)) {
            $result = $this->buildAggregate($grouped);
        } else {
            $result = $this->buildFind($grouped);
        }

        foreach ($this->afterBuildCallbacks as $callback) {
            $result = $callback($result);
        }

        return $result;
    }

    public function insert(): Plan
    {
        $this->bindings = [];
        $this->validateTable();
        $this->validateRows('insert');

        $documents = [];
        foreach ($this->pendingRows as $row) {
            $doc = [];
            foreach ($row as $col => $value) {
                $this->addBinding($value);
                $doc[$col] = '?';
            }
            $documents[] = $doc;
        }

        $operation = [
            'collection' => $this->table,
            'operation' => 'insertMany',
            'documents' => $documents,
        ];

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            executor: $this->executor,
        );
    }

    public function update(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $update = $this->buildUpdate();

        $grouped = Query::groupByType($this->pendingQueries);
        $filter = $this->buildFilter($grouped);

        if (empty($update)) {
            throw new ValidationException('No update operations specified. Call set() before update().');
        }

        $operation = [
            'collection' => $this->table,
            'operation' => 'updateMany',
            'filter' => ! empty($filter) ? $filter : new \stdClass(),
            'update' => $update,
        ];

        if (! empty($this->arrayFilters)) {
            $operation['options'] = ['arrayFilters' => $this->arrayFilters];
        }

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            executor: $this->executor,
        );
    }

    public function delete(): Plan
    {
        $this->bindings = [];
        $this->validateTable();

        $grouped = Query::groupByType($this->pendingQueries);
        $filter = $this->buildFilter($grouped);

        $operation = [
            'collection' => $this->table,
            'operation' => 'deleteMany',
            'filter' => ! empty($filter) ? $filter : new \stdClass(),
        ];

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            executor: $this->executor,
        );
    }

    public function upsert(): Plan
    {
        $this->bindings = [];
        $this->validateTable();
        $this->validateRows('upsert');

        $row = $this->pendingRows[0];

        $filter = [];
        foreach ($this->conflictKeys as $key) {
            if (! isset($row[$key])) {
                throw new ValidationException("Conflict key '{$key}' not found in row data.");
            }
            $this->addBinding($row[$key]);
            $filter[$key] = '?';
        }

        $updateColumns = $this->conflictUpdateColumns;
        if (empty($updateColumns)) {
            $updateColumns = \array_diff(\array_keys($row), $this->conflictKeys);
        }

        $setDoc = [];
        foreach ($updateColumns as $col) {
            $this->addBinding($row[$col] ?? null);
            $setDoc[$col] = '?';
        }

        $operation = [
            'collection' => $this->table,
            'operation' => 'updateOne',
            'filter' => $filter,
            'update' => ['$set' => $setDoc],
            'options' => ['upsert' => true],
        ];

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            executor: $this->executor,
        );
    }

    public function insertOrIgnore(): Plan
    {
        $result = $this->insert();
        /** @var array<string, mixed> $op */
        $op = \json_decode($result->query, true);
        $op['options'] = ['ordered' => false];

        return new Plan(
            \json_encode($op, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $result->bindings,
            executor: $this->executor,
        );
    }

    public function upsertSelect(): Plan
    {
        throw new UnsupportedException('upsertSelect() is not supported in MongoDB builder.');
    }

    private function needsAggregation(GroupedQueries $grouped): bool
    {
        if (! empty(Query::getByType($this->pendingQueries, [Method::OrderRandom], false))) {
            return true;
        }

        return ! empty($grouped->aggregations)
            || ! empty($grouped->groupBy)
            || ! empty($grouped->having)
            || ! empty($grouped->joins)
            || ! empty($this->windowSelects)
            || ! empty($this->unions)
            || ! empty($this->ctes)
            || ! empty($this->subSelects)
            || ! empty($this->rawSelects)
            || ! empty($this->lateralJoins)
            || ! empty($this->whereInSubqueries)
            || ! empty($this->existsSubqueries)
            || $grouped->distinct
            || $this->textSearchTerm !== null
            || $this->sampleSize !== null
            || $this->bucketStage !== null
            || $this->bucketAutoStage !== null
            || $this->facetStages !== null
            || $this->graphLookupStage !== null
            || $this->mergeStage !== null
            || $this->outStage !== null
            || $this->replaceRootExpr !== null
            || $this->searchStage !== null
            || $this->searchMetaStage !== null
            || $this->vectorSearchStage !== null;
    }

    private function buildFind(GroupedQueries $grouped): Plan
    {
        $filter = $this->buildFilter($grouped);
        $projection = $this->buildProjection($grouped);
        $sort = $this->buildSort();

        $operation = [
            'collection' => $this->table,
            'operation' => 'find',
        ];

        if (! empty($filter)) {
            $operation['filter'] = $filter;
        }

        if (! empty($projection)) {
            $operation['projection'] = $projection;
        }

        if (! empty($sort)) {
            $operation['sort'] = $sort;
        }

        if ($grouped->offset !== null) {
            $operation['skip'] = $grouped->offset;
        }

        if ($grouped->limit !== null) {
            $operation['limit'] = $grouped->limit;
        }

        if ($this->indexHint !== null) {
            $operation['hint'] = $this->indexHint;
        }

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            readOnly: true,
            executor: $this->executor,
        );
    }

    private function buildAggregate(GroupedQueries $grouped): Plan
    {
        $pipeline = [];

        // $searchMeta replaces other stages (returns metadata only)
        if ($this->searchMetaStage !== null) {
            $pipeline[] = ['$searchMeta' => $this->searchMetaStage];

            $operation = [
                'collection' => $this->table,
                'operation' => 'aggregate',
                'pipeline' => $pipeline,
            ];

            if ($this->indexHint !== null) {
                $operation['hint'] = $this->indexHint;
            }

            return new Plan(
                \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
                $this->bindings,
                readOnly: true,
                executor: $this->executor,
            );
        }

        // Atlas $search must be FIRST stage
        if ($this->searchStage !== null) {
            $pipeline[] = ['$search' => $this->searchStage];
        }

        // $vectorSearch must be FIRST stage
        if ($this->vectorSearchStage !== null) {
            $pipeline[] = ['$vectorSearch' => $this->vectorSearchStage];
        }

        // Text search must be first (after Atlas search)
        if ($this->textSearchTerm !== null) {
            $this->addBinding($this->textSearchTerm);
            $pipeline[] = ['$match' => ['$text' => ['$search' => '?']]];
        }

        // $sample for table sampling
        if ($this->sampleSize !== null) {
            $size = (int) \ceil($this->sampleSize);
            $pipeline[] = ['$sample' => ['size' => $size]];
        }

        // JOINs via $lookup
        foreach ($grouped->joins as $joinQuery) {
            $stages = $this->buildJoinStages($joinQuery);
            foreach ($stages as $stage) {
                $pipeline[] = $stage;
            }
        }

        // $graphLookup (after $match, similar position to $lookup)
        if ($this->graphLookupStage !== null) {
            $pipeline[] = ['$graphLookup' => $this->graphLookupStage];
        }

        // WHERE IN subqueries
        foreach ($this->whereInSubqueries as $idx => $sub) {
            $stages = $this->buildWhereInSubquery($sub, $idx);
            foreach ($stages as $stage) {
                $pipeline[] = $stage;
            }
        }

        // EXISTS subqueries
        foreach ($this->existsSubqueries as $idx => $sub) {
            $stages = $this->buildExistsSubquery($sub, $idx);
            foreach ($stages as $stage) {
                $pipeline[] = $stage;
            }
        }

        // $match (WHERE filter)
        $filter = $this->buildFilter($grouped);
        if (! empty($filter)) {
            $pipeline[] = ['$match' => $filter];
        }

        // DISTINCT without GROUP BY
        if ($grouped->distinct && empty($grouped->groupBy) && empty($grouped->aggregations)) {
            $stages = $this->buildDistinct($grouped);
            foreach ($stages as $stage) {
                $pipeline[] = $stage;
            }
        }

        // $bucket replaces $group
        if ($this->bucketStage !== null) {
            $pipeline[] = ['$bucket' => $this->bucketStage];
        } elseif ($this->bucketAutoStage !== null) {
            $pipeline[] = ['$bucketAuto' => $this->bucketAutoStage];
        } elseif (! empty($grouped->groupBy) || ! empty($grouped->aggregations)) {
            // GROUP BY + Aggregation
            $pipeline[] = ['$group' => $this->buildGroup($grouped)];

            $reshape = $this->buildProjectFromGroup($grouped);
            if (! empty($reshape)) {
                $pipeline[] = ['$project' => $reshape];
            }
        }

        // $replaceRoot (after $group or as needed)
        if ($this->replaceRootExpr !== null) {
            $pipeline[] = ['$replaceRoot' => ['newRoot' => $this->replaceRootExpr]];
        }

        // HAVING
        if (! empty($grouped->having) || ! empty($this->rawHavings)) {
            $havingFilter = $this->buildHaving($grouped);
            if (! empty($havingFilter)) {
                $pipeline[] = ['$match' => $havingFilter];
            }
        }

        // Window functions ($setWindowFields)
        if (! empty($this->windowSelects)) {
            $stages = $this->buildWindowFunctions();
            foreach ($stages as $stage) {
                $pipeline[] = $stage;
            }
        }

        // SELECT / $project (if not using group, distinct, or bucket)
        if (empty($grouped->groupBy) && empty($grouped->aggregations) && ! $grouped->distinct
            && $this->bucketStage === null && $this->bucketAutoStage === null) {
            $projection = $this->buildProjection($grouped);
            if (! empty($projection)) {
                $pipeline[] = ['$project' => $projection];
            }
        }

        // $facet (typically last or after $match)
        if ($this->facetStages !== null) {
            $facetDoc = [];
            foreach ($this->facetStages as $name => $data) {
                $facetDoc[$name] = $data['pipeline'];
                foreach ($data['bindings'] as $binding) {
                    $this->addBinding($binding);
                }
            }
            $pipeline[] = ['$facet' => $facetDoc];
        }

        // UNION ($unionWith)
        foreach ($this->unions as $union) {
            /** @var array<string, mixed>|null $subOp */
            $subOp = \json_decode($union->query, true);
            if ($subOp === null) {
                throw new UnsupportedException('Cannot parse union query for MongoDB.');
            }

            $subPipeline = $this->operationToPipeline($subOp);
            $unionWith = ['coll' => $subOp['collection']];
            if (! empty($subPipeline)) {
                $unionWith['pipeline'] = $subPipeline;
            }
            $pipeline[] = ['$unionWith' => $unionWith];
            foreach ($union->bindings as $binding) {
                $this->addBinding($binding);
            }
        }

        // Random ordering via $addFields + $sort
        $hasRandomOrder = false;
        $orderQueries = Query::getByType($this->pendingQueries, [Method::OrderRandom], false);
        if (! empty($orderQueries)) {
            $hasRandomOrder = true;
            $pipeline[] = ['$addFields' => ['_rand' => ['$rand' => new \stdClass()]]];
        }

        // ORDER BY
        $sort = $this->buildSort();
        if ($hasRandomOrder) {
            $sort['_rand'] = 1;
        }
        if (! empty($sort)) {
            $pipeline[] = ['$sort' => $sort];
        }

        // Remove _rand field
        if ($hasRandomOrder) {
            $pipeline[] = ['$unset' => '_rand'];
        }

        // OFFSET
        if ($grouped->offset !== null) {
            $pipeline[] = ['$skip' => $grouped->offset];
        }

        // LIMIT
        if ($grouped->limit !== null) {
            $pipeline[] = ['$limit' => $grouped->limit];
        }

        // $merge at the very end of pipeline
        if ($this->mergeStage !== null) {
            $pipeline[] = ['$merge' => $this->mergeStage];
        }

        // $out at the very end of pipeline (only one of $merge/$out allowed)
        if ($this->outStage !== null && $this->mergeStage === null) {
            if (isset($this->outStage['db'])) {
                $pipeline[] = ['$out' => $this->outStage];
            } else {
                $pipeline[] = ['$out' => $this->outStage['coll']];
            }
        }

        $operation = [
            'collection' => $this->table,
            'operation' => 'aggregate',
            'pipeline' => $pipeline,
        ];

        if ($this->indexHint !== null) {
            $operation['hint'] = $this->indexHint;
        }

        return new Plan(
            \json_encode($operation, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
            $this->bindings,
            readOnly: true,
            executor: $this->executor,
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function buildFilter(GroupedQueries $grouped): array
    {
        $conditions = [];

        foreach ($grouped->filters as $filter) {
            $conditions[] = $this->buildFilterQuery($filter);
        }

        if (\count($conditions) === 0) {
            return [];
        }

        if (\count($conditions) === 1) {
            return $conditions[0];
        }

        return ['$and' => $conditions];
    }

    /**
     * @return array<string, mixed>
     */
    private function buildFilterQuery(Query $query): array
    {
        $method = $query->getMethod();
        $attribute = $this->resolveAttribute($query->getAttribute());
        $values = $query->getValues();

        return match ($method) {
            Method::Equal => $this->buildIn($attribute, $values),
            Method::NotEqual => $this->buildNotIn($attribute, $values),
            Method::LessThan => $this->buildComparison($attribute, '$lt', $values),
            Method::LessThanEqual => $this->buildComparison($attribute, '$lte', $values),
            Method::GreaterThan => $this->buildComparison($attribute, '$gt', $values),
            Method::GreaterThanEqual => $this->buildComparison($attribute, '$gte', $values),
            Method::Between => $this->buildBetween($attribute, $values, false),
            Method::NotBetween => $this->buildBetween($attribute, $values, true),
            Method::StartsWith => $this->buildRegexFilter($attribute, $values, '^', ''),
            Method::NotStartsWith => $this->buildNotRegexFilter($attribute, $values, '^', ''),
            Method::EndsWith => $this->buildRegexFilter($attribute, $values, '', '$'),
            Method::NotEndsWith => $this->buildNotRegexFilter($attribute, $values, '', '$'),
            Method::Contains => $this->buildContains($attribute, $values),
            Method::ContainsAny => $query->onArray()
                ? $this->buildIn($attribute, $values)
                : $this->buildContains($attribute, $values),
            Method::ContainsAll => $this->buildContainsAll($attribute, $values),
            Method::NotContains => $this->buildNotContains($attribute, $values),
            Method::Regex => $this->buildUserRegex($attribute, $values),
            Method::IsNull => [$attribute => null],
            Method::IsNotNull => [$attribute => ['$ne' => null]],
            Method::And => $this->buildLogical($query, '$and'),
            Method::Or => $this->buildLogical($query, '$or'),
            Method::Exists => $this->buildFieldExists($query, true),
            Method::NotExists => $this->buildFieldExists($query, false),
            default => throw new UnsupportedException('Unsupported filter type for MongoDB: ' . $method->value),
        };
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildIn(string $attribute, array $values): array
    {
        $nonNulls = [];
        $hasNull = false;

        foreach ($values as $value) {
            if ($value === null) {
                $hasNull = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        if ($hasNull && empty($nonNulls)) {
            return [$attribute => null];
        }

        if (\count($nonNulls) === 1 && ! $hasNull) {
            $this->addBinding($nonNulls[0]);

            return [$attribute => '?'];
        }

        $placeholders = [];
        foreach ($nonNulls as $value) {
            $this->addBinding($value);
            $placeholders[] = '?';
        }

        if ($hasNull) {
            return ['$or' => [
                [$attribute => ['$in' => $placeholders]],
                [$attribute => null],
            ]];
        }

        return [$attribute => ['$in' => $placeholders]];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildNotIn(string $attribute, array $values): array
    {
        $nonNulls = [];
        $hasNull = false;

        foreach ($values as $value) {
            if ($value === null) {
                $hasNull = true;
            } else {
                $nonNulls[] = $value;
            }
        }

        if ($hasNull && empty($nonNulls)) {
            return [$attribute => ['$ne' => null]];
        }

        if (\count($nonNulls) === 1 && ! $hasNull) {
            $this->addBinding($nonNulls[0]);

            return [$attribute => ['$ne' => '?']];
        }

        $placeholders = [];
        foreach ($nonNulls as $value) {
            $this->addBinding($value);
            $placeholders[] = '?';
        }

        if ($hasNull) {
            return ['$and' => [
                [$attribute => ['$nin' => $placeholders]],
                [$attribute => ['$ne' => null]],
            ]];
        }

        return [$attribute => ['$nin' => $placeholders]];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildComparison(string $attribute, string $operator, array $values): array
    {
        $this->addBinding($values[0]);

        return [$attribute => [$operator => '?']];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildBetween(string $attribute, array $values, bool $not): array
    {
        $this->addBinding($values[0]);
        $this->addBinding($values[1]);

        if ($not) {
            return ['$or' => [
                [$attribute => ['$lt' => '?']],
                [$attribute => ['$gt' => '?']],
            ]];
        }

        return [$attribute => ['$gte' => '?', '$lte' => '?']];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildRegexFilter(string $attribute, array $values, string $prefix, string $suffix): array
    {
        /** @var string $rawVal */
        $rawVal = $values[0];
        $pattern = $prefix . \preg_quote($rawVal, '/') . $suffix;
        $this->addBinding($pattern);

        return [$attribute => ['$regex' => '?']];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildNotRegexFilter(string $attribute, array $values, string $prefix, string $suffix): array
    {
        /** @var string $rawVal */
        $rawVal = $values[0];
        $pattern = $prefix . \preg_quote($rawVal, '/') . $suffix;
        $this->addBinding($pattern);

        return [$attribute => ['$not' => ['$regex' => '?']]];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildContains(string $attribute, array $values): array
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $pattern = \preg_quote((string) $values[0], '/');
            $this->addBinding($pattern);

            return [$attribute => ['$regex' => '?']];
        }

        $conditions = [];
        foreach ($values as $value) {
            $pattern = \preg_quote((string) $value, '/');
            $this->addBinding($pattern);
            $conditions[] = [$attribute => ['$regex' => '?']];
        }

        return ['$or' => $conditions];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildContainsAll(string $attribute, array $values): array
    {
        /** @var array<string> $values */
        $conditions = [];
        foreach ($values as $value) {
            $pattern = \preg_quote((string) $value, '/');
            $this->addBinding($pattern);
            $conditions[] = [$attribute => ['$regex' => '?']];
        }

        return ['$and' => $conditions];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildNotContains(string $attribute, array $values): array
    {
        /** @var array<string> $values */
        if (\count($values) === 1) {
            $pattern = \preg_quote((string) $values[0], '/');
            $this->addBinding($pattern);

            return [$attribute => ['$not' => ['$regex' => '?']]];
        }

        $conditions = [];
        foreach ($values as $value) {
            $pattern = \preg_quote((string) $value, '/');
            $this->addBinding($pattern);
            $conditions[] = [$attribute => ['$not' => ['$regex' => '?']]];
        }

        return ['$and' => $conditions];
    }

    /**
     * @param array<mixed> $values
     * @return array<string, mixed>
     */
    private function buildUserRegex(string $attribute, array $values): array
    {
        /** @var string $rawVal */
        $rawVal = $values[0];
        $this->addBinding($rawVal);

        return [$attribute => ['$regex' => '?']];
    }

    /**
     * @return array<string, mixed>
     */
    private function buildLogical(Query $query, string $operator): array
    {
        $conditions = [];
        foreach ($query->getValues() as $subQuery) {
            /** @var Query $subQuery */
            $conditions[] = $this->buildFilterQuery($subQuery);
        }

        if (empty($conditions)) {
            return $operator === '$or' ? ['$expr' => false] : [];
        }

        return [$operator => $conditions];
    }

    /**
     * @return array<string, mixed>
     */
    private function buildFieldExists(Query $query, bool $exists): array
    {
        $conditions = [];
        foreach ($query->getValues() as $attr) {
            /** @var string $attr */
            $field = $this->resolveAttribute($attr);
            if ($exists) {
                $conditions[] = [$field => ['$ne' => null]];
            } else {
                $conditions[] = [$field => null];
            }
        }

        if (\count($conditions) === 1) {
            return $conditions[0];
        }

        return ['$and' => $conditions];
    }

    /**
     * @return array<string, int>
     */
    private function buildProjection(GroupedQueries $grouped): array
    {
        if (empty($grouped->selections)) {
            return [];
        }

        $projection = [];
        /** @var array<string> $values */
        $values = $grouped->selections[0]->getValues();

        foreach ($values as $field) {
            $resolved = $this->resolveAttribute($field);
            $projection[$resolved] = 1;
        }

        if (! isset($projection['_id'])) {
            $projection['_id'] = 0;
        }

        return $projection;
    }

    /**
     * @return array<string, int>
     */
    private function buildSort(): array
    {
        $sort = [];

        $orderQueries = Query::getByType($this->pendingQueries, [
            Method::OrderAsc,
            Method::OrderDesc,
        ], false);

        foreach ($orderQueries as $query) {
            $attr = $this->resolveAttribute($query->getAttribute());
            match ($query->getMethod()) {
                Method::OrderAsc => $sort[$attr] = 1,
                Method::OrderDesc => $sort[$attr] = -1,
                default => null,
            };
        }

        return $sort;
    }

    /**
     * @return array<string, mixed>
     */
    private function buildGroup(GroupedQueries $grouped): array
    {
        $group = [];

        if (! empty($grouped->groupBy)) {
            if (\count($grouped->groupBy) === 1) {
                $group['_id'] = '$' . $grouped->groupBy[0];
            } else {
                $id = [];
                foreach ($grouped->groupBy as $col) {
                    $id[$col] = '$' . $col;
                }
                $group['_id'] = $id;
            }
        } else {
            $group['_id'] = null;
        }

        foreach ($grouped->aggregations as $agg) {
            $method = $agg->getMethod();
            $attr = $agg->getAttribute();
            /** @var string $alias */
            $alias = $agg->getValue('');
            if ($alias === '') {
                $alias = $method->value;
            }

            $group[$alias] = match ($method) {
                Method::Count => ['$sum' => 1],
                Method::Sum => ['$sum' => '$' . $attr],
                Method::Avg => ['$avg' => '$' . $attr],
                Method::Min => ['$min' => '$' . $attr],
                Method::Max => ['$max' => '$' . $attr],
                default => throw new UnsupportedException('Unsupported aggregation for MongoDB: ' . $method->value),
            };
        }

        return $group;
    }

    /**
     * After $group, reshape the output to flatten the _id fields back to top-level.
     *
     * @return array<string, mixed>
     */
    private function buildProjectFromGroup(GroupedQueries $grouped): array
    {
        $project = ['_id' => 0];

        if (! empty($grouped->groupBy)) {
            if (\count($grouped->groupBy) === 1) {
                $project[$grouped->groupBy[0]] = '$_id';
            } else {
                foreach ($grouped->groupBy as $col) {
                    $project[$col] = '$_id.' . $col;
                }
            }
        }

        foreach ($grouped->aggregations as $agg) {
            /** @var string $alias */
            $alias = $agg->getValue('');
            if ($alias === '') {
                $alias = $agg->getMethod()->value;
            }
            $project[$alias] = 1;
        }

        return $project;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function buildJoinStages(Query $joinQuery): array
    {
        $table = $joinQuery->getAttribute();
        $values = $joinQuery->getValues();
        $stages = [];

        if ($joinQuery->getMethod() === Method::CrossJoin || $joinQuery->getMethod() === Method::NaturalJoin) {
            throw new UnsupportedException('Cross/natural joins are not supported in MongoDB builder.');
        }

        if (empty($values)) {
            throw new ValidationException('Join query must have values.');
        }

        /** @var string $leftCol */
        $leftCol = $values[0];
        /** @var string $rightCol */
        $rightCol = $values[2];
        /** @var string $alias */
        $alias = $values[3] ?? $table;

        $localField = $this->stripTablePrefix($leftCol);
        $foreignField = $this->stripTablePrefix($rightCol);

        $stages[] = ['$lookup' => [
            'from' => $table,
            'localField' => $localField,
            'foreignField' => $foreignField,
            'as' => $alias,
        ]];

        $isLeftJoin = $joinQuery->getMethod() === Method::LeftJoin;

        if ($isLeftJoin) {
            $stages[] = ['$unwind' => ['path' => '$' . $alias, 'preserveNullAndEmptyArrays' => true]];
        } else {
            $stages[] = ['$unwind' => '$' . $alias];
        }

        return $stages;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function buildDistinct(GroupedQueries $grouped): array
    {
        $stages = [];

        if (! empty($grouped->selections)) {
            /** @var array<string> $fields */
            $fields = $grouped->selections[0]->getValues();

            $id = [];
            foreach ($fields as $field) {
                $resolved = $this->resolveAttribute($field);
                $id[$resolved] = '$' . $resolved;
            }

            $stages[] = ['$group' => ['_id' => $id]];

            $project = ['_id' => 0];
            foreach ($fields as $field) {
                $resolved = $this->resolveAttribute($field);
                $project[$resolved] = '$_id.' . $resolved;
            }
            $stages[] = ['$project' => $project];
        }

        return $stages;
    }

    /**
     * @return array<string, mixed>
     */
    private function buildHaving(GroupedQueries $grouped): array
    {
        $conditions = [];

        if (! empty($grouped->having)) {
            foreach ($grouped->having as $havingQuery) {
                foreach ($havingQuery->getValues() as $subQuery) {
                    /** @var Query $subQuery */
                    $conditions[] = $this->buildFilterQuery($subQuery);
                }
            }
        }

        if (\count($conditions) === 0) {
            return [];
        }

        if (\count($conditions) === 1) {
            return $conditions[0];
        }

        return ['$and' => $conditions];
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function buildWindowFunctions(): array
    {
        $stages = [];

        foreach ($this->windowSelects as $win) {
            $output = [];
            $func = \strtolower(\trim($win->function, '()'));

            $mongoFunc = match ($func) {
                'row_number' => '$documentNumber',
                'rank' => '$rank',
                'dense_rank' => '$denseRank',
                default => null,
            };

            if ($mongoFunc !== null) {
                $output[$win->alias] = [$mongoFunc => new \stdClass()];
            } else {
                // Try to parse function with argument like SUM(amount)
                if (\preg_match('/^(\w+)\((.+)\)$/i', $win->function, $matches)) {
                    $aggFunc = \strtolower($matches[1]);
                    $aggCol = \trim($matches[2]);
                    $mongoAggFunc = match ($aggFunc) {
                        'sum' => '$sum',
                        'avg' => '$avg',
                        'min' => '$min',
                        'max' => '$max',
                        'count' => '$sum',
                        default => throw new UnsupportedException("Unsupported window function: {$win->function}"),
                    };
                    $output[$win->alias] = [
                        $mongoAggFunc => $aggFunc === 'count' ? 1 : '$' . $aggCol,
                        'window' => ['documents' => ['unbounded', 'current']],
                    ];
                } else {
                    throw new UnsupportedException("Unsupported window function: {$win->function}");
                }
            }

            $stage = ['$setWindowFields' => ['output' => $output]];

            if ($win->partitionBy !== null && $win->partitionBy !== []) {
                if (\count($win->partitionBy) === 1) {
                    $stage['$setWindowFields']['partitionBy'] = '$' . $win->partitionBy[0];
                } else {
                    $partitionBy = [];
                    foreach ($win->partitionBy as $col) {
                        $partitionBy[$col] = '$' . $col;
                    }
                    $stage['$setWindowFields']['partitionBy'] = $partitionBy;
                }
            }

            if ($win->orderBy !== null && $win->orderBy !== []) {
                $sortBy = [];
                foreach ($win->orderBy as $col) {
                    if (\str_starts_with($col, '-')) {
                        $sortBy[\substr($col, 1)] = -1;
                    } else {
                        $sortBy[$col] = 1;
                    }
                }
                $stage['$setWindowFields']['sortBy'] = $sortBy;
            }

            $stages[] = $stage;
        }

        return $stages;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function buildWhereInSubquery(WhereInSubquery $sub, int $idx): array
    {
        $stages = [];
        $subResult = $sub->subquery->build();

        /** @var array<string, mixed>|null $subOp */
        $subOp = \json_decode($subResult->query, true);
        if ($subOp === null) {
            throw new UnsupportedException('Cannot parse subquery for MongoDB WHERE IN.');
        }

        foreach ($subResult->bindings as $binding) {
            $this->addBinding($binding);
        }

        $subCollection = $subOp['collection'] ?? '';
        $subPipeline = $this->operationToPipeline($subOp);

        $subField = $this->extractProjectionField($subPipeline);
        $lookupAlias = '_sub_' . $idx;

        $stages[] = ['$lookup' => [
            'from' => $subCollection,
            'pipeline' => $subPipeline,
            'as' => $lookupAlias,
        ]];

        $stages[] = ['$addFields' => [
            '_sub_ids_' . $idx => ['$map' => [
                'input' => '$' . $lookupAlias,
                'as' => 's',
                'in' => '$$s.' . $subField,
            ]],
        ]];

        $column = $this->resolveAttribute($sub->column);

        if ($sub->not) {
            $stages[] = ['$match' => [
                '$expr' => ['$not' => ['$in' => ['$' . $column, '$_sub_ids_' . $idx]]],
            ]];
        } else {
            $stages[] = ['$match' => [
                '$expr' => ['$in' => ['$' . $column, '$_sub_ids_' . $idx]],
            ]];
        }

        $stages[] = ['$unset' => [$lookupAlias, '_sub_ids_' . $idx]];

        return $stages;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function buildExistsSubquery(ExistsSubquery $sub, int $idx): array
    {
        $stages = [];
        $subResult = $sub->subquery->build();

        /** @var array<string, mixed>|null $subOp */
        $subOp = \json_decode($subResult->query, true);
        if ($subOp === null) {
            throw new UnsupportedException('Cannot parse subquery for MongoDB EXISTS.');
        }

        foreach ($subResult->bindings as $binding) {
            $this->addBinding($binding);
        }

        $subCollection = $subOp['collection'] ?? '';
        $subPipeline = $this->operationToPipeline($subOp);

        // Ensure limit 1 for exists checks
        $hasLimit = false;
        foreach ($subPipeline as $stage) {
            if (isset($stage['$limit'])) {
                $hasLimit = true;
                break;
            }
        }
        if (! $hasLimit) {
            $subPipeline[] = ['$limit' => 1];
        }

        $lookupAlias = '_exists_' . $idx;

        $stages[] = ['$lookup' => [
            'from' => $subCollection,
            'pipeline' => $subPipeline,
            'as' => $lookupAlias,
        ]];

        if ($sub->not) {
            $stages[] = ['$match' => [$lookupAlias => ['$size' => 0]]];
        } else {
            $stages[] = ['$match' => [$lookupAlias => ['$ne' => []]]];
        }

        $stages[] = ['$unset' => $lookupAlias];

        return $stages;
    }

    /**
     * @return array<string, mixed>
     */
    private function buildUpdate(): array
    {
        $update = [];

        if (! empty($this->pendingRows)) {
            $setDoc = [];
            foreach ($this->pendingRows[0] as $col => $value) {
                $this->addBinding($value);
                $setDoc[$col] = '?';
            }
            $update['$set'] = $setDoc;
        }

        // Build $push with support for $each modifiers
        $pushDoc = [];
        foreach ($this->pushOps as $field => $value) {
            $this->addBinding($value);
            $pushDoc[$field] = '?';
        }
        foreach ($this->pushEachOps as $field => $modifier) {
            $eachValues = [];
            foreach ($modifier['values'] as $val) {
                $this->addBinding($val);
                $eachValues[] = '?';
            }
            $eachDoc = ['$each' => $eachValues];
            if (isset($modifier['position'])) {
                $eachDoc['$position'] = $modifier['position'];
            }
            if (isset($modifier['slice'])) {
                $eachDoc['$slice'] = $modifier['slice'];
            }
            if (isset($modifier['sort'])) {
                $eachDoc['$sort'] = $modifier['sort'];
            }
            $pushDoc[$field] = $eachDoc;
        }
        if (! empty($pushDoc)) {
            $update['$push'] = $pushDoc;
        }

        if (! empty($this->pullOps)) {
            $pullDoc = [];
            foreach ($this->pullOps as $field => $value) {
                $this->addBinding($value);
                $pullDoc[$field] = '?';
            }
            $update['$pull'] = $pullDoc;
        }

        if (! empty($this->addToSetOps)) {
            $addDoc = [];
            foreach ($this->addToSetOps as $field => $value) {
                $this->addBinding($value);
                $addDoc[$field] = '?';
            }
            $update['$addToSet'] = $addDoc;
        }

        if (! empty($this->incOps)) {
            $update['$inc'] = $this->incOps;
        }

        if (! empty($this->unsetOps)) {
            $unsetDoc = [];
            foreach ($this->unsetOps as $field) {
                $unsetDoc[$field] = '';
            }
            $update['$unset'] = $unsetDoc;
        }

        if (! empty($this->renameOps)) {
            $update['$rename'] = $this->renameOps;
        }

        if (! empty($this->mulOps)) {
            $update['$mul'] = $this->mulOps;
        }

        if (! empty($this->popOps)) {
            $update['$pop'] = $this->popOps;
        }

        if (! empty($this->pullAllOps)) {
            $pullAllDoc = [];
            foreach ($this->pullAllOps as $field => $values) {
                $placeholders = [];
                foreach ($values as $val) {
                    $this->addBinding($val);
                    $placeholders[] = '?';
                }
                $pullAllDoc[$field] = $placeholders;
            }
            $update['$pullAll'] = $pullAllDoc;
        }

        if (! empty($this->minOps)) {
            $minDoc = [];
            foreach ($this->minOps as $field => $value) {
                $this->addBinding($value);
                $minDoc[$field] = '?';
            }
            $update['$min'] = $minDoc;
        }

        if (! empty($this->maxOps)) {
            $maxDoc = [];
            foreach ($this->maxOps as $field => $value) {
                $this->addBinding($value);
                $maxDoc[$field] = '?';
            }
            $update['$max'] = $maxDoc;
        }

        if (! empty($this->currentDateOps)) {
            $update['$currentDate'] = $this->currentDateOps;
        }

        return $update;
    }

    private function stripTablePrefix(string $field): string
    {
        $parts = \explode('.', $field);

        return \count($parts) > 1 ? $parts[\count($parts) - 1] : $field;
    }

    /**
     * Convert a MongoDB operation descriptor to a pipeline.
     * For `aggregate` operations, returns the pipeline as-is.
     * For `find` operations, converts filter/projection/sort/skip/limit to pipeline stages.
     *
     * @param array<string, mixed> $op
     * @return list<array<string, mixed>>
     */
    private function operationToPipeline(array $op): array
    {
        if (($op['operation'] ?? '') === 'aggregate') {
            /** @var list<array<string, mixed>> */
            return $op['pipeline'] ?? [];
        }

        $pipeline = [];

        if (! empty($op['filter'])) {
            $pipeline[] = ['$match' => $op['filter']];
        }
        if (! empty($op['projection'])) {
            $pipeline[] = ['$project' => $op['projection']];
        }
        if (! empty($op['sort'])) {
            $pipeline[] = ['$sort' => $op['sort']];
        }
        if (isset($op['skip'])) {
            $pipeline[] = ['$skip' => $op['skip']];
        }
        if (isset($op['limit'])) {
            $pipeline[] = ['$limit' => $op['limit']];
        }

        return $pipeline;
    }

    /**
     * Extract the first projected field name from a pipeline's $project stage.
     *
     * @param list<array<string, mixed>> $pipeline
     */
    private function extractProjectionField(array $pipeline): string
    {
        foreach ($pipeline as $stage) {
            if (isset($stage['$project'])) {
                /** @var array<string, mixed> $projection */
                $projection = $stage['$project'];
                foreach ($projection as $field => $value) {
                    if ($field !== '_id' && $value === 1) {
                        return $field;
                    }
                }
            }
        }

        return '_id';
    }
}
