<?php

namespace Utopia\Query;

enum Method: string
{
    // Filter methods
    case Equal = 'equal';
    case NotEqual = 'notEqual';
    case LessThan = 'lessThan';
    case LessThanEqual = 'lessThanEqual';
    case GreaterThan = 'greaterThan';
    case GreaterThanEqual = 'greaterThanEqual';
    case Contains = 'contains';
    case ContainsAny = 'containsAny';
    case NotContains = 'notContains';
    case Search = 'search';
    case NotSearch = 'notSearch';
    case IsNull = 'isNull';
    case IsNotNull = 'isNotNull';
    case Between = 'between';
    case NotBetween = 'notBetween';
    case StartsWith = 'startsWith';
    case NotStartsWith = 'notStartsWith';
    case EndsWith = 'endsWith';
    case NotEndsWith = 'notEndsWith';
    case Regex = 'regex';
    case Exists = 'exists';
    case NotExists = 'notExists';

    // Spatial methods
    case Crosses = 'crosses';
    case NotCrosses = 'notCrosses';
    case DistanceEqual = 'distanceEqual';
    case DistanceNotEqual = 'distanceNotEqual';
    case DistanceGreaterThan = 'distanceGreaterThan';
    case DistanceLessThan = 'distanceLessThan';
    case Intersects = 'intersects';
    case NotIntersects = 'notIntersects';
    case Overlaps = 'overlaps';
    case NotOverlaps = 'notOverlaps';
    case Touches = 'touches';
    case NotTouches = 'notTouches';

    // Vector query methods
    case VectorDot = 'vectorDot';
    case VectorCosine = 'vectorCosine';
    case VectorEuclidean = 'vectorEuclidean';

    case Select = 'select';

    // Order methods
    case OrderDesc = 'orderDesc';
    case OrderAsc = 'orderAsc';
    case OrderRandom = 'orderRandom';

    // Pagination methods
    case Limit = 'limit';
    case Offset = 'offset';
    case CursorAfter = 'cursorAfter';
    case CursorBefore = 'cursorBefore';

    // Logical methods
    case And = 'and';
    case Or = 'or';
    case ContainsAll = 'containsAll';
    case ElemMatch = 'elemMatch';

    // Aggregation methods
    case Count = 'count';
    case Sum = 'sum';
    case Avg = 'avg';
    case Min = 'min';
    case Max = 'max';
    case GroupBy = 'groupBy';
    case Having = 'having';

    // Distinct
    case Distinct = 'distinct';

    // Join methods
    case Join = 'join';
    case LeftJoin = 'leftJoin';
    case RightJoin = 'rightJoin';
    case CrossJoin = 'crossJoin';

    // Union
    case Union = 'union';
    case UnionAll = 'unionAll';

    // Raw
    case Raw = 'raw';

    public function isSpatial(): bool
    {
        return match ($this) {
            self::Crosses,
            self::NotCrosses,
            self::DistanceEqual,
            self::DistanceNotEqual,
            self::DistanceGreaterThan,
            self::DistanceLessThan,
            self::Intersects,
            self::NotIntersects,
            self::Overlaps,
            self::NotOverlaps,
            self::Touches,
            self::NotTouches => true,
            default => false,
        };
    }

    public function isNested(): bool
    {
        return match ($this) {
            self::And,
            self::Or,
            self::ElemMatch,
            self::Having,
            self::Union,
            self::UnionAll => true,
            default => false,
        };
    }

    public function isAggregate(): bool
    {
        return match ($this) {
            self::Count,
            self::Sum,
            self::Avg,
            self::Min,
            self::Max => true,
            default => false,
        };
    }

    public function isJoin(): bool
    {
        return match ($this) {
            self::Join,
            self::LeftJoin,
            self::RightJoin,
            self::CrossJoin => true,
            default => false,
        };
    }

    public function isVector(): bool
    {
        return match ($this) {
            self::VectorDot,
            self::VectorCosine,
            self::VectorEuclidean => true,
            default => false,
        };
    }
}
