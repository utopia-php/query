<?php

namespace Utopia\Query\Schema;

enum ColumnType: string
{
    case String = 'string';
    case Text = 'text';
    case MediumText = 'mediumText';
    case LongText = 'longText';
    case Integer = 'integer';
    case BigInteger = 'bigInteger';
    case Float = 'float';
    case Boolean = 'boolean';
    case Datetime = 'datetime';
    case Timestamp = 'timestamp';
    case Json = 'json';
    case Binary = 'binary';
    case Enum = 'enum';
    case Point = 'point';
    case Linestring = 'linestring';
    case Polygon = 'polygon';
    case Vector = 'vector';
}
