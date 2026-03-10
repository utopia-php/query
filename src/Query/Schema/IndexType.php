<?php

namespace Utopia\Query\Schema;

enum IndexType: string
{
    case Index = 'index';
    case Unique = 'unique';
    case Fulltext = 'fulltext';
    case Spatial = 'spatial';
}
