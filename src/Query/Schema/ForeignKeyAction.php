<?php

namespace Utopia\Query\Schema;

enum ForeignKeyAction: string
{
    case Cascade = 'CASCADE';
    case SetNull = 'SET NULL';
    case SetDefault = 'SET DEFAULT';
    case Restrict = 'RESTRICT';
    case NoAction = 'NO ACTION';
}
