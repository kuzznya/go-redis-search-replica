lexer grammar FTLexer;

options { caseInsensitive=true; }

SPACE : [ \t\r\n]+ -> channel(HIDDEN);


FT : 'FT';
CREATE : 'CREATE';
SEARCH : 'SEARCH';

PREFIX : 'PREFIX';
SCHEMA : 'SCHEMA';

TEXT : 'TEXT';
TAG : 'TAG';
NUMERIC : 'NUMERIC';
GEO : 'GEO';
VECTOR : 'VECTOR';

LIMIT : 'LIMIT';

DOT : '.';
OR : '|';
QUOTE : '"';
COLON : ':';

LBRACE : '(';
RBRACE : ')';

Integral : Digits;

E : 'E';

Numeric
   : Digits '.' Digits? (E [+-]? Digits)?
   | '.' Digits (E [+-]? Digits)?
   | Digits E [+-]? Digits
   ;

fragment Digits : [0-9]+;

Identifier options { caseInsensitive=false; } : [A-Za-z0-9_]+;
FieldIdentifier options { caseInsensitive=false; } : '@' Identifier;
