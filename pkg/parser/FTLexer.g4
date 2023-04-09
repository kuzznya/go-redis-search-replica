lexer grammar FTLexer;

options { caseInsensitive=true; }

SPACE : [ \t\r\n]+ -> channel(HIDDEN);


FT : 'FT';
CREATE : 'CREATE';
SEARCH : 'SEARCH';

DOT : '.';


Identifier options { caseInsensitive=false; } : [A-Za-z0-9]+ ;

Integral : Digits;

E : 'E';

Numeric
   : Digits '.' Digits? (E [+-]? Digits)?
   | '.' Digits (E [+-]? Digits)?
   | Digits E [+-]? Digits
   ;

fragment Digits : [0-9]+;
