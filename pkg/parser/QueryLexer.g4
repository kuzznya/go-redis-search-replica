lexer grammar QueryLexer;

options { caseInsensitive=true; }

SPACE : [ \t\r\n]+ -> channel(HIDDEN);

OR : '|';

LBRACE : '(';
RBRACE : ')';
QUOTE : '"';
COLON : ':';

FieldIdentifier options { caseInsensitive=false; } : '@' String;
EscapedString options { caseInsensitive=false; } : '"' (EscapeChar | AnyNonSyntaxChar)*? '"';
String options { caseInsensitive=false; } : AnyNonSyntaxChar+;

fragment AnyNonSyntaxChar : ~(' ' | '(' | ')' | ':' | '"' | '|');

fragment EscapeChar : '\\' .;
