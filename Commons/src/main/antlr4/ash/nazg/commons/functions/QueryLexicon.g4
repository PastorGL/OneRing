lexer grammar QueryLexicon;

AMPERSAND : '&';
AT : '@';
BACKSLASH : '\\';
BACKTICK : '`';
BANG : '!';
CARET : '^';
CLOSE_PAR : ')';
COLON : ':';
COMMA : ',';
DOLLAR : '$';
DOT : '.';
EQ : '=';
GT : '>';
HASH : '#';
LT : '<';
MINUS : '-';
OPEN_PAR : '(';
PERCENT : '%';
PIPE : '|';
PLUS : '+';
QUESTION : '?';
SCOL : ';';
SLASH : '/';
STAR : '*';
TILDE : '~';

K_AND : A N D;
K_BETWEEN : B E T W E E N;
K_CONCAT : C O N C A T | C A T;
K_FALSE : F A L S E;
K_FROM : F R O M;
K_IN : I N;
K_INNER : I N N E R;
K_IS : I S;
K_JOIN: J O I N;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_MATCH : M A T C H;
K_NOT : N O T;
K_NULL : N U L L;
K_OR : O R;
K_OUTER : O U T E R;
K_REGEXP : R E G E X P | R E G E X;
K_RIGHT : R I G H T;
K_SELECT : S E L E C T;
K_TRUE : T R U E;
K_UNION: U N I O N;
K_WHERE : W H E R E;
K_XOR : X O R;

O_POINT : P O I N T;
O_SEGMENT : T R A C K S E G M E N T;
O_TRACK : S E G M E N T E D T R A C K;
O_POLYGON : P O L Y G O N;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | [a-zA-Z_] [a-zA-Z_0-9]*
 ;

UNARY_OPERATOR
 : PLUS
 | MINUS
 ;

NUMERIC_LITERAL
 : UNARY_OPERATOR? DIGIT+ L?
 | '0' X ( DIGIT | A | B | C | D | E | F )+
 | UNARY_OPERATOR? DIGIT+ ( DOT DIGIT* )? ( E UNARY_OPERATOR? DIGIT+ )?
 | UNARY_OPERATOR? DOT DIGIT+ ( E UNARY_OPERATOR? DIGIT+ )?
 ;

APOS
 : '\''
 ;

STRING_LITERAL
 : APOS ( ~'\'' | '\'\'' )* APOS
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
