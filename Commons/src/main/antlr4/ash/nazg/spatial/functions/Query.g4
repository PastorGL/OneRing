grammar Query;

parse
 : ( select_stmt | error ) EOF
 ;

error
 : UNEXPECTED_CHAR
   {
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
   }
 ;

select_stmt
 : K_SELECT what_expr
   ( K_FROM from_set )?
   ( K_WHERE where_expr )?
   ( K_LIMIT limit_expr )?
 ;

what_expr
 : /* STAR
 | property_name ( COMMA property_name )*
 | */ spatial_object
 ;

from_set
 : IDENTIFIER ( COMMA IDENTIFIER )*
 ;

where_expr
 : ( atomic_expr | OPEN_PAR | CLOSE_PAR | logic_op )+
 ;

limit_expr
 : INTEGER_LITERAL | NUMERIC_LITERAL PERCENT?
 ;

logic_op
 : K_NOT | K_AND | K_OR
 ;

atomic_expr
 : property_name ( equality_op | regex_op ) STRING_LITERAL
 | property_name ( equality_op | comparison_op ) NUMERIC_LITERAL
 | property_name K_IS? K_NOT? K_NULL
 ;

equality_op
 : EQ | EQ2 | NOT_EQ1 | NOT_EQ2
 ;

comparison_op
 : LT | LT_EQ | GT | GT_EQ
 ;

regex_op
 : K_LIKE | K_MATCH | K_REGEXP
 ;

property_name
 : IDENTIFIER ( DOT IDENTIFIER )?
 ;

spatial_object
 : O_POINT | O_SEGMENT | O_TRACK | O_POLYGON
 ;

SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
EQ : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ2 : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';
PERCENT : '%';

K_SELECT : S E L E C T;
K_FROM : F R O M;
K_WHERE : W H E R E;
K_LIMIT : L I M I T;
K_AND : A N D;
K_LIKE : L I K E;
K_REGEXP : R E G E X P;
K_MATCH : M A T C H;
K_NOT : N O T;
K_OR : O R;
K_IS : I S;
K_NULL : N U L L;

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
 : UNARY_OPERATOR? DIGIT+ ( DOT DIGIT* )? ( E UNARY_OPERATOR? DIGIT+ )?
 | UNARY_OPERATOR? DOT DIGIT+ ( E UNARY_OPERATOR? DIGIT+ )?
 ;

INTEGER_LITERAL
 : UNARY_OPERATOR? DIGIT+ L?
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