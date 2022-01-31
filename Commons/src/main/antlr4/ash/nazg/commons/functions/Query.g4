grammar Query;

options { tokenVocab=QueryLexicon; }

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
 : spatial_object
 | STAR
 | expression ( COMMA expression )*
 ;

expression
 : ( property_name | NUMERIC_LITERAL | STRING_LITERAL | K_NULL | K_TRUE | K_FALSE | OPEN_PAR | CLOSE_PAR | expression_op )+
 ;

expression_op
 : ( PLUS | MINUS | STAR | SLASH | PERCENT | PIPE | CARET | DOLLAR | TILDE | BACKSLASH | HASH | AMPERSAND | QUESTION | BANG | GT | LT | EQ | COLON )+
 ;

from_set
 : IDENTIFIER
 | join_op IDENTIFIER ( COMMA IDENTIFIER )+
 | union_op IDENTIFIER ( COMMA IDENTIFIER )+
 | union_op IDENTIFIER STAR
 ;

union_op
 : K_UNION ( K_CONCAT | K_XOR | K_AND )?
 ;

join_op
 : ( K_INNER | K_LEFT | K_RIGHT | K_OUTER )? K_JOIN
 ;

where_expr
 : ( is_op | between_op | in_op | comparison_op | property_name | NUMERIC_LITERAL | STRING_LITERAL | K_NULL | OPEN_PAR | CLOSE_PAR | expression_op | bool_op )+
 ;

limit_expr
 : NUMERIC_LITERAL PERCENT?
 ;

is_op
 : K_IS K_NOT? K_NULL
 ;

between_op
 : K_NOT? K_BETWEEN NUMERIC_LITERAL K_AND NUMERIC_LITERAL
 ;

in_op
 : K_NOT? K_IN OPEN_PAR STRING_LITERAL ( COMMA STRING_LITERAL )* CLOSE_PAR
 | K_NOT? K_IN OPEN_PAR NUMERIC_LITERAL ( COMMA NUMERIC_LITERAL )* CLOSE_PAR
 | K_NOT? K_IN OPEN_PAR sub_query CLOSE_PAR
 ;

sub_query
 : K_SELECT property_name K_FROM IDENTIFIER ( K_WHERE where_expr )?
 ;

comparison_op
 : K_LIKE | K_MATCH | K_REGEXP
 ;

bool_op
 : K_NOT | K_AND | K_OR | K_XOR
 ;

property_name
 : IDENTIFIER ( DOT IDENTIFIER )?
 ;

spatial_object
 : O_POINT | O_SEGMENT | O_TRACK | O_POLYGON
 ;
