grammar query_parser;

@members { size_t jed; }

query: pred EOF;

pred: and_pred (OR and_pred)*                                          # or;  
and_pred: atom_pred (AND atom_pred)*                                   # and;

atom_pred
    : value op=(EQUAL|NOT_EQUAL) CASE? value                           # compareEqual
    | value op=(GREATER|GREATER_EQUAL|LESS|LESS_EQUAL) value           # compare
    | value op=(BEGINSWITH|ENDSWITH|CONTAINS|LIKE) CASE? value         # stringOps
    | NOT_PRED atom_pred                                                    # not
    | '(' pred ')'                                                     # parens
    | val=(TRUE_PRED | FALSE_PRED)                                     # trueOrFalse
    ;

value : prop | constant;

prop
    : aggr=(ANY|ALL|NONE)? path  ID ('.' postOp)?                      # property
    | path ID '.' aggrOp '.' ID                                        # propAggr
    | path ID '.' aggrOp                                               # listAggr
    ;

constant : val=(NUMBER|STRING|TIMESTAMP|UUID|TRUE|FALSE|NULL_VAL|ARG);

path: (ID '.')*;

postOp : type=(COUNT | SIZE); 
aggrOp : type=(MAX | MIN | SUM | AVG); 

// Lexer part

NUMBER: SIGN? (FLOAT_NUM | HEX_NUM | INT_NUM | INFINITY  | NAN);

TRUE_PRED : 'TRUEPREDICATE' | 'truepredicate';
FALSE_PRED : 'FALSEPREDICATE' | 'falsepredicate';
NOT_PRED : '!' | 'not' | 'NOT';

COUNT : '@count';
SIZE :  '@size';
MAX : '@max';
MIN : '@min';
SUM : '@sum';
AVG : '@avg';

ANY : 'any' | 'ANY' | 'some' | 'SOME';
ALL : 'all' | 'ALL';
NONE : 'none' | 'NONE';

TRUE : 'true' | 'TRUE';
FALSE : 'false' | 'FALSE';
NULL_VAL: 'NULL' | 'NIL' | 'null' | 'nil';

AND : 'and' | 'AND' | '&&';
OR : 'or' | 'OR' |'||';

GREATER: '>';
LESS: '<';
GREATER_EQUAL: '>=' | '=>';
LESS_EQUAL: '<=' | '=<';
EQUAL: '=='| '=' | 'in' | 'IN';
NOT_EQUAL: '!=' | '<>';

BEGINSWITH: 'BEGINSWITH' | 'beginswith';
ENDSWITH: 'ENDSWITH' | 'endswith';
CONTAINS: 'CONTAINS' | 'contains';
LIKE: 'LIKE' | 'like';
CASE : '[c]';

ARG    : '$' DIGIT+;
ID     : [a-zA-Z_$] [a-zA-Z_\-$0-9]*;
STRING : SQ_STRING | DQ_STRING;
TIMESTAMP: ('T' NUMBER ':' NUMBER) | ( INT '-' INT '-' INT ('@'|'T') INT ':' INT ':' INT (':' INT)? );
UUID: 'uuid(' HEX8 '-' HEX4 '-' HEX4 '-' HEX4 '-' HEX12 ')';

fragment DQ_STRING :  '"' CHARS* '"' ;
fragment SQ_STRING : '\'' CHARS* '\'';
fragment FLOAT_NUM 
    :   DIGIT+ '.' DIGIT* EXP?
    |   DIGIT* '.' DIGIT+ EXP?
    ;
fragment INT_NUM :  DIGIT+ EXP?;
fragment HEX_NUM : '0' [xX] HEX+;
fragment INFINITY: ('inf' | 'infinity');
fragment NAN: 'NaN';
fragment INT    : DIGIT+;
fragment SIGN   : [+\-'];
fragment DIGIT  : [0-9] ;
fragment EXP    : [eE] SIGN? INT;
fragment CHARS  : ESC | ~["'\\];
fragment ESC :   SIMPLE_ESC | UNICODE ;
fragment SIMPLE_ESC : '\\' ['"/bfnrt0\\];
fragment UNICODE : '\\u' HEX4;
fragment HEX12 : HEX4 HEX4 HEX4;
fragment HEX8 : HEX4 HEX4;
fragment HEX4 : HEX HEX HEX HEX;
fragment HEX : [0-9a-fA-F] ;

WS     :  [ \t\r\n] -> skip;

