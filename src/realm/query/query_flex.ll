%{ /* -*- C++ -*- */
# include <cerrno>
# include <climits>
# include <cstdlib>
# include <cstring> // strerror
# include <string>
# include "realm/query/driver.hpp"
# include "realm/query/query_bison.hpp"
%}

%option noyywrap nounput noinput batch debug

hex     [0-9a-fA-F]
unicode '\\u' hex{4}
simple  '\\' ['"/bfnrt0\\]
id      [a-zA-Z][a-zA-Z_\-0-9]*
string  ["][a-zA-Z0-9_\\/\.,: \[\]+$={}<>!();-]*["]
digit   [0-9]
int     {digit}+
sint    [+-]?{digit}+
optint  {digit}*
exp     [eE]{sint}
chars   ~["'\\]
blank   [ \t\r]

%{
  // Code run each time a pattern is matched.
  # define YY_USER_ACTION  loc.columns (yyleng);
%}
%%
%{
  // A handy shortcut to the location held by the driver.
  yy::location& loc = drv.location;
  // Code run each time yylex is called.
  loc.step ();
%}
{blank}+   loc.step ();
\n+        loc.lines (yyleng); loc.step ();

"=="       return yy::parser::make_EQUAL  (loc);
"!="       return yy::parser::make_NOT_EQUAL(loc);
"<"        return yy::parser::make_LESS   (loc);
">"        return yy::parser::make_GREATER(loc);
"<="       return yy::parser::make_LESS_EQUAL (loc);
">="       return yy::parser::make_GREATER_EQUAL (loc);
&&|(?i:and)       return yy::parser::make_AND    (loc);
"||"|"or"        return yy::parser::make_OR     (loc);
"-"        return yy::parser::make_MINUS  (loc);
"+"        return yy::parser::make_PLUS   (loc);
"*"        return yy::parser::make_STAR   (loc);
"/"        return yy::parser::make_SLASH  (loc);
"("        return yy::parser::make_LPAREN (loc);
")"        return yy::parser::make_RPAREN (loc);
":="       return yy::parser::make_ASSIGN (loc);
"!"        return yy::parser::make_NOT    (loc);
"."        return yy::parser::make_DOT    (loc);
"any"|"ANY"|"some"|"SOME" return yy::parser::make_ANY(loc);
"all"|"ALL" return yy::parser::make_ALL(loc);
"none"|"NONE" return yy::parser::make_NONE(loc);
(?i:beginswith) return yy::parser::make_BEGINSWITH    (loc);
(?i:endswith) return yy::parser::make_ENDSWITH    (loc);
(?i:contains) return yy::parser::make_CONTAINS    (loc);
(?i:like) return yy::parser::make_LIKE    (loc);
(?i:truepredicate) return yy::parser::make_TRUEPREDICATE (loc); 
(?i:falsepredicate) return yy::parser::make_FALSEPREDICATE (loc); 
(?i:null)|(?i:nil) return yy::parser::make_NULL_VAL (loc);
"@size" return yy::parser::make_SIZE    (loc);
"@count" return yy::parser::make_COUNT    (loc);
"@max" return yy::parser::make_MAX    (loc);
"@min" return yy::parser::make_MIN    (loc);
"@sum" return yy::parser::make_SUM    (loc);
"@avg" return yy::parser::make_AVG    (loc);
(true|TRUE) return yy::parser::make_TRUE    (loc);
(false|FALSE) return yy::parser::make_FALSE    (loc);
"uuid("{hex}{8}"-"{hex}{4}"-"{hex}{4}"-"{hex}{4}"-"{hex}{12}")" return yy::parser::make_UUID(yytext, loc); 
"oid("{hex}{24}")" return yy::parser::make_OID(yytext, loc); 
("T"{sint}":"{sint})|({int}"-"{int}"-"{int}[@T]{int}":"{int}":"{int}(":"{int})?) return yy::parser::make_TIMESTAMP(yytext, loc);
"$"{int} return yy::parser::make_ARG(yytext, loc); 
{int}{exp}?     return yy::parser::make_NUMBER (yytext, loc);
({int}"."{optint})|({optint}"."{int}){exp}?    return yy::parser::make_FLOAT (yytext, loc);
{string}   return yy::parser::make_STRING (yytext, loc);
{id}       return yy::parser::make_ID (yytext, loc);

.          {
             throw yy::parser::syntax_error
               (loc, "invalid character: " + std::string(yytext));
           }

<<EOF>>    return yy::parser::make_END (loc);
%%

void realm::query_parser::ParserDriver::scan_begin (bool trace_scanning)
{
    yy_flex_debug = trace_scanning;
    YY_BUFFER_STATE bp;
    bp = yy_scan_bytes(parse_string.c_str(), parse_string.size());
    yy_switch_to_buffer(bp);
    scan_buffer = (void *)bp;
}

void realm::query_parser::ParserDriver::scan_end ()
{
   yy_delete_buffer((YY_BUFFER_STATE)scan_buffer);
}
