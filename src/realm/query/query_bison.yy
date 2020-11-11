%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.4"
%defines
// %no-lines

%define api.token.constructor
%define api.value.type variant
%define parse.assert

%code requires {
  # include <string>
  # include "realm/query_expression.hpp"
  namespace realm::query_parser {
    class ParserDriver;
    class ConstantNode;
    class PropertyNode;
    class PostOpNode;
    class AggrNode;
    class ValueNode;
    class TrueOrFalseNode;
    class OrNode;
    class AndNode;
    class AtomPredNode;
    class PathNode;
  }  
  using namespace realm::query_parser;
}

// The parsing context.
%param { ParserDriver& drv }

%locations

%define parse.trace
%define parse.error verbose

%code {
#include <realm/query/driver.hpp>
#include <realm/table.hpp>
using namespace realm;
using namespace realm::query_parser;
}

%define api.token.prefix {TOK_}
%token
  END  0  "end of file"
  TRUEPREDICATE "truepredicate"
  FALSEPREDICATE "falsepredicate"
  TRUE    "true"
  FALSE   "false"
  NULL_VAL "null"
  ASSIGN  ":="
  EQUAL   "=="
  NOT_EQUAL   "!="
  LESS    "<"
  GREATER ">"
  GREATER_EQUAL ">="
  LESS_EQUAL    "<="
  BEGINSWITH "beginswith"
  ENDSWITH "endswith"
  CONTAINS "contains"
  LIKE    "like"
  ANY     "any"
  ALL     "all"
  NONE    "none"
  SIZE    "@size"
  COUNT   "@count"
  MAX     "@max"
  MIN     "@min"
  SUM     "@sun"
  AVG     "@average"
  AND     "&&"
  OR      "||"
  MINUS   "-"
  PLUS    "+"
  STAR    "*"
  SLASH   "/"
  LPAREN  "("
  RPAREN  ")"
  NOT     "!"
  DOT     "."
;

%token <std::string> ID "identifier"
%token <std::string> STRING "string"
%token <std::string> NUMBER "number"
%token <std::string> FLOAT "float"
%token <std::string> TIMESTAMP "date"
%token <std::string> UUID "UUID"
%token <std::string> OID "ObjectId"
%token <std::string> ARG "argument"
%type  <int> equality relational stringops
%type  <ConstantNode*> constant
%type  <PropertyNode*> prop
%type  <PostOpNode*> post_op
%type  <AggrNode*> aggr_op
%type  <ValueNode*> value
%type  <TrueOrFalseNode*> boolexpr
%type  <int> comp_type
%type  <OrNode*> pred
%type  <AtomPredNode*> atom_pred
%type  <AndNode*> and_pred
%type  <PathNode*> path
%type  <std::string> path_elem

%printer { yyo << $$; } <*>;
%printer { yyo << "<>"; } <>;

%%
%start query;
query: pred { drv.result = $1; };

%left "||";
%left "&&";
%left "+" "-";
%left "*" "/";
%right "!";

pred
    : and_pred                  { $$ = drv.m_parse_nodes.create<OrNode>($1); }
    | pred "||" and_pred        { $1->and_preds.emplace_back($3); $$ = $1; }

and_pred
    : atom_pred                 { $$ = drv.m_parse_nodes.create<AndNode>($1); }
    | and_pred "&&" atom_pred   { $1->atom_preds.emplace_back($3); $$ = $1; }

atom_pred
    : value equality value      { $$ = drv.m_parse_nodes.create<EqualitylNode>($1, $2, $3); }
    | value relational value    { $$ = drv.m_parse_nodes.create<RelationalNode>($1, $2, $3); }
    | value stringops value     { $$ = drv.m_parse_nodes.create<StringOpsNode>($1, $2, $3); }
    | NOT atom_pred             { $$ = drv.m_parse_nodes.create<NotNode>($2); }
    | "(" pred ")"              { $$ = drv.m_parse_nodes.create<ParensNode>($2); }
    | boolexpr                  { $$ = drv.m_parse_nodes.create<NotNode>($1); }

value
    : constant                  { $$ = drv.m_parse_nodes.create<ValueNode>($1);}
    | prop                      { $$ = drv.m_parse_nodes.create<ValueNode>($1);}

prop
    : comp_type path ID         { $$ = drv.m_parse_nodes.create<PropNode>($2, $3, ExpressionComparisonType($1)); }
    | path ID post_op           { $$ = drv.m_parse_nodes.create<PropNode>($1, $2, $3); }
    | path ID "." aggr_op "."  ID   { $$ = drv.m_parse_nodes.create<LinkAggrNode>($1, $2, $4, $6); }
    | path ID "." aggr_op       { $$ = drv.m_parse_nodes.create<ListAggrNode>($1, $2, $4); }

constant
    : NUMBER                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NUMBER, $1); }
    | STRING                    { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::STRING, $1); }
    | FLOAT                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FLOAT, $1); }
    | TIMESTAMP                 { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TIMESTAMP, $1); }
    | UUID                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::UUID_T, $1); }
    | OID                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::OID, $1); }
    | TRUE                      { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::TRUE, ""); }
    | FALSE                     { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::FALSE, ""); }
    | NULL_VAL                  { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::NULL_VAL, ""); }
    | ARG                       { $$ = drv.m_parse_nodes.create<ConstantNode>(ConstantNode::ARG, $1); }

boolexpr
    : "truepredicate"           { $$ = drv.m_parse_nodes.create<TrueOrFalseNode>(true); }
    | "falsepredicate"          { $$ = drv.m_parse_nodes.create<TrueOrFalseNode>(false); }

comp_type
    : ANY                       { $$ = int(ExpressionComparisonType::Any); }
    | ALL                       { $$ = int(ExpressionComparisonType::All); }
    | NONE                      { $$ = int(ExpressionComparisonType::None); }

post_op
    : %empty                    { $$ = nullptr; }
    | "." COUNT                 { $$ = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::COUNT);}
    | "." SIZE                  { $$ = drv.m_parse_nodes.create<PostOpNode>(PostOpNode::SIZE);}

aggr_op
    : MAX                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::MAX);}
    | MIN                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::MIN);}
    | SUM                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::SUM);}
    | AVG                       { $$ = drv.m_parse_nodes.create<AggrNode>(AggrNode::AVG);}

equality
    : EQUAL                     { $$ = CompareNode::EQUAL; }
    | NOT_EQUAL                 { $$ = CompareNode::NOT_EQUAL; }

relational
    : LESS                      { $$ = CompareNode::LESS; }
    | LESS_EQUAL                { $$ = CompareNode::LESS_EQUAL; }
    | GREATER                   { $$ = CompareNode::GREATER; }
    | GREATER_EQUAL             { $$ = CompareNode::GREATER_EQUAL; }

stringops
    : BEGINSWITH                { $$ = CompareNode::BEGINSWITH; }
    | ENDSWITH                  { $$ = CompareNode::ENDSWITH; }
    | CONTAINS                  { $$ = CompareNode::CONTAINS; }
    | LIKE                      { $$ = CompareNode::LIKE; }

path
    : %empty                    { $$ = drv.m_parse_nodes.create<PathNode>(); }
    | path path_elem            { $1->path_elems.push_back($2); $$ = $1; }

path_elem
    : ID "."                    { $$ = $1; }
%%

void
yy::parser::error (const location_type& l, const std::string& m)
{
    std::ostringstream ostr;
    ostr << l << ": " << m;
    drv.error(ostr.str());
}
