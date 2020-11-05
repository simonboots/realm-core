#ifndef DRIVER_HH
#define DRIVER_HH
#include <string>
#include <map>
#include <variant>
#include <realm/table.hpp>
#include "query_parserBaseVisitor.h"

namespace realm {
// Conducting the whole scanning and parsing of Calc++.
class ParserDriver : public query_parserVisitor {
public:
    ParserDriver(TableRef t, query_builder::Arguments& args)
        : m_base_table(t)
        , m_args(args)
    {
    }
    ~ParserDriver() override;

    Query parse(const std::string& str);

private:
    using Values = std::variant<ExpressionComparisonType, Subexpr*, DataType>;
    TableRef m_base_table;
    antlr4::tree::ParseTreeProperty<Values> m_values;
    query_builder::Arguments& m_args;

    template <class T>
    Query simple_query(int op, ColKey col_key, T val);

    antlrcpp::Any visitQuery(query_parserParser::QueryContext* context) override;
    antlrcpp::Any visitNot(query_parserParser::NotContext* context) override;
    antlrcpp::Any visitParens(query_parserParser::ParensContext* context) override;
    antlrcpp::Any visitCompareEqual(query_parserParser::CompareEqualContext*);
    antlrcpp::Any visitCompare(query_parserParser::CompareContext* context) override;
    antlrcpp::Any visitStringOps(query_parserParser::StringOpsContext*) override;
    antlrcpp::Any visitOr(query_parserParser::OrContext* context) override;
    antlrcpp::Any visitAnd(query_parserParser::AndContext* context) override;
    antlrcpp::Any visitValue(query_parserParser::ValueContext* context) override;
    antlrcpp::Any visitProperty(query_parserParser::PropertyContext*) override;
    antlrcpp::Any visitPostOp(query_parserParser::PostOpContext*) override;
    antlrcpp::Any visitPropAggr(query_parserParser::PropAggrContext*) override;
    antlrcpp::Any visitListAggr(query_parserParser::ListAggrContext*) override;
    antlrcpp::Any visitAggrOp(query_parserParser::AggrOpContext*) override;
    antlrcpp::Any visitConstant(query_parserParser::ConstantContext*) override;
    antlrcpp::Any visitPath(query_parserParser::PathContext*) override;
    antlrcpp::Any visitTrueOrFalse(query_parserParser::TrueOrFalseContext*) override;

    std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>>
    cmp(const std::vector<query_parserParser::ValueContext*>& values);
};
} // namespace realm
#endif // ! DRIVER_HH
