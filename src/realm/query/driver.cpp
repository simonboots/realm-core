#include "realm/query/driver.hpp"
#include <realm/parser/query_builder.hpp>
#include <realm/query_expression.hpp>
#include <realm/decimal128.hpp>

#include "query_parserBaseVisitor.h"
#include "query_parserParser.h"
#include "query_parserLexer.h"
#include <ANTLRInputStream.h>

#include <map>
#include <variant>

using namespace realm;
using namespace std::string_literals;

namespace {
class MyErrorListener : public antlr4::BaseErrorListener {
public:
    void syntaxError(antlr4::Recognizer*, antlr4::Token*, size_t, size_t, const std::string& msg,
                     std::exception_ptr) override
    {
        error_msg = msg;
        called = true;
    }
    bool called = false;
    std::string error_msg;
};

class BailLexer : public query_parserLexer {
public:
    BailLexer(antlr4::ANTLRInputStream* inp)
        : query_parserLexer(inp)
    {
    }
    void recover(const antlr4::LexerNoViableAltException& e) override
    {
        throw std::runtime_error("LexerNoViableAltException");
    }
};

class BailErrorStrategy : public antlr4::DefaultErrorStrategy {
public:
    antlr4::Token* recoverInline(antlr4::Parser* recognizer) override
    {
        throw antlr4::InputMismatchException(recognizer);
        return nullptr;
    }
    void recover(antlr4::Parser*, std::exception_ptr e) override
    {
        throw std::runtime_error("Parser error");
    }
    void sync(antlr4::Parser*) override {}
};

class QueryParser : public query_parserVisitor {
public:
    QueryParser()
        : m_args(s_default_args)
    {
    }

    QueryParser(TableRef t, query_builder::Arguments& args)
        : m_base_table(t)
        , m_args(args)
    {
    }

    Query build_query(const std::string& str)
    {
        antlr4::ANTLRInputStream inp(str);
        BailLexer lexer(&inp);
        lexer.removeErrorListeners();
        lexer.addErrorListener(&m_error_listener);
        antlr4::CommonTokenStream tokens(&lexer);
        query_parserParser parser(&tokens);
        parser.setErrorHandler(std::make_shared<BailErrorStrategy>());
        parser.removeErrorListeners();
        parser.addErrorListener(&m_error_listener);

        try {
            antlr4::tree::ParseTree* tree = parser.query();
            return visit(tree).as<Query>();
        }
        catch (const std::exception& e) {
            if (m_error_listener.called) {
                std::string msg = "Invalid predicate: '" + str + "': " + m_error_listener.error_msg;
                throw std::runtime_error(msg);
            }
            else {
                throw e;
            }
        }
        return {};
    }

    void parse_only(const std::string& str)
    {
        std::cout << str << std::endl;
        MyErrorListener error_listener;
        antlr4::ANTLRInputStream inp(str);
        BailLexer lexer(&inp);
        lexer.removeErrorListeners();
        lexer.addErrorListener(&error_listener);
        antlr4::CommonTokenStream tokens(&lexer);
        query_parserParser parser(&tokens);
        parser.setErrorHandler(std::make_shared<BailErrorStrategy>());
        parser.removeErrorListeners();
        parser.addErrorListener(&error_listener);
        try {
            parser.query();
        }
        catch (const std::exception& e) {
            if (error_listener.called) {
                std::string msg = "Invalid predicate: '" + str + "': " + error_listener.error_msg;
                throw std::runtime_error(msg);
            }
            else {
                throw e;
            }
        }
    }

private:
    using QP = query_parserParser;
    using Expr = std::unique_ptr<Subexpr>;
    using Values = std::variant<ExpressionComparisonType, Subexpr*, DataType>;

    TableRef m_base_table;
    query_builder::Arguments& m_args;
    antlr4::tree::ParseTreeProperty<Values> m_values;
    MyErrorListener m_error_listener;

    static query_builder::NoArguments s_default_args;

    antlrcpp::Any visitQuery(QP::QueryContext* context) override;
    antlrcpp::Any visitNot(QP::NotContext* context) override;
    antlrcpp::Any visitParens(QP::ParensContext* context) override;
    antlrcpp::Any visitCompareEqual(QP::CompareEqualContext*);
    antlrcpp::Any visitCompare(QP::CompareContext* context) override;
    antlrcpp::Any visitStringOps(QP::StringOpsContext*) override;
    antlrcpp::Any visitOr(QP::OrContext* context) override;
    antlrcpp::Any visitAnd(QP::AndContext* context) override;
    antlrcpp::Any visitValue(QP::ValueContext* context) override;
    antlrcpp::Any visitProperty(QP::PropertyContext*) override;
    antlrcpp::Any visitPostOp(QP::PostOpContext*) override;
    antlrcpp::Any visitPropAggr(QP::PropAggrContext*) override;
    antlrcpp::Any visitListAggr(QP::ListAggrContext*) override;
    antlrcpp::Any visitAggrOp(QP::AggrOpContext*) override;
    antlrcpp::Any visitConstant(QP::ConstantContext*) override;
    antlrcpp::Any visitPath(QP::PathContext*) override;
    antlrcpp::Any visitTrueOrFalse(QP::TrueOrFalseContext*) override;

    template <class T>
    Query simple_query(int op, ColKey col_key, T val)
    {
        switch (op) {
            case query_parserParser::EQUAL:
                return m_base_table->where().equal(col_key, val);
            case query_parserParser::NOT_EQUAL:
                return m_base_table->where().not_equal(col_key, val);
            case query_parserParser::GREATER:
                return m_base_table->where().greater(col_key, val);
            case query_parserParser::LESS:
                return m_base_table->where().less(col_key, val);
            case query_parserParser::GREATER_EQUAL:
                return m_base_table->where().greater_equal(col_key, val);
            case query_parserParser::LESS_EQUAL:
                return m_base_table->where().less_equal(col_key, val);
        }
        return m_base_table->where();
    }
    std::pair<Expr, Expr> cmp(const std::vector<QP::ValueContext*>& values);
};

query_builder::NoArguments QueryParser::s_default_args;

Timestamp get_timestamp_if_valid(int64_t seconds, int32_t nanoseconds)
{
    const bool both_non_negative = seconds >= 0 && nanoseconds >= 0;
    const bool both_non_positive = seconds <= 0 && nanoseconds <= 0;
    if (both_non_negative || both_non_positive) {
        return Timestamp(seconds, nanoseconds);
    }
    throw std::runtime_error("Invalid timestamp format");
}

antlrcpp::Any QueryParser::visitQuery(query_parserParser::QueryContext* context)
{
    return visit(context->pred());
}


antlrcpp::Any QueryParser::visitNot(query_parserParser::NotContext* context)
{
    Query query = visit(context->atom_pred()).as<Query>();
    Query q = m_base_table->where();
    q.Not();
    q.and_query(query);
    return {q};
}

antlrcpp::Any QueryParser::visitParens(query_parserParser::ParensContext* context)
{
    return visit(context->pred());
}

std::pair<std::unique_ptr<Subexpr>, std::unique_ptr<Subexpr>>
QueryParser::cmp(const std::vector<query_parserParser::ValueContext*>& values)
{
    std::unique_ptr<Subexpr> left;
    std::unique_ptr<Subexpr> right;

    auto left_constant = values[0]->constant();
    auto right_constant = values[1]->constant();
    auto left_prop = values[0]->prop();
    auto right_prop = values[1]->prop();

    if (left_constant && right_constant) {
        throw std::runtime_error("Cannot compare two constants");
    }

    if (right_constant) {
        // Take left first - it cannot be a constant
        left = std::move(visit(left_prop).as<std::unique_ptr<Subexpr>>());
        m_values.put(right_constant, left->get_type());
        right = std::move(visit(right_constant).as<std::unique_ptr<Subexpr>>());
    }
    else {
        right = std::move(visit(right_prop).as<std::unique_ptr<Subexpr>>());
        if (left_constant) {
            m_values.put(left_constant, right->get_type());
            left = std::move(visit(left_constant).as<std::unique_ptr<Subexpr>>());
        }
        else {
            left = std::move(visit(left_prop).as<std::unique_ptr<Subexpr>>());
        }
    }
    return {std::move(left), std::move(right)};
}

antlrcpp::Any QueryParser::visitCompareEqual(query_parserParser::CompareEqualContext* context)
{
    auto values = context->value();
    auto [left, right] = cmp(values);
    auto op = context->op->getType();
    bool case_sensitive = true;
    if (context->CASE()) {
        case_sensitive = false;
    }

    const TableProperty* prop = dynamic_cast<const TableProperty*>(left.get());
    if (prop && !prop->links_exist() && right->has_constant_evaluation() && left->get_type() == right->get_type()) {
        auto col_key = prop->column_key();
        switch (left->get_type()) {
            case type_Int:
                return simple_query(op, col_key, right->get_mixed().get_int());
            case type_Bool:
                switch (op) {
                    case query_parserParser::EQUAL:
                        return m_base_table->where().equal(col_key, right->get_mixed().get_bool());
                    case query_parserParser::NOT_EQUAL:
                        return m_base_table->where().not_equal(col_key, right->get_mixed().get_bool());
                }
                break;
            case type_String:
                break;
            case type_Binary:
                break;
            case type_Timestamp:
                return simple_query(op, col_key, right->get_mixed().get<Timestamp>());
            case type_Float:
                break;
            case type_Double:
                break;
            case type_Decimal:
                return simple_query(op, col_key, right->get_mixed().get<Decimal128>());
                break;
            case type_ObjectId:
                break;
            case type_UUID:
                return simple_query(op, col_key, right->get_mixed().get<UUID>());
                break;
            default:
                break;
        }
    }
    if (case_sensitive) {
        switch (op) {
            case query_parserParser::EQUAL:
                return Query(std::unique_ptr<Expression>(new Compare<Equal>(std::move(right), std::move(left))));
            case query_parserParser::NOT_EQUAL:
                return Query(std::unique_ptr<Expression>(new Compare<NotEqual>(std::move(right), std::move(left))));
        }
    }
    else {
        switch (op) {
            case query_parserParser::EQUAL:
                return Query(std::unique_ptr<Expression>(new Compare<EqualIns>(std::move(right), std::move(left))));
            case query_parserParser::NOT_EQUAL:
                return Query(
                    std::unique_ptr<Expression>(new Compare<NotEqualIns>(std::move(right), std::move(left))));
        }
    }
    return {};
}

static std::map<int, std::string> opstr = {{query_parserParser::GREATER, ">"},
                                           {query_parserParser::LESS, "<"},
                                           {query_parserParser::GREATER_EQUAL, ">="},
                                           {query_parserParser::LESS_EQUAL, "<="}};

antlrcpp::Any QueryParser::visitCompare(query_parserParser::CompareContext* context)
{
    auto values = context->value();
    auto [left, right] = cmp(values);
    auto op = context->op->getType();

    if (left->get_type() == type_UUID) {
        throw std::logic_error(util::format(
            "Unsupported operator %1 in query. Only equal (==) and not equal (!=) are supported for this type.", op));
    }

    const TableProperty* prop = dynamic_cast<const TableProperty*>(left.get());
    if (prop && !prop->links_exist() && right->has_constant_evaluation() && left->get_type() == right->get_type()) {
        auto col_key = prop->column_key();
        switch (left->get_type()) {
            case type_Int:
                return simple_query(op, col_key, right->get_mixed().get_int());
            case type_Bool:
                break;
            case type_String:
                break;
            case type_Binary:
                break;
            case type_Timestamp:
                return simple_query(op, col_key, right->get_mixed().get<Timestamp>());
            case type_Float:
                break;
            case type_Double:
                break;
            case type_Decimal:
                return simple_query(op, col_key, right->get_mixed().get<Decimal128>());
                break;
            case type_ObjectId:
                break;
            case type_UUID:
                break;
            default:
                break;
        }
    }
    switch (op) {
        case query_parserParser::GREATER:
            return Query(std::unique_ptr<Expression>(new Compare<Less>(std::move(right), std::move(left))));
        case query_parserParser::LESS:
            return Query(std::unique_ptr<Expression>(new Compare<Greater>(std::move(right), std::move(left))));
        case query_parserParser::GREATER_EQUAL:
            return Query(std::unique_ptr<Expression>(new Compare<LessEqual>(std::move(right), std::move(left))));
        case query_parserParser::LESS_EQUAL:
            return Query(std::unique_ptr<Expression>(new Compare<GreaterEqual>(std::move(right), std::move(left))));
    }
    return {};
}

antlrcpp::Any QueryParser::visitStringOps(query_parserParser::StringOpsContext* context)
{
    auto values = context->value();
    auto [left, right] = cmp(values);

    auto string_expr = dynamic_cast<ConstantStringValue*>(right.get());
    std::string val = string_expr->get_mixed().get_string();

    auto op = context->op->getType();
    bool case_sensitive = true;
    if (context->CASE()) {
        case_sensitive = false;
    }

    const TableProperty* prop = dynamic_cast<const TableProperty*>(left.get());
    if (prop && !prop->links_exist() && left->get_type() == type_String) {
        auto col_key = prop->column_key();
        switch (op) {
            case query_parserParser::BEGINSWITH:
                return m_base_table->where().begins_with(col_key, val, case_sensitive);
            case query_parserParser::ENDSWITH:
                return m_base_table->where().ends_with(col_key, val, case_sensitive);
            case query_parserParser::CONTAINS:
                return m_base_table->where().contains(col_key, val, case_sensitive);
            case query_parserParser::LIKE:
                return m_base_table->where().like(col_key, val, case_sensitive);
        }
    }

    if (case_sensitive) {
        switch (op) {
            case query_parserParser::BEGINSWITH:
                return Query(std::unique_ptr<Expression>(new Compare<BeginsWith>(std::move(right), std::move(left))));
            case query_parserParser::ENDSWITH:
                return Query(std::unique_ptr<Expression>(new Compare<EndsWith>(std::move(right), std::move(left))));
            case query_parserParser::CONTAINS:
                return Query(std::unique_ptr<Expression>(new Compare<Contains>(std::move(right), std::move(left))));
            case query_parserParser::LIKE:
                return Query(std::unique_ptr<Expression>(new Compare<Like>(std::move(right), std::move(left))));
        }
    }
    else {
        switch (op) {
            case query_parserParser::BEGINSWITH:
                return Query(
                    std::unique_ptr<Expression>(new Compare<BeginsWithIns>(std::move(right), std::move(left))));
            case query_parserParser::ENDSWITH:
                return Query(
                    std::unique_ptr<Expression>(new Compare<EndsWithIns>(std::move(right), std::move(left))));
            case query_parserParser::CONTAINS:
                return Query(
                    std::unique_ptr<Expression>(new Compare<ContainsIns>(std::move(right), std::move(left))));
            case query_parserParser::LIKE:
                return Query(std::unique_ptr<Expression>(new Compare<LikeIns>(std::move(right), std::move(left))));
        }
    }
    return {};
}

antlrcpp::Any QueryParser::visitOr(query_parserParser::OrContext* context)
{
    auto ctxs = context->and_pred();
    if (ctxs.size() == 1) {
        return visit(ctxs[0]);
    }
    auto it = ctxs.begin();
    auto q = visit(*it).as<Query>();
    q.Or();

    ++it;
    while (it != ctxs.end()) {
        q.and_query(std::move(visit(*it).as<Query>()));
        ++it;
    }
    return q;
}

antlrcpp::Any QueryParser::visitAnd(query_parserParser::AndContext* context)
{
    auto ctxs = context->atom_pred();
    if (ctxs.size() == 1) {
        return visit(ctxs[0]);
    }
    Query q(m_base_table);
    for (auto ctx : ctxs) {
        q.and_query(std::move(visit(ctx).as<Query>()));
    }
    return q;
}

antlrcpp::Any QueryParser::visitValue(query_parserParser::ValueContext* context)
{
    if (auto ctx = context->prop()) {
        return visit(ctx);
    }
    return visit(context->constant());
}

antlrcpp::Any QueryParser::visitProperty(query_parserParser::PropertyContext* context)
{
    ExpressionComparisonType comp_type = ExpressionComparisonType::Any;
    if (context->aggr) {
        if (context->aggr->getType() == query_parserParser::ALL)
            comp_type = ExpressionComparisonType::All;
        if (context->aggr->getType() == query_parserParser::NONE)
            comp_type = ExpressionComparisonType::None;
    }

    m_values.put(context, comp_type);
    Subexpr* subexpr = visit(context->path()).as<LinkChain>().column(context->ID()->getText());

    if (auto ctx = context->postOp()) {
        m_values.put(context, subexpr);
        return visit(ctx);
    }
    return std::unique_ptr<realm::Subexpr>(subexpr);
}

antlrcpp::Any QueryParser::visitPostOp(query_parserParser::PostOpContext* context)
{
    auto subexpr = std::unique_ptr<realm::Subexpr>(std::get<Subexpr*>(m_values.get(context->parent)));
    switch (context->type->getType()) {
        case query_parserParser::COUNT:
            if (auto s = dynamic_cast<Columns<Link>*>(subexpr.get())) {
                return s->count().clone();
            }
            if (auto s = dynamic_cast<ColumnListBase*>(subexpr.get())) {
                return s->size().clone();
            }
            break;
        case query_parserParser::SIZE:
            if (auto s = dynamic_cast<ColumnListBase*>(subexpr.get())) {
                return s->size().clone();
            }
            break;
    }
    return subexpr;
}

antlrcpp::Any QueryParser::visitPropAggr(query_parserParser::PropAggrContext* context)
{
    std::unique_ptr<realm::Subexpr> sub_column;
    {
        auto path = visit(context->path()).as<LinkChain>();
        auto subexpr = std::unique_ptr<Subexpr>(path.column(context->ID(0)->getText()));
        auto link_prop = dynamic_cast<Columns<Link>*>(subexpr.get());
        if (!link_prop) {
            std::string msg = "Property '"s + context->ID(0)->getText() + "' is not a linklist"s;
            throw std::runtime_error(msg);
        }
        auto col_name = context->ID(1)->getText();
        auto col_key = path.get_current_table()->get_column_key(col_name);

        switch (col_key.get_type()) {
            case type_Double:
                sub_column = link_prop->column<double>(col_key).clone();
                break;
            default:
                break;
        }
    }

    auto s = dynamic_cast<SubColumnBase*>(sub_column.get());
    switch (visit(context->aggrOp()).as<size_t>()) {
        case query_parserParser::MAX:
            return s->max_of();
            break;
        case query_parserParser::MIN:
            return s->min_of();
            break;
        case query_parserParser::SUM:
            return s->sum_of();
            break;
        case query_parserParser::AVG:
            return s->avg_of();
            break;
    }
    return {};
}

antlrcpp::Any QueryParser::visitListAggr(query_parserParser::ListAggrContext* context)
{
    auto path = visit(context->path()).as<LinkChain>();
    auto subexpr = std::unique_ptr<Subexpr>(path.column(context->ID()->getText()));
    auto list_prop = dynamic_cast<ColumnListBase*>(subexpr.get());
    REALM_ASSERT(list_prop);

    switch (visit(context->aggrOp()).as<size_t>()) {
        case query_parserParser::MAX:
            return list_prop->max_of();
            break;
        case query_parserParser::MIN:
            return list_prop->min_of();
            break;
        case query_parserParser::SUM:
            return list_prop->sum_of();
            break;
        case query_parserParser::AVG:
            return list_prop->avg_of();
            break;
    }

    return {};
}

antlrcpp::Any QueryParser::visitAggrOp(query_parserParser::AggrOpContext* context)
{
    return context->type->getType();
}

antlrcpp::Any QueryParser::visitConstant(query_parserParser::ConstantContext* context)
{
    std::unique_ptr<Subexpr> ret;
    auto hint = std::get<DataType>(m_values.get(context));
    switch (context->val->getType()) {
        case query_parserParser::NUMBER: {
            auto s = context->NUMBER()->getText();
            if (hint == type_Decimal) {
                Decimal128 d(s);
                ret = std::make_unique<Value<Decimal128>>(d);
            }
            else {
                if (s.find_first_of(".eE") < s.length()) {
                    double d = strtod(s.c_str(), nullptr);
                    ret = std::make_unique<Value<double>>(d);
                }
                else {
                    int64_t n = strtol(s.c_str(), nullptr, 0);
                    ret = std::make_unique<Value<int64_t>>(n);
                }
            }
            break;
        }
        case query_parserParser::STRING: {
            auto s = context->STRING()->getText();
            std::string str = s.substr(1, s.size() - 2);
            ret = std::make_unique<ConstantStringValue>(str);
            break;
        }
        case query_parserParser::TIMESTAMP: {
            auto s = context->TIMESTAMP()->getText();
            int64_t seconds;
            int32_t nanoseconds;
            if (s[0] == 'T') {
                size_t colon_pos = s.find(":");
                std::string s1 = s.substr(1, colon_pos - 1);
                std::string s2 = s.substr(colon_pos + 1);
                seconds = strtol(s1.c_str(), nullptr, 0);
                nanoseconds = strtol(s2.c_str(), nullptr, 0);
            }
            else {
                // readable format YYYY-MM-DD-HH:MM:SS:NANOS nanos optional
                struct tm tmp = tm();
                char sep = s.find("@") < s.size() ? '@' : 'T';
                std::string fmt = "%d-%d-%d"s + sep + "%d:%d:%d:%d"s;
                int cnt = sscanf(s.c_str(), fmt.c_str(), &tmp.tm_year, &tmp.tm_mon, &tmp.tm_mday, &tmp.tm_hour,
                                 &tmp.tm_min, &tmp.tm_sec, &nanoseconds);
                REALM_ASSERT(cnt >= 6);
                tmp.tm_year -= 1900; // epoch offset (see man mktime)
                tmp.tm_mon -= 1;     // converts from 1-12 to 0-11

                if (tmp.tm_year < 0) {
                    // platform timegm functions do not throw errors, they return -1 which is also a valid time
                    throw std::logic_error("Conversion of dates before 1900 is not supported.");
                }

                seconds = platform_timegm(tmp); // UTC time
                if (cnt == 6) {
                    nanoseconds = 0;
                }
                if (nanoseconds < 0) {
                    throw std::logic_error("The nanoseconds of a Timestamp cannot be negative.");
                }
                if (seconds < 0) { // seconds determines the sign of the nanoseconds part
                    nanoseconds *= -1;
                }
            }
            ret = std::make_unique<Value<Timestamp>>(get_timestamp_if_valid(seconds, nanoseconds));
            break;
        }
        case query_parserParser::UUID: {
            auto s = context->UUID()->getText();
            ret = std::make_unique<Value<UUID>>(UUID(s.substr(5, s.size() - 6)));
            break;
        }
        case query_parserParser::NULL_VAL: {
            ret = std::make_unique<Value<null>>(realm::null());
            break;
        }
        case query_parserParser::TRUE: {
            ret = std::make_unique<Value<Bool>>(true);
            break;
        }
        case query_parserParser::FALSE: {
            ret = std::make_unique<Value<Bool>>(false);
            break;
        }
        case query_parserParser::ARG: {
            auto s = context->ARG()->getText();
            size_t arg_no = size_t(strtol(s.substr(1).c_str(), nullptr, 10));
            if (m_args.is_argument_null(arg_no)) {
                ret = std::make_unique<Value<null>>(realm::null());
            }
            else {
                switch (hint) {
                    case type_Int:
                        ret = std::make_unique<Value<int64_t>>(m_args.long_for_argument(arg_no));
                        break;
                    case type_String:
                        ret = std::make_unique<ConstantStringValue>(m_args.string_for_argument(arg_no));
                        break;
                    case type_Decimal:
                        ret = std::make_unique<Value<Decimal128>>(m_args.decimal128_for_argument(arg_no));
                        break;
                    case type_UUID:
                        ret = std::make_unique<Value<UUID>>(m_args.uuid_for_argument(arg_no));
                        break;
                    default:
                        break;
                }
            }
            break;
        }
    }
    return antlrcpp::Any(std::move(ret));
}

antlrcpp::Any QueryParser::visitTrueOrFalse(query_parserParser::TrueOrFalseContext* context)
{
    Query q = m_base_table->where();
    if (context->val->getType() == query_parserParser::TRUE_PRED) {
        q.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
    }
    else {
        q.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
    }
    return q;
}

antlrcpp::Any QueryParser::visitPath(query_parserParser::PathContext* context)
{
    auto comp_type = std::get<ExpressionComparisonType>(m_values.get(context->parent));
    LinkChain link_chain(m_base_table, comp_type);
    auto path_elems = context->ID();
    for (auto path_elem : path_elems) {
        link_chain.link(path_elem->getText());
    }
    return link_chain;
}


class MixedArguments : public query_builder::Arguments {
public:
    MixedArguments(const std::vector<Mixed>& args)
        : m_args(args)
    {
    }
    bool bool_for_argument(size_t n) final
    {
        return m_args.at(n).get<bool>();
    }
    long long long_for_argument(size_t n) final
    {
        return m_args.at(n).get<int64_t>();
    }
    float float_for_argument(size_t n) final
    {
        return m_args.at(n).get<float>();
    }
    double double_for_argument(size_t n) final
    {
        return m_args.at(n).get<double>();
    }
    StringData string_for_argument(size_t n) final
    {
        return m_args.at(n).get<StringData>();
    }
    BinaryData binary_for_argument(size_t n) final
    {
        return m_args.at(n).get<BinaryData>();
    }
    Timestamp timestamp_for_argument(size_t n) final
    {
        return m_args.at(n).get<Timestamp>();
    }
    ObjectId objectid_for_argument(size_t n) final
    {
        return m_args.at(n).get<ObjectId>();
    }
    UUID uuid_for_argument(size_t n) final
    {
        return m_args.at(n).get<UUID>();
    }
    Decimal128 decimal128_for_argument(size_t n) final
    {
        return m_args.at(n).get<Decimal128>();
    }
    ObjKey object_index_for_argument(size_t n) final
    {
        return m_args.at(n).get<ObjKey>();
    }
    bool is_argument_null(size_t n) final
    {
        return m_args.at(n).is_null();
    }

private:
    const std::vector<Mixed>& m_args;
};

} // namespace
namespace realm {

namespace antlr4_parser {

void parse_query(const std::string& str)
{
    QueryParser driver;
    driver.parse_only(str);
}

} // namespace antlr4_parser

Query Table::query(const std::string& query_string, const std::vector<Mixed>& arguments) const
{
    MixedArguments args(arguments);
    return query(query_string, args, {});
}

Query Table::query(const std::string& query_string, query_builder::Arguments& args,
                   const parser::KeyPathMapping&) const
{
    QueryParser driver(m_own_ref, args);
    return std::move(driver.build_query(query_string));
}

Subexpr* LinkChain::column(std::string col)
{
    auto col_key = m_current_table->get_column_key(col);
    if (!col_key) {
        std::string err = m_current_table->get_name();
        err += " has no property: ";
        err += col;
        throw std::runtime_error(err);
    }

    if (m_current_table->is_list(col_key)) {
        switch (col_key.get_type()) {
            case col_type_Int:
                return new Columns<Lst<Int>>(col_key, m_base_table, m_link_cols, m_comparison_type);
            case col_type_String:
                return new Columns<Lst<String>>(col_key, m_base_table, m_link_cols, m_comparison_type);
            case col_type_Timestamp:
                return new Columns<Lst<Timestamp>>(col_key, m_base_table, m_link_cols, m_comparison_type);
            case col_type_LinkList:
                add(col_key);
                return new Columns<Link>(col_key, m_base_table, m_link_cols, m_comparison_type);
            default:
                break;
        }
    }
    else {
        switch (col_key.get_type()) {
            case col_type_Int:
                return new Columns<Int>(col_key, m_base_table, m_link_cols);
            case col_type_Bool:
                return new Columns<Bool>(col_key, m_base_table, m_link_cols);
            case col_type_String:
                return new Columns<String>(col_key, m_base_table, m_link_cols);
            case col_type_Float:
                return new Columns<Float>(col_key, m_base_table, m_link_cols);
            case col_type_Double:
                return new Columns<Double>(col_key, m_base_table, m_link_cols);
            case col_type_Timestamp:
                return new Columns<Timestamp>(col_key, m_base_table, m_link_cols);
            case col_type_Decimal:
                return new Columns<Decimal128>(col_key, m_base_table, m_link_cols);
            case col_type_UUID:
                return new Columns<UUID>(col_key, m_base_table, m_link_cols);
            case col_type_Link:
                return new Columns<ObjKey>(col_key, m_base_table, m_link_cols);
            default:
                break;
        }
    }
    return nullptr;
}
} // namespace realm
