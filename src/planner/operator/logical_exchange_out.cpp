#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_exchange_out.hpp"

#include <string>
#include <vector>

namespace duckdb {
LogicalExchangeOut::LogicalExchangeOut(ExchangeType type, ulong conn_id, vector<unique_ptr<Expression>> hash_exprs)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXCHANGE_OUT), exchange_type(type), conn_id(conn_id),
      part_expressions(std::move(hash_exprs)) {
	if (this->part_expressions.empty() && exchange_type == ExchangeType::SHUFFLE) {
		throw InternalException("LogicalExchangeOut requires at least one partition expression for SHUFFLE");
	}
}

void FindColumnBindings(const Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		bool found = false;
		for(const auto &existing_binding : bindings) {
			if (existing_binding == col_ref.binding) {
				found = true;
				break;
			}
		}
		if (!found) {
			bindings.push_back(col_ref.binding);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { FindColumnBindings(child, bindings); });
}

vector<ColumnBinding> LogicalExchangeOut::GetColumnBindings() {
	D_ASSERT(!children.empty());
	if (!bindings.empty()) {
		return bindings;
	} else {
		for(auto &bind : children[0]->GetColumnBindings()) {
			bindings.push_back(bind);
		}
		return bindings;
	}
}


void LogicalExchangeOut::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
