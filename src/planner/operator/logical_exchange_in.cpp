#include "duckdb/planner/operator/logical_exchange_in.hpp"
#include <string>
#include <vector>

namespace duckdb {
LogicalExchangeIn::LogicalExchangeIn(ExchangeType type, ulong conn_id)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXCHANGE_IN), exchange_type(type), conn_id(conn_id) {
}

LogicalExchangeIn::LogicalExchangeIn(ExchangeType type, vector<shared_ptr<Expression>> hash_exprs)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXCHANGE_IN), exchange_type(type),
      part_expressions(std::move(hash_exprs)) {
}

vector<ColumnBinding> LogicalExchangeIn::GetColumnBindings() {
	D_ASSERT(!children.empty());
	return children[0]->GetColumnBindings();
}


void LogicalExchangeIn::ResolveTypes() {
	types = children[0]->types;
}
} // namespace duckdb
