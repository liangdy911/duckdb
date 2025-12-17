#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/exchange_type.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/log.hpp"

namespace duckdb {

// LogicalExchange\u64cd\u4f5c\u7b26
class LogicalExchangeIn : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXCHANGE_IN;

public:
	ExchangeType exchange_type;
	ulong conn_id{UINT64_MAX};
	// The fragment to which this ExchangeIn belongs.
	FragmentID fragment_id{DConstants::INVALID_INDEX};
	// ExchangeIn and ExchangeOut are always bound together.
	// ExchangeIn is in parent fragment and ExchangeOut is in child fragment.
	// child_exch_out_fragment_id is the ID of the child fragment.
	FragmentID child_exch_out_fragment_id{DConstants::INVALID_INDEX};
	vector<shared_ptr<Expression>> part_expressions; // \u7528\u4e8eSHUFFLE\u7c7b\u578b

	LogicalExchangeIn(ExchangeType type, ulong conn_id);

	LogicalExchangeIn(ExchangeType type, vector<shared_ptr<Expression>> hash_exprs);

	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	// unique_ptr<LogicalOperator> Copy() const {
	//   // TODO ericbqu: add copy constructor
	//   auto copy = make_uniq<LogicalExchangeIn>(exchange_type);
	//   for (auto &expr : part_expressions) {
	//     copy->part_expressions.emplace_back(std::move(expr->Copy()));
	//   }
	//   copy->children = CopyChildren();
	//   return move(copy);
	// }

	string GetName() const override {
		return std::to_string(operator_id) + ".ExchangeIn(" + ExchangeTypeToString(exchange_type) + ")";
	}

private:

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
