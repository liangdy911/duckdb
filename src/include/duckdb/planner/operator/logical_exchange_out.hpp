#pragma once

#include "duckdb/common/enums/exchange_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/log.hpp"

namespace duckdb {

class LogicalExchangeOut : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXCHANGE_OUT;

public:
	ExchangeType exchange_type;
	ulong conn_id{UINT64_MAX};
	// See comment in LogicalExchangeIn.
	FragmentID fragment_id{DConstants::INVALID_INDEX};
	FragmentID parent_exch_in_fragment_id{DConstants::INVALID_INDEX};
	vector<unique_ptr<Expression>> part_expressions; // \u7528\u4e8eSHUFFLE\u7c7b\u578b
	vector<ColumnBinding> bindings;

	bool print_chunk = false;

	LogicalExchangeOut(ExchangeType type, ulong conn_id, vector<unique_ptr<Expression>> hash_exprs);

	vector<ColumnBinding> GetColumnBindings() override;

	void SetPrintChunk(bool print_chunk) { this->print_chunk = print_chunk; }
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	// unique_ptr<LogicalOperator> Copy() const {
	//   // TODO ericbqu: add copy constructor
	//   auto copy = make_uniq<LogicalExchangeOut>(exchange_type);
	//   for (auto &expr : part_expressions) {
	//     copy->part_expressions.emplace_back(std::move(expr->Copy()));
	//   }
	//   copy->children = CopyChildren();
	//   return move(copy);
	// }

	string GetName() const override {
		return std::to_string(operator_id) + ".ExchangeOut(" + ExchangeTypeToString(exchange_type) + ")";
	}

	bool RequireOptimizer() const override {
		return false;
	}

private:

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
