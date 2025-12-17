#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/exchange_type.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

class LogicalMonitorDump : public LogicalOperator {
public:
    static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_MONITOR_DUMP;

public:
    LogicalMonitorDump();

    string GetName() const override {
		return "MonitorDump";
	}
    vector<ColumnBinding> GetColumnBindings() override;

    void Serialize(Serializer &serializer) const override;
    static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
    void ResolveTypes() override;
};


}
