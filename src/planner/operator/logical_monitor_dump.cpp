#include "duckdb/planner/operator/logical_monitor_dump.hpp"
#include <string>
#include <vector>

namespace duckdb {
LogicalMonitorDump::LogicalMonitorDump() :
    LogicalOperator(LogicalOperatorType::LOGICAL_MONITOR_DUMP) {}

vector<ColumnBinding> LogicalMonitorDump::GetColumnBindings() {
    D_ASSERT(!children.empty());
	return children[0]->GetColumnBindings();
}

void LogicalMonitorDump::ResolveTypes() {
	types = children[0]->types;
}

}
