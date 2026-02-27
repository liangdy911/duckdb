//
// Created by ericqu on 2025/4/9.
//

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/shuffle/physical_exchange_out.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_exchange_out.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExchangeOut &op) {
	// Create the plan for the child first
	auto &children_plan = CreatePlan(*op.children[0]);

	// Create the appropriate physical Exchange operator
	auto &physical_plan =
	    Make<PhysicalExchangeOut>(op.exchange_type, op.types, op.estimated_cardinality, op.conn_id, op.fragment_id,
	                              op.parent_exch_in_fragment_id, std::move(op.part_expressions), op.print_chunk);
	// Add the child plan to the physical operator
	physical_plan.children.push_back(children_plan);

	return physical_plan;
}

} // namespace duckdb
