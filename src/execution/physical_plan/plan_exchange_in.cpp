//
// Created by ericqu on 2025/4/9.
//

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/shuffle/physical_exchange_in.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_exchange_in.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExchangeIn &op, ClientContext &context) {
	auto &logical = (LogicalExchangeIn &)op;

	// no need to CreatePlan children[0], just save in PhysicalExchangeIn
	// we will serialize and send this LogicalOperator

	// Create the plan for the child first
	auto &children_plan = CreatePlan(*logical.children[0]);

	// Create the appropriate physical Exchange operator
	auto &physical_plan =
	    Make<PhysicalExchangeIn>(logical.exchange_type, logical.types, op.estimated_cardinality, logical.conn_id,
	                             logical.fragment_id, logical.child_exch_out_fragment_id);

	if (context.IsDistributeCoordinator()) {
		// Add the child plan to the physical operator
		physical_plan.children.push_back(children_plan);
	}

	return physical_plan;
}

} // namespace duckdb
