#include "duckdb/distribute/fragment_tree_generator.hpp"

#include "duckdb/common/log.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/planner/operator/logical_exchange_in.hpp"
#include "duckdb/planner/operator/logical_exchange_out.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"

#ifdef AS_MYSQL_SUBPROJECT
#include "sql/ve/extension/include/tdstore_fragment.hpp"
#endif

namespace duckdb {

bool PlanFragment::IsBroadcast() const {
  if (IsRoot())
    return false;
  return false;//return ((LogicalExchangeOut*)op)->is_broadcast;
}

string PlanFragment::ToString() const {
	//TextTreeRenderer renderer;
	return "";//return renderer.ToString(*this);
}

InsertionOrderPreservingMap<string> PlanFragment::ParamsToString() const{
	InsertionOrderPreservingMap<string> result;
	result["fragment"] = fragment.ToString();
	return result;
}

vector<FragmentInfo *> FragmentTree::GetFragments() {
	vector<FragmentInfo *> fragments;
	root->GetFragments(fragments);
	return fragments;
}

FragmentTreeGenerator::FragmentTreeGenerator(ClientContext &context) : context(context) {
}

FragmentTreeGenerator::~FragmentTreeGenerator() {
}

DistributeRole FragmentTreeGenerator::GetDistributeRole(LogicalOperator *plan, bool first /* = true*/) {
	// It's must be worker if first is exchange-out
	if (first && plan->type == LogicalOperatorType::LOGICAL_EXCHANGE_OUT) {
		return DistributeRole::WORKER;
	}
	for (idx_t i = 0; i < plan->children.size(); i++) {
		DistributeRole role = GetDistributeRole(plan->children[i].get(), false);
		if (role != DistributeRole::UNKNNON)
			return role;
	}
	// It's must be coordinator if the first is not exchange-out and exist exchange-in.
	if (plan->type == LogicalOperatorType::LOGICAL_EXCHANGE_IN) {
		return DistributeRole::COORDINATOR;
	}
	return DistributeRole::UNKNNON;
}

unique_ptr<FragmentTree> FragmentTreeGenerator::Create(LogicalOperator *op, StatementType statement_type,
                                                       bool is_explain_analyze) {
#ifdef AS_MYSQL_SUBPROJECT
	DistributeRole dist_role = GetDistributeRole(op);
	if (dist_role != DistributeRole::UNKNNON) {

		unique_ptr<FragmentTree> frag_tree = make_uniq<FragmentTree>(dist_role, statement_type, is_explain_analyze);

		if (dist_role == DistributeRole::COORDINATOR) {
			frag_tree->root = make_uniq<PlanFragment>(op);
			auto &frag_info = frag_tree->root->fragment;
			auto conn_id = GetQueryID();
			frag_info.conn_id_ = conn_id;
			frag_info.fragment_id_ = fragment_id_sequence_++;
			GetAllFragments(op, frag_tree->root.get(), conn_id);

			// Set other parameters.
			auto &profiler = QueryProfiler::Get(context);
			profiler.StartPhase(MetricsType::TDSQL_COMPUTE_FRAGMENT_PARAMS);
			ComputeFragmentParams(context, frag_tree->root);
			profiler.EndPhase();
		}
		return frag_tree;
	} else {
		// Plan is not distributed, run it in current node.
		// Assign rep_group_meta for TSCs in this plan.
		ComputePlanRepGroupMeta(context, op);
		return nullptr;
	}
#else
	return nullptr;
#endif
}

void FragmentTreeGenerator::GetAllFragments(LogicalOperator *op, PlanFragment *plan_fragment, const uint64_t conn_id) {
#ifdef AS_MYSQL_SUBPROJECT
	// ExchangeOut is the beginner of a fragment.
	if (op->type == LogicalOperatorType::LOGICAL_EXCHANGE_OUT) {
		unique_ptr<PlanFragment> fragment_new = make_uniq<PlanFragment>(op);
		fragment_new->parent = plan_fragment;
		auto &frag_info = fragment_new->fragment;
		frag_info.conn_id_ = conn_id;
		frag_info.fragment_id_ = fragment_id_sequence_++;
		plan_fragment->children.push_back(std::move(fragment_new));
		// update the current fragment.
		plan_fragment = plan_fragment->children.back().get();
	}

	// Process child
	for (idx_t i = 0; i < op->children.size(); i++) {
		GetAllFragments(op->children[i].get(), plan_fragment, conn_id);
	}

#endif
}

} // namespace duckdb
