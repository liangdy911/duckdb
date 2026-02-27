#include "duckdb/distribute/fragment_scheduler.hpp"
#include "duckdb/distribute/fragment_tree_generator.hpp"
#include "duckdb/common/log.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#ifdef AS_MYSQL_SUBPROJECT
#include "sql/ve/extension/include/tdstore_fragment.hpp"
#endif

namespace duckdb {

/*void FregmentScheduler::ExecuteForever(atomic<bool> *marker) {
  fprintf(stderr, "ExecuteForever\n");
  TaskScheduler::ExecuteForever(marker);
}

idx_t FregmentScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
  fprintf(stderr, "ExecuteTasks: %lu\n", max_tasks);
  return TaskScheduler::ExecuteTasks(marker, max_tasks);
}

void FregmentScheduler::ExecuteTasks(idx_t max_tasks) {
  fprintf(stderr, "ExecuteTasks\n");
  TaskScheduler::ExecuteTasks(max_tasks);
}*/

FragmentScheduler &FragmentScheduler::GetScheduler(ClientContext &context) {
	return DatabaseInstance::GetDatabase(context).GetDistributeScheduler();
}

void FragmentScheduler::ExecuteInThread(FragmentTree *frag_tree, ClientContext *context) {
	if (frag_tree->statement_type != StatementType::EXPLAIN_STATEMENT || frag_tree->is_explain_analyze) {
		if (frag_tree->role == DistributeRole::COORDINATOR) {
			ExecuteInCoordinator(frag_tree, context);
		} else if (frag_tree->role == DistributeRole::WORKER) {
			ExecuteInWorker(frag_tree, context);
		} else {
			D_ASSERT(false);
		}
	}
}

void FragmentScheduler::ExecuteFinalize(FragmentTree *frag_tree, ClientContext *context) {
#ifdef AS_MYSQL_SUBPROJECT
	if (frag_tree->statement_type != StatementType::EXPLAIN_STATEMENT || frag_tree->is_explain_analyze) {
		D_ASSERT(frag_tree->role != DistributeRole::UNKNNON);
		if (frag_tree->role == DistributeRole::COORDINATOR) {
			// 1. clean context
			CleanAllContexts(frag_tree, context);
		}
	}
#endif
}

void FragmentScheduler::CleanAllContexts(FragmentTree *frag_tree, ClientContext *context) {
#ifdef AS_MYSQL_SUBPROJECT
	vector<FragmentInfo *> fragments = frag_tree->GetFragments();

	ulong conn_id = UINT64_MAX;
	std::unordered_set<std::string> node_addrs;
	for (size_t i = 0; i < fragments.size(); i++) {
		FragmentInfo *fragment = fragments[i];
		if (i > 0) {
			D_ASSERT(conn_id == fragment->conn_id_);
		}
		conn_id = fragment->conn_id_;
		if (!fragment->IsRoot()) {
			for (const auto &[_, exec_info] : fragment->placement_and_exec_info) {
				node_addrs.emplace(exec_info.placement_node->node_rpc_addr);
			}
		}
	}

	CleanContexts(*context, conn_id, node_addrs);
#endif
}

void FragmentScheduler::ExecuteInCoordinator(FragmentTree *frag_tree, ClientContext *context) {
#ifdef AS_MYSQL_SUBPROJECT
	if DUCKDB_CAN_PRINT_LOG(INFO) {
		auto &root = frag_tree->GetRootPlanFragment();
		DUCK_LOG(INFO) << "print plan_tree for query:" <<context->GetQuery() << ", plan info:" << std::endl << root->ToString();
	}

	vector<FragmentInfo *> fragments = frag_tree->GetFragments();
	// 1. Send fragments
	for (FragmentInfo *fragment : fragments) {
		if (GlobalSendAllFragment(*context, *fragment)) {
			throw InternalException("GlobalSendAllFragment fail");
		}
	}

	// 2. Setup fragment channels
	for (FragmentInfo *fragment : fragments) {
		if (GlobalSetupChannels(*context, *fragment)) {
			throw InternalException("GlobalSetupChannels fail");
		}
	}

	// 3. Execute fragment plan
	for (FragmentInfo *fragment : fragments) {
		if (ExecutePlan(*context, *fragment)) {
			throw InternalException("ExecutePlan fail");
		}
	}

	// 4. Setup mgr channel

	// 5. Do while for get the status from workers.
#endif
}

void FragmentScheduler::ExecuteInWorker(FragmentTree *frag_tree, ClientContext *context) {
#ifdef AS_MYSQL_SUBPROJECT

	// 1. Do while for get the status to coordinator.
#endif
}
} // namespace duckdb
