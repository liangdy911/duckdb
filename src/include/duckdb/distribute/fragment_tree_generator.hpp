#pragma once

#include "duckdb/common/enums/distribute_status.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/distribute/fragment.h"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

class PlanFragment {
public:
	PlanFragment(LogicalOperator *op) : op(op) {
	}

	bool IsRoot() const {
		//assert((op->type != LogicalOperatorType::LOGICAL_EXCHANGE_OUT) == fragment.IsRoot());
		return op->type != LogicalOperatorType::LOGICAL_EXCHANGE_OUT;
	}

	void GetFragments(vector<FragmentInfo *> &fragments) {
		fragments.push_back(&fragment);
		for (unique_ptr<PlanFragment> &child : children) {
			child->GetFragments(fragments);
		}
	}

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
	bool IsBroadcast() const;

	string ToString() const;

	InsertionOrderPreservingMap<string> ParamsToString() const;

	LogicalOperator *op = nullptr;
	FragmentInfo fragment;
	vector<unique_ptr<PlanFragment>> children;
	PlanFragment *parent {nullptr};
};
/**
 Example:
             Project                             Project
           ExchangeIn1                         ExchangeIn1
           ExchangeOut1                            |
            Aggregate             ==>           Fragment1
            Project                             /        \
             Join                           Fragment2  Fragment3
          /          \
    ExchangeIn2   ExchangeIn3
    ExchangeOut2  ExchangeOut3
         |          |
      Filter1     Filter2
      supplier    orders

 There are 3 fragments above.
 *
 */
class FragmentTree {
public:
	FragmentTree(DistributeRole role_, StatementType statement_type_, bool is_explain_analyze_)
	    : role(role_), statement_type(statement_type_), is_explain_analyze(is_explain_analyze_) {
	}

	const unique_ptr<PlanFragment>& GetRootPlanFragment() const {
		return root;
	}

	vector<FragmentInfo *> GetFragments();

	unique_ptr<PlanFragment> root;
	DistributeRole role;
	StatementType statement_type;
	bool is_explain_analyze;
};

class FragmentTreeGenerator {
public:
	explicit FragmentTreeGenerator(ClientContext &context);
	~FragmentTreeGenerator();

	unique_ptr<FragmentTree> Create(LogicalOperator *op, StatementType statement_type, bool is_explain_analyze);

private:
	static DistributeRole GetDistributeRole(LogicalOperator *plan, bool first = true);
	void GetAllFragments(LogicalOperator *op, PlanFragment *plan_fragment, const uint64_t conn_id);

	ClientContext &context;
  // Used to allocate fragment id.
	FragmentID fragment_id_sequence_ {ROOT_FRAGMENT_ID};
};

} // namespace duckdb
