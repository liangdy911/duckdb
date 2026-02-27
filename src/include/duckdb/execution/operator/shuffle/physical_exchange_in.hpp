#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/exchange_type.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

class PhysicalExchangeIn : public PhysicalOperator {
public:
	PhysicalExchangeIn(ExchangeType exchange_type, vector<LogicalType> types, idx_t estimated_cardinality,
	                   ulong conn_id, FragmentID fragment_id, FragmentID child_exch_out_fragment_id);
	PhysicalExchangeIn(ExchangeType exchange_type, vector<unique_ptr<Expression>> part_expressions,
	                   vector<LogicalType> types, idx_t estimated_cardinality);

	ExchangeType exchange_type;
	vector<unique_ptr<Expression>> part_expressions; // \u7528\u4e8eSHUFFLE\u7c7b\u578b

	string GetName() const override;
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

  // source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	SourceResultType GetDataCombine(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:

	unique_ptr<LogicalOperator> op;

	ulong conn_id{UINT64_MAX};
	FragmentID fragment_id{DConstants::INVALID_INDEX};
	FragmentID child_exch_out_fragment_id{DConstants::INVALID_INDEX};
};

// \u4e3aExchange\u521b\u5efa\u4e13\u7528\u7684\u64cd\u4f5c\u7b26\u72b6\u6001\u7c7b
class PhysicalExchangeInState : public OperatorState {
public:
	// \u5bf9\u4e8e\u5206\u5e03\u5f0f\u6267\u884c\u7684\u72b6\u6001\u7ba1\u7406
	size_t current_node_index = 0;
	// \u5176\u4ed6\u53ef\u80fd\u9700\u8981\u7684\u72b6\u6001...
};

class PhysicalExchangeInGlobalScanState : public GlobalSourceState {
public:
	explicit PhysicalExchangeInGlobalScanState(ulong conn_id, FragmentID fragment_id,
	                                           FragmentID child_exch_out_fragment_id);

	idx_t MaxThreads() override {
		D_ASSERT(channel_num != DConstants::INVALID_INDEX);
		return channel_num;
	}

	idx_t NextExecIndex() {
		idx_t index = exec_index++;
		return index;
	}

	void SetMaxThreads(idx_t threads) /*override*/ {
		D_ASSERT(max_threads == DConstants::INVALID_INDEX);
		max_threads = threads;
		// here we know actuall threads, so split channels
		for (idx_t i = 0; i < channel_num; i++) {
			channel_index_sets[i % max_threads].push_back(i);
		}
	}

public:
	//
	const ulong conn_id;

	const FragmentID fragment_id{DConstants::INVALID_INDEX};

	const FragmentID child_exch_out_fragment_id{DConstants::INVALID_INDEX};

	// actual threads
	idx_t max_threads {DConstants::INVALID_INDEX};

	// all channels num
	idx_t channel_num {DConstants::INVALID_INDEX};

	// which channel(s) each local scan is responsible
	// local scan may less than all channels, so one local scan is responsible for multi channels
	std::map<idx_t, std::deque<idx_t>> channel_index_sets;

	atomic<idx_t> exec_index;
};

class PhysicalExchangeInLocalScanState : public LocalSourceState {
public:
	PhysicalExchangeInLocalScanState(GlobalSourceState &global_state, bool no_wait, bool combine);
	SourceResultType LocalStateReadChannelData(ExecutionContext &execution_context, DataChunk &chunk,
	                                           InterruptState *interrupt_state);

	idx_t GetChannelIndex() {
		return index;
	}

	idx_t GetExecIndex() {
		return exec_index;
	}

	bool GetNoWait() {
		return no_wait;
	}
	PhysicalExchangeInGlobalScanState &gstate;
	//ExchangeInOneProfiler profiler;

	bool combine_chunk {false};
	std::unique_ptr<DataChunk> remain_chunk;
	bool finished {false};

private:
	// which channel this local scan is reading
	idx_t index {DConstants::INVALID_INDEX};

	// get from PhysicalExchangeInGlobalScanState
	std::deque<idx_t> channel_indexes;

	idx_t exec_index {DConstants::INVALID_INDEX};

	bool no_wait {false};

	//\\std::shared_ptr<ChannelSignalState> channel_signal_state;
};

} // namespace duckdb
