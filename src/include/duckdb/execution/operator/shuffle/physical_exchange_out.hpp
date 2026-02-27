#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_exchange_out.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

struct SendDataWrapper {
	// when send/receive use brpc, we keep the serialized data
	std::shared_ptr<MemoryStream> brpc_channel_data;

	// when send/receive use share_queue, just keep the DataChunk
	std::shared_ptr<DataChunk> share_queue_data;
	// it is not easy to add stat in DataChunk, so use a seperate variable
	std::string share_queue_stat;
};

// no_wait == true -> async mode, Sink/Finalize may be called multi times with same input
//                        so need to save intermediate state
//
// no_wait == false -> sync mode
class SendStateInfo {
public:
	void Reset() {
		is_new = true;
		nodes_data.clear();
	}

	bool no_wait {false};
	// first time for the input chunk, reset for each new chunk
	bool is_new {true};

	// node_addr ---> data for Sink (eof state for Finalize)
	std::map<std::string, std::shared_ptr<SendDataWrapper>> nodes_data;

	// save which node_addr is sending
	std::map<std::string, std::shared_ptr<SendDataWrapper>>::iterator it;

	idx_t index {DConstants::INVALID_INDEX};

	//\\ExchangeOutOneProfiler profiler;
	// used by Finalize
	ClientContext *context {nullptr};
};

class PhysicalExchangeOut : public PhysicalOperator {
public:
	PhysicalExchangeOut(ExchangeType exchange_type, vector<LogicalType> types, idx_t estimated_cardinality,
	                    ulong conn_id, FragmentID fragment_id, FragmentID parent_exch_in_fragment_id,
	                    vector<unique_ptr<Expression>> expressions, bool print_chunk);

	ExchangeType exchange_type;

	ulong conn_id {UINT64_MAX};
	FragmentID fragment_id {DConstants::INVALID_INDEX};
	FragmentID parent_exch_in_fragment_id {DConstants::INVALID_INDEX};

	vector<unique_ptr<Expression>> part_expressions; // \u7528\u4e8eSHUFFLE\u7c7b\u578b
	//! The types of the part_expressions
	vector<LogicalType> part_expressions_types;

	bool print_chunk = false;

	string GetName() const override;
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	// Sink interface
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

	// Dummy source interface to prevent throw exceptions.
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
};

// \u4e3aExchange\u521b\u5efa\u4e13\u7528\u7684\u64cd\u4f5c\u7b26\u72b6\u6001\u7c7b
class PhysicalExchangeOutState : public OperatorState {
public:
	// \u5bf9\u4e8e\u5206\u5e03\u5f0f\u6267\u884c\u7684\u72b6\u6001\u7ba1\u7406
	size_t current_node_index = 0;
	// \u5176\u4ed6\u53ef\u80fd\u9700\u8981\u7684\u72b6\u6001...
};

class PhysicalExchangeOutLocalSinkState : public LocalSinkState {
public:
	PhysicalExchangeOutLocalSinkState(const PhysicalExchangeOut &op, ClientContext &context);

  void Reset();

  class ShuffleOutSel {
	public:
		ShuffleOutSel() {
		}

		ShuffleOutSel(idx_t sel_capacity) : sel(sel_capacity) {
		}

		ShuffleOutSel(const ShuffleOutSel &other) : sel(other.sel), count(other.count) {
		}

		SelectionVector sel;
		ulong count{0};
  };

public:
	idx_t SerializeChunk(DataChunk &src_chunk, const SelectionVector &sel, ulong count, bool compress);
	idx_t SerializeChunk(DataChunk &src_chunk, bool compress);

	idx_t index {DConstants::INVALID_INDEX};

	ExpressionExecutor join_key_executor;
	DataChunk join_keys;
	Vector hash_values;
	Vector partition_indices;

	std::unordered_map<std::string, ShuffleOutSel> nodes_sel;

	SendStateInfo send_state_info;

	MemoryStream memory_stream;
	// used for serialize, see SerializeChunk
	DataChunk chunk;
	BinarySerializer serializer;
};

} // namespace duckdb
