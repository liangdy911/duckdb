//
// Created by ericqu on 2025/4/9.
//
#include "duckdb/execution/operator/shuffle/physical_exchange_out.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/distribute/duckdb_fragment_stub.hpp"
#include "duckdb/common/log.hpp"

namespace duckdb {

PhysicalExchangeOut::PhysicalExchangeOut(ExchangeType exchange_type, vector<LogicalType> types,
                                         idx_t estimated_cardinality, ulong conn_id, FragmentID fragment_id,
                                         FragmentID parent_exch_in_fragment_id,
                                         vector<unique_ptr<Expression>> expressions, bool print_chunk)
    : PhysicalOperator(PhysicalOperatorType::EXCHANGE_OUT, std::move(types), estimated_cardinality),
      exchange_type(exchange_type), conn_id(conn_id), fragment_id(fragment_id),
      parent_exch_in_fragment_id(parent_exch_in_fragment_id), part_expressions(std::move(expressions)),
	  print_chunk(print_chunk) {
	for (idx_t idx = 0; idx < part_expressions.size(); idx++) {
		part_expressions_types.push_back(part_expressions[idx]->return_type);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PhysicalExchangeOutGlobalSinkState : public GlobalSinkState {
public:
	PhysicalExchangeOutGlobalSinkState(ulong conn_id, FragmentID fragment_id, bool no_wait) {
		max_channel_index = GetMaxChannelIndex(conn_id, fragment_id);
		//DUCK_LOG(INFO) << max_channel_index << " channels setup for conn_id:" << conn_id
		//               << " fragment_id:" << fragment_id;

		for (idx_t i = 0; i < max_channel_index; i++) {
			free_channel_queue.push(i);
		}

		send_state_info.no_wait = no_wait;

		// will update in PhysicalExchangeOut::Finalize
		send_state_info.index = DConstants::INVALID_INDEX;
	}

	idx_t AcquireChannelIndex() {
		lock_guard<mutex> guard(lock);
		if (free_channel_queue.empty()) {
			throw InternalException("more PipelineExecutor than channels:%lu", max_channel_index);
		}
		idx_t index = free_channel_queue.front();
		free_channel_queue.pop();
		return index;
	}

	void ReleaseChannelIndex(idx_t index) {
		lock_guard<mutex> guard(lock);
		free_channel_queue.push(index);
	}

	idx_t MaxThreads(idx_t source_max_threads) override {
		if (source_max_threads > max_channel_index) {
			return max_channel_index;
		}
		return source_max_threads;
	}

	idx_t MaxChannelIndex() {
		return max_channel_index;
	}

	std::queue<idx_t> &FreeChannelQueue() {
		return free_channel_queue;
	}

	// used for Finalize
	SendStateInfo send_state_info;

private:
	mutex lock;
	std::queue<idx_t> free_channel_queue;

	// how many channels set per node
	// each PipelineExecutor need its own channel
	idx_t max_channel_index {0};
};

PhysicalExchangeOutLocalSinkState::PhysicalExchangeOutLocalSinkState(const PhysicalExchangeOut &op,
                                                                     ClientContext &context)
    : join_key_executor(context), hash_values(LogicalType::HASH), partition_indices(LogicalType::UBIGINT),
      serializer(memory_stream) {

	auto &allocator = BufferAllocator::Get(context);

	for (auto &expr : op.part_expressions) {
		join_key_executor.AddExpression(*expr);
	}
	if (op.part_expressions_types.size()) {
		join_keys.Initialize(allocator, op.part_expressions_types);
	}

	send_state_info.no_wait = GetWaitMode(context);
}

idx_t PhysicalExchangeOutLocalSinkState::SerializeChunk(DataChunk &src_chunk, const SelectionVector &sel, ulong count,
                                                        bool compress) {
	if (chunk.data.empty()) {
		chunk.InitializeEmpty(src_chunk.GetTypes());
	}
	idx_t old = memory_stream.GetPosition();

	chunk.Slice(src_chunk, sel, count);
	serializer.Begin();
	//\\chunk.SerializeConst(serializer, compress);
	serializer.End();

	return memory_stream.GetPosition() - old;
}

idx_t PhysicalExchangeOutLocalSinkState::SerializeChunk(DataChunk &src_chunk, bool compress) {
	idx_t old = memory_stream.GetPosition();
	D_ASSERT(old == 0);

	if (chunk.data.empty()) {
		chunk.InitializeEmpty(src_chunk.GetTypes());
	}
	chunk.Reference(src_chunk);
	serializer.Begin();
	//\\chunk.SerializeConst(serializer, compress);
	serializer.End();

	return memory_stream.GetPosition() - old;
}

void PhysicalExchangeOutLocalSinkState::Reset() {
	nodes_sel.clear();
	send_state_info.Reset();
	memory_stream.SetPosition(0);
}

unique_ptr<GlobalSinkState> PhysicalExchangeOut::GetGlobalSinkState(ClientContext &context) const {
	bool no_wait = GetWaitMode(context);
	auto state = make_uniq<PhysicalExchangeOutGlobalSinkState>(conn_id, fragment_id, no_wait);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalExchangeOut::GetLocalSinkState(ExecutionContext &context) const {
  return make_uniq<PhysicalExchangeOutLocalSinkState>(*this, context.client);
}

SinkResultType PhysicalExchangeOut::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PhysicalExchangeOutGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PhysicalExchangeOutLocalSinkState>();
	if (lstate.index == DConstants::INVALID_INDEX) {
		lstate.index = gstate.AcquireChannelIndex();
		//DUCK_LOG(INFO) << "local sink state:" << &lstate<< " init assign index:" << lstate.index;
	}
	if (/*unlikely*/(print_chunk)) {
		//DUCK_LOG(INFO) << "PhysicalExchangeOut::Sink chunk: " << chunk.ToString();
	}
	auto res = SendChannelData(conn_id, fragment_id, chunk, &lstate, &input.interrupt_state);

	if (res != SinkResultType::BLOCKED) {
		// duckdb do not add rows for sink, so need do manually here
		OperatorProfiler &profiler = context.thread.profiler;
		auto &info = profiler.GetOperatorInfo(*this);
		info.AddReturnedElements(chunk.size());
	}

	return res;
}

SinkCombineResultType PhysicalExchangeOut::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalExchangeOutLocalSinkState>();
	auto &gstate = input.global_state.Cast<PhysicalExchangeOutGlobalSinkState>();

	// this local state do not attach any channel before( no send data)
	// assign one channel index(exec index) to save profiler info
	if (lstate.index == DConstants::INVALID_INDEX) {
		lstate.index = gstate.AcquireChannelIndex();
	}

	SaveExchangeOutOneProfiler(context, &lstate, fragment_id, parent_exch_in_fragment_id);

	gstate.ReleaseChannelIndex(lstate.index);

	// we send stat in Finalize, so do nothing here
	// MUST send in Finalize because profiler is ready only after all Combine done
	return SinkCombineResultType::FINISHED;
}

// send eof for all channels (last channel send stat)
SinkFinalizeType PhysicalExchangeOut::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	// There exists errors such as exceptions and don't send EOF to coordinator in case it will finish too early.
	if (context.interrupted) {
		return SinkFinalizeType::READY;
	}

	auto &gstate = input.global_state.Cast<PhysicalExchangeOutGlobalSinkState>();

	D_ASSERT(gstate.FreeChannelQueue().size() == gstate.MaxChannelIndex());
	while (gstate.FreeChannelQueue().size()) {
		gstate.send_state_info.index = gstate.FreeChannelQueue().front();

		if (gstate.send_state_info.index == gstate.MaxChannelIndex() - 1) {
			// last channel need send more info, so set context
			gstate.send_state_info.context = &context;
		} else {
			gstate.send_state_info.context = nullptr;
		}

		SinkFinalizeType res =
		    SendChannelFinalize(conn_id, fragment_id, &gstate.send_state_info, &input.interrupt_state);

		if (res == SinkFinalizeType::BLOCKED) {
			//DUCK_LOG(INFO) << "out finalize:" << this << " need block";
			return res;
		}

		gstate.FreeChannelQueue().pop();
	}

	return SinkFinalizeType::READY;
}

string PhysicalExchangeOut::GetName() const {
	return /*std::to_string(operator_id) + */".EXCHANGE_OUT(" + ExchangeTypeToString(exchange_type) + ")";
}


OperatorResultType PhysicalExchangeOut::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate, OperatorState &state_p) const {
	//auto &state = state_p.Cast<PhysicalExchangeOutState>();
	// state.executor.Execute(input, chunk);
	if (input.size() == 0) {
		// If the input chunk from the child is empty,
		// it means the child might be done or needs more time.
		// Signal that we need more input from the child
		return OperatorResultType::NEED_MORE_INPUT;
	}
	switch (exchange_type) {
	case ExchangeType::GATHER:
		chunk.Reference(input);
		break;

	case ExchangeType::BROADCAST:
		chunk.Reference(input);
		break;

	case ExchangeType::SHUFFLE:
		chunk.Reference(input);
		break;
	default:
		throw InternalException("Unknown exchange type :%s", ExchangeTypeToString(exchange_type));
	}


	// LogInfo("Exchange (%s) processed %lld input rows", ExchangeTypeToString(exchange_type).c_str(), input.size());
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalExchangeOut::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalExchangeOutState>();
}

InsertionOrderPreservingMap<string> PhysicalExchangeOut::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	// result["conn_id"] = std::to_string(conn_id);
	result["fragment_id"] = std::to_string(fragment_id);
	result["parent_exch_in_fragment_id"] = std::to_string(parent_exch_in_fragment_id);

	//ref TDStoreOptimizer::TDStoreOptimizerInstance::AllocateExchangeOperatorAsTop
	// result["node_conn_id"] = std::to_string(conn_id >> 32);
	// result["query_id"] = std::to_string(conn_id & 0xffffffff);

	for (auto &expr: part_expressions) {
		result["part_expressions"].append(expr->ToString());
	}
	PhysicalOperator::SetEstimatedCardinality(result, estimated_cardinality);

	return result;
}

//===--------------------------------------------------------------------===//
// Dummy source
//===--------------------------------------------------------------------===//
class PhysicalExchangeOutGlobalSourceState : public GlobalSourceState {
public:
	explicit PhysicalExchangeOutGlobalSourceState() {
	}

	idx_t MaxThreads() override {
		return 1;
	}
};

class PhysicalExchangeOutLocalSourceState : public LocalSourceState {
public:
	PhysicalExchangeOutLocalSourceState() {
	}
};

unique_ptr<GlobalSourceState> PhysicalExchangeOut::GetGlobalSourceState(ClientContext &context) const {
  return make_uniq<PhysicalExchangeOutGlobalSourceState>();
}

unique_ptr<LocalSourceState> PhysicalExchangeOut::GetLocalSourceState(ExecutionContext &,
                                                                      GlobalSourceState &global_state) const {
	return make_uniq<PhysicalExchangeOutLocalSourceState>();
}

SourceResultType PhysicalExchangeOut::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	D_ASSERT(chunk.size() == 0);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
