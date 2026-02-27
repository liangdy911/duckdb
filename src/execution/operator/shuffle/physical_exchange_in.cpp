//
// Created by ericqu on 2025/4/9.
//
#include "duckdb/execution/operator/shuffle/physical_exchange_in.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/distribute/duckdb_fragment_stub.hpp"
#include "duckdb/common/log.hpp"
#include "duckdb/parallel/interrupt.hpp"

extern std::string global_tdsql_rpc_addr;

namespace duckdb {

PhysicalExchangeIn::PhysicalExchangeIn(ExchangeType exchange_type, vector<LogicalType> types,
                                       idx_t estimated_cardinality, ulong conn_id, FragmentID fragment_id,
                                       FragmentID child_exch_out_fragment_id)
    : PhysicalOperator(PhysicalOperatorType::EXCHANGE_IN, std::move(types), estimated_cardinality),
      exchange_type(exchange_type), conn_id(conn_id), fragment_id(fragment_id),
      child_exch_out_fragment_id(child_exch_out_fragment_id) {
}

PhysicalExchangeIn::PhysicalExchangeIn(ExchangeType exchange_type, vector<unique_ptr<Expression>> part_expressions,
                                   vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::EXCHANGE_IN, std::move(types), estimated_cardinality),
      exchange_type(exchange_type) {
	for (auto &expr : part_expressions) {
		this->part_expressions.push_back(std::move(expr));
	}
}

string PhysicalExchangeIn::GetName() const {
	return /*std::to_string(operator_id) + */".EXCHANGE_IN(" + ExchangeTypeToString(exchange_type) + ")";
}

PhysicalExchangeInGlobalScanState::PhysicalExchangeInGlobalScanState(ulong conn_id, FragmentID fragment_id,
                                                                     FragmentID child_exch_out_fragment_id)
    : conn_id(conn_id), fragment_id(fragment_id), child_exch_out_fragment_id(child_exch_out_fragment_id),
      exec_index(0) {
	channel_num = GetChannelSize(conn_id, fragment_id, child_exch_out_fragment_id);
	//DUCK_LOG(INFO) << "PhysicalExchangeInGlobalScanState channel_num:" << channel_num;
}

PhysicalExchangeInLocalScanState::PhysicalExchangeInLocalScanState(GlobalSourceState &global_state, bool no_wait,
                                                                   bool combine)
    : gstate(global_state.Cast<PhysicalExchangeInGlobalScanState>()), combine_chunk(combine), no_wait(no_wait) {
	exec_index = gstate.NextExecIndex();
	if (DUCKDB_UNLIKELY(gstate.channel_index_sets.find(exec_index) == gstate.channel_index_sets.cend())) {
		throw InternalException("Can not find channel_indexes for specified exec_index");
	}
	channel_indexes = gstate.channel_index_sets[exec_index];
	//DUCK_LOG(INFO) << "local state:" << this << " init assign exec_index:" << exec_index << " no_wait:" << no_wait;
	//for (auto &idx : channel_indexes) {
		//DUCK_LOG(INFO) << "local state:" << this << " init assign index:" << idx;
	//}

	//channel_signal_state = std::make_shared<ChannelSignalState>();
}

// get data from brpc channel, continue with other channel if current channel is finished
// if no_wait == false, this func may block, so make sure there are enough active bthread to do other things
//
// if no_wait == true, this func never block, return SourceResultType::BLOCKED when data is not ready
//    interrupt_state->Callback() will triggered when data is ready
SourceResultType PhysicalExchangeInLocalScanState::LocalStateReadChannelData(ExecutionContext &execution_context,
                                                                             DataChunk &chunk,
                                                                             InterruptState *interrupt_state) {
	if (channel_indexes.empty()) {
		//DUCK_LOG(INFO) << "local state:" << this << " finish get_data";
		return SourceResultType::FINISHED;
	}
	//interrupt_state->SetChannelSignalState(channel_signal_state);

	uint block_num = 0;

	while (1) {
		if (block_num >= channel_indexes.size()) {
			// before return block, need check again, channel i may has data when we check channel j
			//if (interrupt_state->NeedBlock()) {
				//DUCK_LOG(INFO) << "local state:" << this << " all channel block, return block";
			//	return SourceResultType::BLOCKED;
			//}

			//DUCK_LOG(INFO) << "local state:" << this << " some channel has data, read again";
			block_num = 0;
			continue;
		}

		index = channel_indexes.front();
		//DUCK_LOG(INFO) << "local state:" << this << " begin read data from channel:" << index;
		SourceResultType res = ReadChannelData(this, execution_context, chunk, interrupt_state);
		if (chunk.size() != 0) {
			//DUCK_LOG(INFO) << "local state:" << this << " read data success, chunk size:" << chunk.size();
			//interrupt_state->ResetChannelSignalState();
			return SourceResultType::HAVE_MORE_OUTPUT;
		}

		// finish with current channel, read from another channel
		if (res == SourceResultType::FINISHED) {
			channel_indexes.pop_front();
			if (channel_indexes.empty()) {
				//DUCK_LOG(INFO) << "local state:" << this << " finish get_data";
				//interrupt_state->ResetChannelSignalState();
				return SourceResultType::FINISHED;
			}
			continue;
		}

		// no data for current channel, try another channel
		D_ASSERT(res == SourceResultType::BLOCKED);
		//DUCK_LOG(INFO) << "local state:" << this << " current index block, try another channel:" << index;
		channel_indexes.pop_front();
		channel_indexes.push_back(index);
		block_num++;
		continue;
	}
}

// init all channels with PhysicalExchangeOut
unique_ptr<GlobalSourceState> PhysicalExchangeIn::GetGlobalSourceState(ClientContext &context) const {
	auto gstate = make_uniq<PhysicalExchangeInGlobalScanState>(conn_id, fragment_id, child_exch_out_fragment_id);
	return gstate;
}

unique_ptr<LocalSourceState> PhysicalExchangeIn::GetLocalSourceState(ExecutionContext &context,
                                                                     GlobalSourceState &global_state) const {
	bool no_wait = false;//GetWaitMode(context.client);
	bool combine = false;//GetCombineInMode(context.client);
	return make_uniq<PhysicalExchangeInLocalScanState>(global_state, no_wait, combine);
}

// dst += other
void CombineChunk(DataChunk &dst, PhysicalExchangeInLocalScanState &lstate) {
	DataChunk &other = *lstate.remain_chunk;
	if (dst.size() == 0) {
		//dst.Swap(other);
		return;
	}

	//lstate.profiler.combine_num++;
	dst.Flatten();
	dst.Append(other);
	other.Reset();
}

// Return as much data as possible in one GetDataCombine call. Combine the data from multiple calls to GetData into one.
SourceResultType PhysicalExchangeIn::GetDataCombine(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalExchangeInLocalScanState>();
	if (!(lstate.remain_chunk)) {
		lstate.remain_chunk = std::make_unique<DataChunk>();
		lstate.remain_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	} else if (lstate.finished) {
		return SourceResultType::FINISHED;
	}

	// use remain_chunk as first chunk
	if (lstate.remain_chunk->size()) {
		//chunk.Swap(*lstate.remain_chunk);
	}

	SourceResultType res;
	while (1) {
		res = lstate.LocalStateReadChannelData(context, *lstate.remain_chunk, &input.interrupt_state);

		if (res == SourceResultType::HAVE_MORE_OUTPUT) {
			// get next chunk success, but can not combine
			// next call will use this chunk as first chunk
			if (chunk.size() + lstate.remain_chunk->size() > STANDARD_VECTOR_SIZE) {
				return SourceResultType::HAVE_MORE_OUTPUT;
			} else {
				CombineChunk(chunk, lstate);
				continue;
			}
		} else if (res == SourceResultType::FINISHED) {
			if (chunk.size()) {
				// get next chunk return finish
				// set finished, next call will return SourceResultType::FINISHED
				lstate.finished = true;
				return SourceResultType::HAVE_MORE_OUTPUT;
			} else {
				// get first chunk return finish, just return finish
				return SourceResultType::FINISHED;
			}
		} else {
			D_ASSERT(res == SourceResultType::BLOCKED);
			// get next chunk return block, need save in remain_chunk
			// caller ALWAYS Reset chunk
			if (chunk.size()) {
				//chunk.Swap(*lstate.remain_chunk);
			}
			return res;
		}
	}

	D_ASSERT(0);
	return res;
}

SourceResultType PhysicalExchangeIn::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalExchangeInLocalScanState>();

	SourceResultType res;
	if (!(lstate.combine_chunk)) {
		res = lstate.LocalStateReadChannelData(context, chunk, &input.interrupt_state);
	} else {
		res = GetDataCombine(context, chunk, input);
	}

  if (res == SourceResultType::FINISHED) {
	  // current execution is finished, time for save profiler
		SaveExchangeInOneProfiler(context, &lstate, fragment_id, child_exch_out_fragment_id);
  }

  return res;
}

InsertionOrderPreservingMap<string> PhysicalExchangeIn::ParamsToString() const{
	InsertionOrderPreservingMap<string> result;
	// result["conn_id"] = std::to_string(conn_id);
	result["fragment_id"] = std::to_string(fragment_id);
	result["child_exch_out_fragment_id"] = std::to_string(child_exch_out_fragment_id);

	//ref TDStoreOptimizer::TDStoreOptimizerInstance::AllocateExchangeOperatorAsTop
	// result["node_conn_id"] = std::to_string(conn_id >> 32);
	// result["query_id"] = std::to_string(conn_id & 0xffffffff);
	PhysicalOperator::SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

// all we need is get data from PhysicalExchangeOut
// no need to add operator or child
void PhysicalExchangeIn::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
}

void PhysicalExchangeIn::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state_p) const {
//auto &state = state_p->Cast<PhysicalExchangeInState>();

	// \u6839\u636eExchange\u7c7b\u578b\u6267\u884c\u76f8\u5e94\u7684\u5904\u7406
	switch (exchange_type) {
	case ExchangeType::GATHER:
		// \u4ece\u5b50\u7b97\u5b50\u83b7\u53d6\u6570\u636e
		// children[0]->GetChunk(context, chunk, state.child_states[0].get());
		// \u5728\u5b9e\u9645\u5206\u5e03\u5f0f\u73af\u5883\u4e2d\uff0c\u8fd9\u91cc\u5e94\u8be5\u4ece\u591a\u4e2a\u8282\u70b9\u6536\u96c6\u6570\u636e
		break;

	case ExchangeType::BROADCAST:
		// \u4ece\u5b50\u7b97\u5b50\u83b7\u53d6\u6570\u636e
		// children[0]->GetChunk(context, chunk, state.child_states[0].get());
		// \u5728\u5b9e\u9645\u5206\u5e03\u5f0f\u73af\u5883\u4e2d\uff0c\u8fd9\u91cc\u5e94\u8be5\u5411\u591a\u4e2a\u8282\u70b9\u53d1\u9001\u76f8\u540c\u7684\u6570\u636e
		break;

	case ExchangeType::SHUFFLE:
		// \u4ece\u5b50\u7b97\u5b50\u83b7\u53d6\u6570\u636e
		// children[0]->GetChunk(context, chunk, state.child_states[0].get());
		// if (chunk.size() == 0)
		//	return;

		// \u5728\u5b9e\u9645\u5206\u5e03\u5f0f\u73af\u5883\u4e2d\uff0c\u8fd9\u91cc\u5e94\u8be5\u6839\u636epart_expressions\u91cd\u65b0\u5206\u533a\u6570\u636e
		// \u73b0\u5728\u53ea\u662f\u57fa\u672c\u5b9e\u73b0
		break;

	case ExchangeType::NONE:
	default:
		D_ASSERT(0);
		break;
	}
}

OperatorResultType PhysicalExchangeIn::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state_p) const {

	//auto &state = state_p.Cast<PhysicalExchangeInState>();
	// state.executor.Execute(input, chunk);
	if (input.size() == 0) {
		// If the input chunk from the child is empty,
		// it means the child might be done or needs more time.
		// Signal that we need more input from the child
		return OperatorResultType::NEED_MORE_INPUT;
	}
	switch (exchange_type) {
	case ExchangeType::GATHER:
		// GATHER \u7c7b\u578b\u9700\u8981\u6536\u96c6\u6570\u636e\u5e76\u4f20\u9012
		chunk.Reference(input);
		break;

	case ExchangeType::BROADCAST:
		// BROADCAST \u7c7b\u578b\u5c06\u6570\u636e\u5e7f\u64ad\u5230\u6240\u6709\u8282\u70b9
		chunk.Reference(input);
		break;

	case ExchangeType::SHUFFLE:
		// SHUFFLE \u7c7b\u578b\u6839\u636e\u8868\u8fbe\u5f0f\u91cd\u65b0\u5206\u533a\u6570\u636e
		// \u5728\u5355\u8282\u70b9\u6a21\u62df\u65f6\uff0c\u6211\u4eec\u53ea\u662f\u4f20\u9012\u6570\u636e
		chunk.Reference(input);
		break;

	case ExchangeType::NONE:
	default:
		D_ASSERT(0);
		break;
	}


	// LogInfo("Exchange (%s) processed %lld input rows", ExchangeTypeToString(exchange_type).c_str(), input.size());
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalExchangeIn::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalExchangeInState>();
}


} // namespace duckdb
