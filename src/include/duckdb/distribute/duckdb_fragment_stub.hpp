#pragma once

// DuckDB can be compiled in two modes: if AS_MYSQL_SUBPROJECT is defined, DuckDB will be linked with the SQLEngine
// and use functions provided by SQLEngine. If compiled standalone, some stub functions are provided to ensure
// successful compilation in both modes.
//

#ifdef AS_MYSQL_SUBPROJECT
#include "storage/cstore/duckdb/extension/include/tdstore_fragment.hpp"
#else
#include "fragment.h"
#include "duckdb/common/unique_ptr.hpp"
namespace duckdb {

class PhysicalExchangeInLocalScanState;
class FragmentPlan;

// below functions should not called if not define DUCKDB_UNDER_SQLENGINE
bool GlobalSetupChannels(ClientContext &context, FragmentInfo &fragment) {
	D_ASSERT(0);
	return false;
}
ulong GetChannelSize(ulong conn_id, FragmentID fragment_id, FragmentID child_exch_out_fragment_id) {
	D_ASSERT(0);
	return 0;
}

SourceResultType ReadChannelData(PhysicalExchangeInLocalScanState *lstate, ExecutionContext &execution_context,
                                 DataChunk &chunk, InterruptState *interrupt_state) {
	D_ASSERT(0);
	return SourceResultType::FINISHED;
}

SinkResultType SendChannelData(ulong conn_id, FragmentID fragment_id, DataChunk &chunk,
                               PhysicalExchangeOutLocalSinkState *lstate, InterruptState *interrupt_state) {
	D_ASSERT(0);
	return SinkResultType::FINISHED;
}

SinkCombineResultType SendChannelCombine(ulong conn_id, FragmentID fragment_id, SendStateInfo *send_state_info,
                                         InterruptState *interrupt_state) {
	D_ASSERT(0);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType SendChannelFinalize(ulong conn_id, FragmentID fragment_id, SendStateInfo *send_state_info,
                                     InterruptState *interrupt_state) {
	D_ASSERT(0);
	return SinkFinalizeType::READY;
}

void ComputeFragmentParams(ClientContext &context, unique_ptr<FragmentPlan> &frag_plan) {
	D_ASSERT(0);
}

void ComputePlanRepGroupMeta(ClientContext &context, LogicalOperator *op) {
	D_ASSERT(0);
}

bool GlobalSendAllFragment(ClientContext &context, FragmentInfo &fragment) {
	D_ASSERT(0);
	return false;
}

ulong GetMaxChannelIndex(ulong conn_id, FragmentID fragment_id) {
	D_ASSERT(0);
	return 0;
}
bool GetWaitMode(ClientContext &context) {
	D_ASSERT(0);
	return false;
}

bool ExecutePlan(ClientContext &context, FragmentInfo &fragment) {
	D_ASSERT(0);
  return 0;
}
bool CleanContext(ClientContext &context, FragmentInfo &fragment) {
	D_ASSERT(0);
  return 0;
}

void SaveExchangeOutOneProfiler(ExecutionContext &execution_context, PhysicalExchangeOutLocalSinkState *lstate,
                                ulong fragment_id, ulong parent_exch_in_fragment_id) {
	D_ASSERT(0);
}

void SaveExchangeInOneProfiler(ExecutionContext &execution_context, PhysicalExchangeInLocalScanState *lstate,
                               ulong fragment_id, ulong child_exch_out_fragment_id) {
	D_ASSERT(0);
}

} // namespace duckdb
#endif
