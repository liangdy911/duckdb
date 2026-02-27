#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DistributeRole : uint8_t { UNKNNON = 0, COORDINATOR, WORKER };

enum class DistributeCoordinatorStatusType : uint8_t { WAIT_FOR_PLAN = 0, READY, ERROR, COMPLETE };

enum class DistributeWorkerStatusType : uint8_t { WAIT_FOR_PLAN = 0, READY, ERROR, PROCESSING, SEND_DATA, COMPLETE };

} // namespace duckdb
