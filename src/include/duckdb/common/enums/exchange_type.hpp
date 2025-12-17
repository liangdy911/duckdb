//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/exchange_type.hpp
//
// Created by ericqu on 2025/4/24.
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ExchangeType : uint32_t {
	// Collects data from multiple nodes to a single node(the coordinator)
	GATHER = 0,
	// Broadcasts data from one node to multiple nodes
	BROADCAST,
	// Hash shuffles data based on the given expressions
	SHUFFLE,
	NONE = 255
};

inline string ExchangeTypeToString(ExchangeType type) {
	switch (type) {
	case ExchangeType::GATHER:
		return "GATHER";
	case ExchangeType::BROADCAST:
		return "BROADCAST";
	case ExchangeType::SHUFFLE:
		return "HASH_SHUFFLE";
	case ExchangeType::NONE:
		return "NONE";
	}
	return "NONE";
}

} // namespace duckdb
