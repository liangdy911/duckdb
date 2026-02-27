#pragma once
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <deque>

#include "duckdb/common/constants.hpp"

#ifdef AS_MYSQL_SUBPROJECT
#include "my_bitmap.h"
#else
struct MY_BITMAP {};
#endif

namespace tdsql {
class HyperNode;
}

// should only contain base struct
// sqlengine and duckdb all may include this header
namespace duckdb {

class FragmentExecInfo {
public:
	FragmentExecInfo(const std::shared_ptr<tdsql::HyperNode> &node) : placement_node(node) {
	}

	void SetResult(int code, const std::string &msg = "") {
		//result_code_.emplace(code);
		result_code_ = code;
		result_msg_ = msg;
	}

	bool CheckWithAck() const {
		//return result_code_.has_value();
		return result_code_ != -1;
	}

	struct RepGroupMeta {
		uint64_t rep_group_id;
		int64_t meta_version;
		int64_t member_version;
		int64_t key_range_version;
		int64_t key_range_shrink_version;
	};
	// FragmentInfo is in logical-fragment level.
	// FragmentExecInfo is in physical-fragment level.
	// A fragment may access multiple tables and it's why table_meta_info is a mapping.
	// A physical fragment may access multiple partitions of a partitioned table and it's why TableMetaInfo has
	// read_partitions. TableMetaInfo::rep_group_meta indicates the data-obj-id of these partitions and their
	// rep-group-meta.
	class TableMetaInfo {
	public:
		~TableMetaInfo();
		// Used by partition table.
		MY_BITMAP read_partitions;
		// Mapping key : table's tindex_id for non-partition table.
		//               physical partition's tindex_id for partition table.
		// Mapped value: RGs of the non-partition table or physical partition.
		// Why deque ? A data obj has multiple RGs.
		std::unordered_map<uint32_t, std::deque<RepGroupMeta>> rep_group_meta;
	};

	// Mapping key: table's tindex_id.
	std::unordered_map<uint32_t, TableMetaInfo> table_meta_info;
	std::shared_ptr<tdsql::HyperNode> placement_node;

private:
	//std::optional<int> result_code_;
	int result_code_ = -1;
	std::string result_msg_;
};

typedef std::unordered_set<std::shared_ptr<tdsql::HyperNode>> HyperNodeSet;

class FragmentInfo {
public:
	ulong conn_id_{UINT64_MAX};
	FragmentID fragment_id_{DConstants::INVALID_INDEX};

	// Where this fragment is dispatched to. A fragment may be dispatched to multiple ve nodes to execute.
	// Mapping key is the id of the ve node to which this fragment is dispatched.
	// Mapping value is this fragment's exec info in the ve node.
	std::unordered_map<uint64_t, FragmentExecInfo> placement_and_exec_info;

	// Where this fragment sends data to.
	// Mapping key is the id of the fragment to which the result data are sent.
	// Mapping value is the ve nodes of the parent fragment.
	// Note: A fragment can only have one single parent fragment, so exch_out_nodes.size() == 1, using an unordered_map
	// is for impl convenience. A logical fragment may be dispatched to multiple ve nodes, that's why the mapping value
	// is a set of ve nodes. If exchange-out is GATHER, it sends to one node (where the coordinator locates). If
	// exchange-out is SHUFFLE or BROADCAST, it sends to multiple nodes.
	std::unordered_map<FragmentID, HyperNodeSet> exch_out_nodes;

	// Where this fragment reads data from.
	// Mapping key is the id of the fragments from which the source data are read.
	// Mapping value is the ve nodes of the children fragments.
	// Note: A fragment may have multiple exchange-in operators.
	// For each exchange-in, it may read from multiple nodes.
	std::unordered_map<FragmentID, HyperNodeSet> exch_in_nodes;

	ulong exchange_out_dop_;

	std::vector<unsigned char> plan_data;

	std::string ToString() const;

	bool IsRoot() const;
};

}
