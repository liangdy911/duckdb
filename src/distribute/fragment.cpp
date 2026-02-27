#include "duckdb/distribute/fragment.h"

#include "duckdb/distribute/fragment_tree_generator.hpp"

#ifdef AS_MYSQL_SUBPROJECT
#include "sql/tdsql/route/node.h"
#else
namespace tdsql {
struct HyperNode {
	std::string node_rpc_addr;
};
} // namespace tdsql
#endif

namespace duckdb {

FragmentExecInfo::TableMetaInfo::~TableMetaInfo() {
#ifdef AS_MYSQL_SUBPROJECT
	bitmap_free(&read_partitions);
#endif
}

void PrintRPCNodes(std::stringstream &ss, const std::unordered_map<uint64_t, FragmentExecInfo> &rpc_nodes) {
	bool first_elem = true;
	for (const auto &elem : rpc_nodes) {
		if (first_elem) {
			first_elem = false;
		} else {
			ss << ",";
		}
		ss << elem.second.placement_node->node_rpc_addr;
	}
}

void PrintRPCNodes(std::stringstream &ss, const std::unordered_map<FragmentID, HyperNodeSet> &rpc_nodes) {
	/*bool first_elem = true;
	for (const auto &[_, nodes] : rpc_nodes) {
		for (const auto &node : nodes) {
			if (first_elem) {
				first_elem = false;
			} else {
				ss << ",";
			}
			ss << node->node_rpc_addr;
		}
	}*/
}

std::string FragmentInfo::ToString() const {
	std::stringstream ss;
	ss << "conn_id=" << conn_id_ << ",fragment_id=" << fragment_id_ << ",";

	ss << "placement=[";
	PrintRPCNodes(ss, placement_and_exec_info);
	ss << "],in=[";
	PrintRPCNodes(ss, exch_in_nodes);
	ss << "],out=[";
	PrintRPCNodes(ss, exch_out_nodes);
	ss << "]";

	return ss.str();
}

bool FragmentInfo::IsRoot() const {
	return fragment_id_ == ROOT_FRAGMENT_ID;
}

} // namespace duckdb
