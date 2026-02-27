#pragma once

#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

class FragmentTree;

class FragmentScheduler /* : public TaskScheduler*/ {

public:
	explicit FragmentScheduler(DatabaseInstance &db) : m_db(db) {
	}

  ~FragmentScheduler() {
  }

  static FragmentScheduler &GetScheduler(ClientContext &context);

  void ExecuteInThread(FragmentTree *frag_tree, ClientContext *context);
  void ExecuteFinalize(FragmentTree *frag_tree, ClientContext *context);

  // void ExecuteForever(atomic<bool> *marker) /*override*/;
  // idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) /*override*/;
  // void ExecuteTasks(idx_t max_tasks) /*override*/;

  private:
  void CleanAllContexts(FragmentTree *frag_tree, ClientContext *context);
  void ExecuteInCoordinator(FragmentTree *frag_tree, ClientContext *context);
  void ExecuteInWorker(FragmentTree *frag_tree, ClientContext *context);

  DatabaseInstance &m_db;
};
}
