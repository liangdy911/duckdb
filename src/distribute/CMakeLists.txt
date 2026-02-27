add_library_unity(
  duckdb_distribute
  OBJECT
  fragment.cpp
  fragment_scheduler.cpp
  fragment_tree_generator.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_distribute>
    PARENT_SCOPE)
