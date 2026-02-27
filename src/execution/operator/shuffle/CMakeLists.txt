add_library_unity(duckdb_operator_exchange
        OBJECT
        physical_exchange_out.cpp
        physical_exchange_in.cpp)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES}
        $<TARGET_OBJECTS:duckdb_operator_exchange>
        PARENT_SCOPE)