add_executable(revindex revindex.cpp)
set_target_properties(revindex PROPERTIES FOLDER tpie)
target_link_libraries(revindex tpie pipelining)

add_executable(matcher matcher.cpp)
set_target_properties(matcher PROPERTIES FOLDER tpie)
target_link_libraries(matcher tpie pipelining)
