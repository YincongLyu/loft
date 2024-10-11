# 将 data 文件复制到构建目录
configure_file(${CMAKE_SOURCE_DIR}/data ${CMAKE_BINARY_DIR}/test/data COPYONLY)

include(GoogleTest)

file(GLOB_RECURSE SRC_TEST ./*.cpp)

foreach(F ${SRC_TEST}) # unit tests
    file(RELATIVE_PATH R ${CMAKE_CURRENT_SOURCE_DIR} ${F})
    string(REPLACE ".cpp" "" R ${R})
    message(STATUS "+ " ${F})
    add_executable(${R} ${F})

    # Link with gtest and bloomstore
    target_link_libraries(${R} gtest gtest_main bloomstore)

    set_target_properties(${R}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test
        COMMAND ${R}
    )
endforeach()

# Enable CTest for unit tests
gtest_discover_tests(${R})
