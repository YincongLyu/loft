include(GoogleTest)

file(GLOB_RECURSE SRC_TEST ./*.cpp)

foreach(F ${SRC_TEST}) # unit tests
    file(RELATIVE_PATH R ${CMAKE_CURRENT_SOURCE_DIR} ${F})
    string(REPLACE ".cpp" "" R ${R})
    message(STATUS "+ " ${F})
    add_executable(${R} ${F})

    # Link with gtest and loft
    target_link_libraries(${R} gtest gtest_main loft)

    set_target_properties(${R}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test
        COMMAND ${R}
    )

    # Enable CTest for unit tests
    gtest_discover_tests(${R})
endforeach()

