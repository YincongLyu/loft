file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/bin/*.cpp)

message(STATUS "EXECUTABLES")
foreach(F ${SRC})
    message(STATUS "+ " ${F})
    # 获取文件名，不包含路径和扩展名
    get_filename_component(R ${F} NAME_WE)
    # 添加可执行文件
    add_executable(${R} ${F})
    target_link_libraries(${R} PRIVATE loft)
endforeach()

unset(SRC)
