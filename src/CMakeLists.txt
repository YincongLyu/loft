file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE MYSQL_SRC ${PROJECT_SOURCE_DIR}/src/sql/*.cpp)

# add_library(loft SHARED ${SRC}) 如果暂时没写 test case，就只能编译成静态库！
add_library(loft STATIC ${SRC} ${MYSQL_SRC})

# Find Threads package
find_package(Threads REQUIRED)

# Link libraries
target_link_libraries(loft Threads::Threads)

# Include directories for bloomstore
target_include_directories(loft
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${RAPIDJSON_INCLUDE_DIR}
)
