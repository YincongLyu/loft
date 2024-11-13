file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE MYSQL_SRC ${PROJECT_SOURCE_DIR}/src/sql/*.cpp)
file(GLOB_RECURSE COMMON_SRC ${PROJECT_SOURCE_DIR}/src/common/*.cpp)

# add_library(loft SHARED ${SRC}) 如果暂时没写 test case，就只能编译成静态库！
add_library(loft STATIC ${SRC} ${MYSQL_SRC} ${COMMON_SRC})

# Find Threads package
find_package(Threads REQUIRED)

# Link libraries
target_link_libraries(loft Threads::Threads)

# Include directories for loft
target_include_directories(loft
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
