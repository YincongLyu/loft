file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/src/*.cpp)

add_library(loft SHARED ${SRC})

# Find Threads package
find_package(Threads REQUIRED)

# Link libraries
target_link_libraries(loft Threads::Threads)

# Include directories for bloomstore
target_include_directories(loft
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
