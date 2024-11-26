cmake_minimum_required(VERSION 3.18)

# -- Configure project compiler options
project(loft LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)

# -- CTest
enable_testing()

# -- CMake compile options
option(NDEBUG ON) # for debug
option(LOFT_TESTING "Build unit tests" YES) # for test

# -- Manage Compile Options w/ ASAN flag
if(NDEBUG)
    add_compile_options(
        -O3 -Werror -Wno-attributes
            -DNDEBUG=1
        -DPROJECT_VERSION=""
        -DEVMONE_X86_64_ARCH_LEVEL=2
    )
else()
#    add_compile_options(-fsanitize=address)
#    add_link_options(-fsanitize=address)
    add_compile_options(
        -O0 -ggdb -Werror -Wno-attributes
        -DPROJECT_VERSION=""
        -DEVMONE_X86_64_ARCH_LEVEL=2
    )
endif()

# -- Output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# -- Includes
include_directories(${PROJECT_SOURCE_DIR}/include)
#include_directories(${PROJECT_SOURCE_DIR}/deps/date/include)
#INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/deps /usr/local/common/include/deps)

# -- third Libraries
add_subdirectory(deps)

# Add subdirectory for source
add_subdirectory(src)

add_subdirectory(bin)

# Manage testing option
if(LOFT_TESTING)
    add_subdirectory(test)
endif()
