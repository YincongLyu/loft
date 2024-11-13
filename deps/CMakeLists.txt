# 自动检测项目中的 test 目录下的测试文件
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(googletest)
#add_subdirectory(common)