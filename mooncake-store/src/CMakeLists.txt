cmake_minimum_required(VERSION 3.12)

# Find Python package
find_package(Python3 REQUIRED COMPONENTS Development)
find_package(pybind11 REQUIRED)

file(GLOB SOURCES "*.cpp")
add_subdirectory(cachelib_memory_allocator)

add_library(cache_allocator ${SOURCES})
target_link_libraries(cache_allocator PUBLIC transfer_engine glog gflags)
# Add Python include directories to cache_allocator
target_include_directories(cache_allocator PRIVATE ${Python3_INCLUDE_DIRS})

# Create the Python module without SOABI suffix
pybind11_add_module(distributed_object_store ${SOURCES})
set_target_properties(distributed_object_store PROPERTIES NO_SONAME ON)
target_link_libraries(distributed_object_store PRIVATE cache_allocator cachelib_memory_allocator etcd-cpp-api-core)

# Include Python headers
target_include_directories(distributed_object_store PRIVATE ${Python3_INCLUDE_DIRS})

install(TARGETS distributed_object_store DESTINATION .)
