set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
cmake_minimum_required(VERSION 3.14)
project(delta_compress)

add_compile_options(-msse3)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native")

include(FetchContent)

# Build xxHash inside our tree, don’t install it
set(XXHASH_BUNDLED_MODE ON)

FetchContent_Declare(
  xxhash
  GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
  GIT_TAG        v0.8.2
  SOURCE_SUBDIR  cmake_unofficial    
  )

FetchContent_MakeAvailable(xxhash)

add_subdirectory(Gdelta)
include_directories(Gdelta)

add_subdirectory(edelta)
include_directories(edelta/src)
include_directories(edelta/util)

add_executable(delta_compress src/main.cpp 
                   src/chunker.cpp)



add_executable(delta_decode src/main_decode.cpp)
target_link_libraries(delta_decode PRIVATE xxHash::xxhash)
target_link_libraries(delta_compress PRIVATE xxHash::xxhash Gdelta edelta)