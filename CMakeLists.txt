set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
cmake_minimum_required(VERSION 3.14)
project(delta_compress)

add_compile_options(-msse3)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -march=native  -flto")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -flto")

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

add_subdirectory(fdelta)
include_directories(fdelta)

add_subdirectory(xdelta3)
include_directories(xdelta3)

add_subdirectory(EDelta)
include_directories(EDelta)

add_subdirectory(Ddelta)
include_directories(Ddelta)

add_subdirectory(zdelta)
include_directories(zdelta)

include_directories(src)

add_executable(delta_compress
                    src/main.cpp
                    src/encoders/xdelta_encoder.cc
                    src/encoders/fdelta_encoder.cc
                    src/encoders/gdelta_encoder.cc
                    src/encoders/edelta_encoder.cc
                    src/encoders/zdelta_encoder.cc
                    src/encoders/ddelta_encoder.cc
                    src/lz4/lz4.c)



add_executable(delta_decode src/main_decode.cpp)
target_link_libraries(delta_decode PRIVATE xxHash::xxhash )
target_link_libraries(delta_compress PRIVATE xxHash::xxhash Gdelta fdelta xdelta3 edelta ddelta zdelta)
