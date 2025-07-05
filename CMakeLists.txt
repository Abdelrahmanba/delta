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

add_executable(delta_compress src/main.cpp 
                   src/chunker.cpp)

target_link_libraries(delta_compress PRIVATE xxHash::xxhash)