

include_directories(.)
include_directories(..)

# xxHash::xxhash

add_library(fdelta STATIC
fdelta.cc
)
target_link_libraries(fdelta PRIVATE xxHash::xxhash)