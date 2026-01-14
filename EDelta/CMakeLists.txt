

include_directories(.)

add_library(edelta STATIC
edelta.cc htable.cc util.cc 
)

target_link_libraries(edelta PRIVATE xxHash::xxhash )