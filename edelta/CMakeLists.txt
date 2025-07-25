aux_source_directory(src/ SRC)
aux_source_directory(util/ UTIL)

include_directories(.)
include_directories(./src)
include_directories(./util)


add_library(edelta ${SRC} ${UTIL})
target_link_libraries(edelta PRIVATE xxHash::xxhash)