

include_directories(.)
include_directories(..)
include_directories(../hypersonic-rle-kit/src)

add_library(fdelta STATIC
fdelta.cc
)



# print cmake dir
target_link_libraries(fdelta
    PRIVATE xxHash::xxhash
    PUBLIC hsrlekit
)
