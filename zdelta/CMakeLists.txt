include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(zdelta STATIC
    adler32.c
    deflate.c
    infblock.c
    infcodes.c
    inffast.c
    inflate.c
    inftrees.c
    infutil.c
    zd_mem.c
    trees.c
    zdelta.c
    zutil.c
)
