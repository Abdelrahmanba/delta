

#include <chrono>
#include <iostream>
#include <map>
#include "chunker.hpp"
Chunker::Chunker() {
    minChunkSize = 256;
    maxChunkSize = 20000;
    avgChunkSize = 1024;
}



size_t Chunker::nextChunk(const unsigned char *readBuffer, size_t buffBegin,
                          size_t buffEnd) {
    return 0;
}

Chunker::~Chunker() {}