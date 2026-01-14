#include "zdelta_encoder.h"

uint64_t ZDeltaEncoder::encode() {
    uLongf delta_size = static_cast<uLongf>(MAX_CHUNK_SIZE);
    int status = zd_compress(baseBuf, static_cast<uLong>(baseSize), inputBuf,
                             static_cast<uLong>(inputSize), outputBuf,
                             &delta_size);
    if (status != ZD_OK) {
        std::cerr << "ZDeltaEncoder::encode() failed: " << status << "\n";
        return 0;
    }
    outputSize = delta_size;
    return outputSize;
}

uint64_t ZDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    uLongf target_size = static_cast<uLongf>(inputSize);
    int status = zd_uncompress(baseBuf, static_cast<uLong>(baseSize), outputBuf,
                               &target_size, delta_buf,
                               static_cast<uLong>(delta_size));
    if (status != ZD_OK) {
        std::cerr << "ZDeltaEncoder::decode() failed: " << status << "\n";
        return 0;
    }
    outputSize = target_size;
    return outputSize;
}
