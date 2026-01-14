#include "xdelta_encoder.h"

uint64_t XDeltaEncoder::encode() {
    xd3_encode_memory(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize, 64 * 1024,0);
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
}

uint64_t XDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    size_t decoded_size = xd3_decode_memory(delta_buf, static_cast<uint32_t>(delta_size), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize, 64 * 1024, 0);
    return outputSize;
}