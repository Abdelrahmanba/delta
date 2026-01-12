#include "gdelta_encoder.h"

uint64_t GDeltaEncoder::encode() {
    gencode(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), &outputBuf,
            reinterpret_cast<uint32_t*>(&outputSize));
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
}

uint64_t GDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    size_t decoded_size = gdecode(delta_buf, static_cast<uint32_t>(delta_size), baseBuf,
            static_cast<uint32_t>(baseSize), &outputBuf,
            reinterpret_cast<uint32_t*>(&outputSize));
    return decoded_size;
}