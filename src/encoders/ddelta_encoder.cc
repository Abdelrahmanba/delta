#include "ddelta_encoder.h"

uint64_t DDeltaEncoder::encode() {
    DDeltaEncode(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize);
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
}

uint64_t DDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    size_t decoded_size = DDeltaDecode(delta_buf, static_cast<uint32_t>(delta_size), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize);
    return outputSize;
}