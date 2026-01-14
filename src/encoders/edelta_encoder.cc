#include "edelta_encoder.h"

uint64_t EDeltaEncoder::encode() {
    EDeltaEncode(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize);
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
}

uint64_t EDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    size_t decoded_size = EDeltaDecode(delta_buf, static_cast<uint32_t>(delta_size), baseBuf,
            static_cast<uint32_t>(baseSize), outputBuf,
            &outputSize);
    return outputSize;
}