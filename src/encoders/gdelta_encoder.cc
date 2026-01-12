#include "gdelta_encoder.h"

uint64_t GDeltaEncoder::encode() {
    gencode(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), &outputBuf,
            reinterpret_cast<uint32_t*>(&outputSize));
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
}
