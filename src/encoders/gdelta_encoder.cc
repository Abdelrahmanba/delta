#include "gdelta_encoder.h"

uint8_t* lz4Bufferr = new uint8_t[1024*64];  // 64KB buffer for LZ4 compression

uint64_t GDeltaEncoder::encode() {
    gencode(inputBuf, static_cast<uint32_t>(inputSize), baseBuf,
            static_cast<uint32_t>(baseSize), &outputBuf,
            reinterpret_cast<uint32_t*>(&outputSize));
    std::cout << "inputSize: " << inputSize << ", baseSize: " << baseSize
              << ", outputSize: " << outputSize << "\n";
    return outputSize;
    // uint64_t compressedSize = LZ4_compress_fast(
    //     reinterpret_cast<const char*>(outputBuf),
    //     reinterpret_cast<char*>(lz4Bufferr), static_cast<int>(outputSize),
    //     static_cast<int>(1024 * 64), 1);

    // return compressedSize;
}

uint64_t GDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    size_t decoded_size = gdecode(delta_buf, static_cast<uint32_t>(delta_size), baseBuf,
            static_cast<uint32_t>(baseSize), &outputBuf,
            reinterpret_cast<uint32_t*>(&outputSize));
    return decoded_size;
}