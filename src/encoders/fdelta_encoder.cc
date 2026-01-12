#include "fdelta_encoder.h"


uint64_t FDeltaEncoder::encode() {
    return fencode(inputBuf, static_cast<uint64_t>(inputSize), baseBuf,
                   static_cast<uint64_t>(baseSize), outputBuf);

}
uint64_t FDeltaEncoder::decode(uint8_t* delta_buf, uint64_t delta_size) {
    // FDelta decoding not implemented yet
    std::cerr << "FDeltaDecoder::decode() not implemented.\n";
    return 0;
}