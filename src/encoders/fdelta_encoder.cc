#include "fdelta_encoder.h"


uint64_t FDeltaEncoder::encode() {
    return fencode(inputBuf, static_cast<uint64_t>(inputSize), baseBuf,
                   static_cast<uint64_t>(baseSize), outputBuf);

}
