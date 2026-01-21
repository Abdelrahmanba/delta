#pragma once
#include <cstdint>

uint64_t fencode(unsigned char* inputBuf, uint64_t inputSize,unsigned char* baseBuf,
                 uint64_t baseSize, unsigned char* outputBuf);

uint64_t fdecode(unsigned char* deltaBuf, uint64_t deltaSize,unsigned char* baseBuf,
                uint64_t baseSize, unsigned char* outputBuf);