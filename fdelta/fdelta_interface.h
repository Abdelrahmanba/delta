#pragma once
#include <cstdint>

uint64_t fencode(unsigned char* inputBuf, uint64_t inputSize,unsigned char* baseBuf,
                 uint64_t baseSize, unsigned char* outputBuf);