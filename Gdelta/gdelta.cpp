#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <algorithm>
using namespace std;

#include "gdelta.h"
#include "gear_matrix.h"
//#include "jemalloc/jemalloc.h"

// Global delta buffer pointer for VCDIFF encoding
static char* deltaPtr = nullptr;
static uint32_t baseOff;

// ---------- minimal vcdiff helpers -------------------------------
inline void writeVarint(uint32_t v)  // <-- no return value
{
    while (v >= 0x80) {
        *deltaPtr++ = char((v & 0x7F) | 0x80);  // continuation-bit = 1
        v >>= 7;
    }
    *deltaPtr++ = char(v);  // final byte, cont-bit = 0
}

inline void emitADD(const char* data, size_t len) {
    *deltaPtr++ = 0x00;                       // ADD, size follows
    writeVarint(static_cast<uint32_t>(len));  // updates deltaPtr inside
    std::memcpy(deltaPtr, data, len);
    deltaPtr += len;  // advance cursor
}

inline void emitCOPY(size_t addr, size_t len) {
    *deltaPtr++ = 0x25;  // COPY (var-size, mode 0)
    writeVarint(static_cast<uint32_t>(len));
    writeVarint(static_cast<uint32_t>(addr+baseOff));  // dictionary offset
}

void GFixSizeChunking(unsigned char *data, int len, int begflag, int begsize,
                      uint32_t *hash_table, int mask, uint64_t hashMask) {
    if (len < WordSize)
        return;

    int i = 0;
    int movebitlength = sizeof(FPTYPE) * 8 / WordSize;
    if (sizeof(FPTYPE) * 8 % WordSize != 0)
        movebitlength++;
    FPTYPE fingerprint = 0;

    /** GEAR **/
    for (; i < WordSize; i++) {
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i]];
    }

    i -= WordSize;
    FPTYPE index = 0;
    int numChunks = len - WordSize;

    int _begsize = begflag ? begsize : 0;
    int indexMoveLength = (sizeof(FPTYPE) * 8 - mask);

    while (i < numChunks) {
        index = (fingerprint) >> indexMoveLength;
        hash_table[index] = i + _begsize;
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize]];
        i++;
    }

    return;
}

void GFixSizeChunking2(unsigned char *data, int len, int begflag, int begsize,
                       uint32_t *hash_table, int mask, uint64_t hashMask) {
    if (len < WordSize)
        return;

    int i = 0;
    int movebitlength = sizeof(FPTYPE) * 8 / WordSize;
    if (sizeof(FPTYPE) * 8 % WordSize != 0)
        movebitlength++;
    FPTYPE fingerprint = 0;

    /** GEAR **/
    for (; i < WordSize; i++) {
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i]];
    }

    i -= WordSize;
    FPTYPE index = 0;
    int numChunks = len - WordSize - 1;

    int _begsize = begflag ? begsize : 0;
    int indexMoveLength = (sizeof(FPTYPE) * 8 - mask);

    while (i < numChunks) {
        index = (fingerprint) >> indexMoveLength;
        hash_table[index] = i + _begsize;
        fingerprint = (fingerprint << 2) + Gearmx_l[data[i + WordSize]] + GEARmx[data[i + WordSize + 1]];
        i+=2;
    }
}

void GFixSizeChunking_3(unsigned char *data, int len, int begflag, int begsize,
                      uint32_t *hash_table, int mask, uint64_t hashMask) {
    if (len < WordSize)
        return;

    int i = 0;
    int movebitlength = sizeof(FPTYPE) * 8 / WordSize;
    if (sizeof(FPTYPE) * 8 % WordSize != 0)
        movebitlength++;
    FPTYPE fingerprint = 0;

    /** GEAR **/
    for (; i < WordSize; i++) {
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i]];
    }

    i -= WordSize;
    FPTYPE index = 0;
    int numChunks = len - WordSize - 2;

    int _begsize = begflag ? begsize : 0;
    int indexMoveLength = (sizeof(FPTYPE) * 8 - mask);

    while (i < numChunks) {
        index = (fingerprint) >> indexMoveLength;
        hash_table[index] = i + _begsize;
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize]];
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize + 1]];
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize + 2]];
        i+=3;
    }
}

void GFixSizeChunking_4(unsigned char *data, int len, int begflag, int begsize,
                        uint32_t *hash_table, int mask, uint64_t hashMask) {
    if (len < WordSize)
        return;

    int i = 0;
    int movebitlength = sizeof(FPTYPE) * 8 / WordSize;
    if (sizeof(FPTYPE) * 8 % WordSize != 0)
        movebitlength++;
    FPTYPE fingerprint = 0;

    /** GEAR **/
    for (; i < WordSize; i++) {
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i]];
    }

    i -= WordSize;
    FPTYPE index = 0;
    int numChunks = len - WordSize - 3;

    int _begsize = begflag ? begsize : 0;
    int indexMoveLength = (sizeof(FPTYPE) * 8 - mask);

    while (i < numChunks) {
        index = (fingerprint) >> indexMoveLength;
        hash_table[index] = i + _begsize;
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize]];
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize + 1]];
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize + 2]];
        fingerprint = (fingerprint << (movebitlength)) + GEARmx[data[i + WordSize + 3]];
        i+=4;
    }
}

// Helper function to read varint from buffer (for decoder)
inline uint32_t readVarint(const uint8_t*& ptr) {
    uint32_t result = 0;
    uint32_t shift = 0;
    uint8_t byte;
    
    do {
        byte = *ptr++;
        result |= (byte & 0x7F) << shift;
        shift += 7;
    } while (byte & 0x80);
    
    return result;
}

int gencode(uint8_t *newBuf, uint32_t newSize, uint8_t *baseBuf,
            uint32_t baseSize, uint8_t **deltaBuf, uint32_t *deltaSize, uint32_t baseOffset) {
#if PRINT_PERF
    struct timespec tf0, tf1;
    clock_gettime(CLOCK_MONOTONIC, &tf0);
#endif

    /* detect the head and tail of one chunk */
    uint32_t beg = 0, end = 0, begSize = 0, endSize = 0;
    baseOff = baseOffset;
    if (*deltaBuf == nullptr) {
        *deltaBuf = (uint8_t *) malloc(INIT_BUFFER_SIZE);
    }

    // Set up global delta pointer for VCDIFF encoding
    deltaPtr = (char*)*deltaBuf;

    // Find first difference
    // First in 8 byte blocks and then in 1 byte blocks for speed
    while (begSize + sizeof(uint64_t) <= baseSize &&
           begSize + sizeof(uint64_t) <= newSize &&
           *(uint64_t *) (baseBuf + begSize) == *(uint64_t *) (newBuf + begSize)) {
        begSize += sizeof(uint64_t);
    }

    while (begSize < baseSize &&
           begSize < newSize &&
           baseBuf[begSize] == newBuf[begSize]) {
        begSize++;
    }

    if (begSize > 16)
        beg = 1;
    else
        begSize = 0;

    // Find first difference (from the end)
    while (endSize + sizeof(uint64_t) <= baseSize &&
           endSize + sizeof(uint64_t) <= newSize &&
           *(uint64_t *) (baseBuf + baseSize - endSize - sizeof(uint64_t)) ==
           *(uint64_t *) (newBuf + newSize - endSize - sizeof(uint64_t))) {
        endSize += sizeof(uint64_t);
    }

    while (endSize < baseSize &&
           endSize < newSize &&
           baseBuf[baseSize - endSize - 1] == newBuf[newSize - endSize - 1]) {
        endSize++;
    }

    if (begSize + endSize > newSize)
        endSize = newSize - begSize;

    if (endSize > 16)
        end = 1;
    else
        endSize = 0;
    /* end of detect */

    uint32_t inputPos = begSize;
    uint32_t lastLiteral_begin = inputPos;
    uint32_t literalLength = 0;  // Track accumulated literal data

    if (begSize + endSize >= baseSize) { // TODO: test this path
        if (beg) {
            // Data at start is from the original file, write instruction to copy from base
            emitCOPY(0, begSize);
        }
        if (newSize - begSize - endSize > 0) {
            int32_t litlen = newSize - begSize - endSize;
            emitADD((char*)(newBuf + begSize), litlen);
        }
        if (end) {
            int32_t offset = baseSize - endSize;
            emitCOPY(offset, endSize);
        }

        *deltaSize = deltaPtr - (char*)*deltaBuf;
        return *deltaSize;
    }

    /* chunk the baseFile */
    int32_t tmp = (baseSize - begSize - endSize) + 10;

    int32_t bit = 0;
    for (bit = 0; tmp; bit++)
        tmp >>= 1;

    uint64_t hashMask = 0XFFFFFFFFFFFFFFFF >> (64 - bit); // mask
    uint32_t handleBytes = begSize;
    FPTYPE fingerprint = 0;
    uint32_t cursor = 0;
    uint32_t matchNum = 0;
    int32_t moveBitLength = 0;
    uint32_t lastMatchPos = inputPos;
    uint32_t *hash_table = nullptr;
    uint32_t baseoffset = 0;
    uint32_t hash_size = hashMask + 1;
    bool isFindMatch = false;

    if (beg) {
        // Data at start is from the original file, write instruction to copy from base
        emitCOPY(0, begSize);
    }

    uint32_t moveindex = (sizeof(FPTYPE) * 8 - bit);

    isFindMatch = true;
    hash_table = (uint32_t *) malloc(hash_size * sizeof(uint32_t));
    memset(hash_table, 0, sizeof(uint32_t) * hash_size);

#if PRINT_PERF
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
#endif

#ifdef BaseSampleRate
    if(BaseSampleRate == 2 && WordSize == 64)
    {
        GFixSizeChunking2(baseBuf + begSize, baseSize - begSize - endSize, beg, begSize, hash_table, bit, hashMask);
    }else if(BaseSampleRate == 3)
    {
        GFixSizeChunking_3(baseBuf + begSize, baseSize - begSize - endSize, beg, begSize, hash_table, bit, hashMask);
    }else if(BaseSampleRate == 4)
    {
        GFixSizeChunking_4(baseBuf + begSize, baseSize - begSize - endSize, beg, begSize, hash_table, bit, hashMask);
    } else
    {
        GFixSizeChunking(baseBuf + begSize, baseSize - begSize - endSize, beg, begSize, hash_table, bit, hashMask);
    }
#else
    GFixSizeChunking(baseBuf + begSize, baseSize - begSize - endSize, beg, begSize, hash_table, bit, hashMask);
#endif

#if PRINT_PERF
    clock_gettime(CLOCK_MONOTONIC, &t1);
    fprintf(stderr, "size:%d\n", baseSize - begSize - endSize);
    fprintf(stderr, "hash size:%d\n", hash_size);
    fprintf(stderr, "rolling hash:%.3fMB/s\n",
            (double)(baseSize - begSize - endSize) / 1024 / 1024 /
                ((t1.tv_sec - t0.tv_sec) * 1000000000 + t1.tv_nsec - t0.tv_nsec) *
                1000000000);
    fprintf(stderr, "rolling hash:%zd\n",
            (t1.tv_sec - t0.tv_sec) * 1000000000 + t1.tv_nsec - t0.tv_nsec);
    clock_gettime(CLOCK_MONOTONIC, &t0);
    fprintf(stderr, "hash table :%zd\n",
            (t0.tv_sec - t1.tv_sec) * 1000000000 + t0.tv_nsec - t1.tv_nsec);
#endif
    /* end of inserting */

    if (sizeof(FPTYPE) * 8 % WordSize == 0)
        moveBitLength = sizeof(FPTYPE) * 8 / WordSize;
    else
        moveBitLength = sizeof(FPTYPE) * 8 / WordSize + 1;

    for (uint32_t i = 0; i < WordSize && i < newSize - endSize - inputPos; i++) {
        fingerprint = (fingerprint << (moveBitLength)) + GEARmx[(newBuf + inputPos)[i]];
    }

    while (inputPos + WordSize <= newSize - endSize) {
        uint32_t length;
        bool matchflag = false;
        cursor = inputPos + WordSize;
        length = WordSize;

        int32_t index = (fingerprint) >> moveindex;
        uint32_t offset = 0;
        baseoffset = hash_table[index];

        if (baseoffset != 0 && memcmp(newBuf + inputPos, baseBuf + baseoffset, length) == 0) {
            matchflag = true;
            offset = baseoffset;
        }

#ifdef ReverseMatch
        if (baseoffset != 0 && !matchflag) {
            uint32_t i = length;
            uint8_t *basepos_end = baseBuf + baseoffset + length - 1;
            uint8_t *inputpos_end = newBuf + inputPos + length - 1;
            for (; i > 0 && (*basepos_end == *inputpos_end); i--, basepos_end--, inputpos_end--) {
            }

            uint32_t matchlen_end = length - i;

            if (matchlen_end > WordSize / 2) {
                int j = 0;
                {
                    uint8_t *p1 = baseBuf + baseoffset + length;
                    uint8_t *p2 = newBuf + inputPos + length;
                    uint32_t *basepos_end1 = (uint32_t *) p1;
                    uint32_t *inputpos_end1 = (uint32_t *) p2;

                    for (; p1 + 3 + j < baseBuf + baseSize &&
                           p2 + 3 + j < newBuf + newSize - endSize &&
                           (*basepos_end1 == *inputpos_end1); j += 4, basepos_end1++, inputpos_end1++) {
                    }
                }

                // Emit accumulated literal data if any
                if (literalLength > 0) {
                    emitADD((char*)(newBuf + lastLiteral_begin), literalLength);
                    literalLength = 0;
                }

                emitCOPY(basepos_end - baseBuf + 1, matchlen_end + j);
                
                handleBytes += (length + j);
                inputPos += (length + j);
                lastLiteral_begin = inputPos;
                lastMatchPos = inputPos;
                
                for (int k = 0; k < WordSize && (inputPos + k < newSize - endSize); k++) {
                    fingerprint = (fingerprint << (moveBitLength)) + GEARmx[newBuf[inputPos + k]];
                }
                continue;
            }
        }
#endif

        /* New data match found in hashtable/base data; attempt to create copy instruction*/
        if (matchflag) {
            matchNum++;
            // Check how much is possible to copy
            int32_t j = 0;
#if 1 /* 8-bytes optimization */
            while (offset + length + j + 7 < baseSize - endSize &&
                   cursor + j + 7 < newSize - endSize &&
                   *(uint64_t *) (baseBuf + offset + length + j) == *(uint64_t *) (newBuf + cursor + j)) {
                j += sizeof(uint64_t);
            }
            while (offset + length + j < baseSize - endSize &&
                   cursor + j < newSize - endSize &&
                   baseBuf[offset + length + j] == newBuf[cursor + j]) {
                j++;
            }
#endif
            cursor += j;

            int32_t matchlen = cursor - inputPos;
            handleBytes += cursor - inputPos;
            uint64_t _offset = offset;

            // Check if we can extend the match backwards into previous literal
            uint32_t k = 0;
            while (k + 1 <= offset && k + 1 <= literalLength) {
                if (baseBuf[offset - (k + 1)] == newBuf[inputPos - (k + 1)])
                    k++;
                else
                    break;
            }

            if (k > 0) {
                // Reduce literal by the amount covered by the copy
                literalLength -= k;
                // Set up adjusted copy parameters
                matchlen += k;
                _offset -= k;
            }

            // Emit accumulated literal data if any
            if (literalLength > 0) {
                emitADD((char*)(newBuf + lastLiteral_begin), literalLength);
                literalLength = 0;
            }

            emitCOPY(_offset, matchlen);

            // Update cursor (inputPos) and fingerprint
            for (uint32_t k = cursor; k < cursor + WordSize && cursor + WordSize < newSize - endSize; k++) {
                fingerprint = (fingerprint << (moveBitLength)) + GEARmx[newBuf[k]];
            }

            inputPos = cursor;
            lastMatchPos = cursor;
            lastLiteral_begin = cursor;
        } else { // No match, accumulate literal data
            literalLength += 1;
            handleBytes += 1;
            
            // Update cursor (inputPos) and fingerprint
            if (inputPos + WordSize < newSize - endSize) {
                fingerprint = (fingerprint << (moveBitLength)) + GEARmx[newBuf[inputPos + WordSize]];
            }
            inputPos++;
            
#ifdef SkipOn
            int step = ((inputPos - lastMatchPos) >> SkipStep);

            if(step <= WordSize)
            {
                for(int i = 0; i < step && (inputPos + WordSize < newSize - endSize); i++,inputPos++)
                {
                    fingerprint = (fingerprint << (moveBitLength)) + GEARmx[newBuf[inputPos + WordSize]];
                    handleBytes += 1;
                    literalLength += 1;
                }
            }
            else
            {
                fingerprint = 0;
                int cursor = inputPos + step;
                int len = 0;
                for(int i = 0; i < WordSize && (cursor + i < newSize - endSize); i++)
                {
                    fingerprint = (fingerprint << (moveBitLength)) + GEARmx[newBuf[cursor + i]];
                    len++;
                }
                int l = min(newSize - endSize, inputPos + step);
                int realStep = l - inputPos;
                handleBytes += realStep;
                literalLength += realStep;
                inputPos += realStep;
            }
#endif
        }
    }

#if PRINT_PERF
    clock_gettime(CLOCK_MONOTONIC, &t1);
    fprintf(stderr, "look up:%zd\n",
            (t1.tv_sec - t0.tv_sec) * 1000000000 + t1.tv_nsec - t0.tv_nsec);
    fprintf(stderr, "look up:%.3fMB/s\n",
            (double)(baseSize - begSize - endSize) / 1024 / 1024 /
                ((t1.tv_sec - t0.tv_sec) * 1000000000 + t1.tv_nsec - t0.tv_nsec) *
                1000000000);
#endif

    // Handle remaining literal data
    if (literalLength > 0 || !isFindMatch) {
        uint32_t remainingLen = newSize - endSize - lastLiteral_begin;
        if (remainingLen > 0) {
            emitADD((char*)(newBuf + lastLiteral_begin), remainingLen);
        }
    } else {
        // Last operation was COPY, check if there's remaining data
        if (newSize - endSize - handleBytes > 0) {
            uint32_t remainingLen = newSize - endSize - lastLiteral_begin;
            if (remainingLen > 0) {
                emitADD((char*)(newBuf + lastLiteral_begin), remainingLen);
            }
        }
    }

    if (end) {
        int32_t offset = baseSize - endSize;
        emitCOPY(offset, endSize);
    }

    *deltaSize = deltaPtr - (char*)*deltaBuf;

#if PRINT_PERF
    clock_gettime(CLOCK_MONOTONIC, &tf1);
    fprintf(stderr, "gencode took: %zdns\n", (tf1.tv_sec - tf0.tv_sec) * 1000000000 + tf1.tv_nsec - tf0.tv_nsec);
#endif

    if (hash_table)
        free(hash_table);

    return *deltaSize;
}

int gdecode(uint8_t *deltaBuf, uint32_t deltaSize, uint8_t *baseBuf, uint32_t baseSize,
            uint8_t **outBuf, uint32_t *outSize) {

    if (*outBuf == nullptr) {
        *outBuf = (uint8_t *) malloc(INIT_BUFFER_SIZE);
    }

#if PRINT_PERF
    struct timespec tf0, tf1;
    clock_gettime(CLOCK_MONOTONIC, &tf0);
#endif

    const uint8_t* deltaPtr = deltaBuf;
    const uint8_t* deltaEnd = deltaBuf + deltaSize;
    uint8_t* outPtr = *outBuf;
    uint32_t outCapacity = INIT_BUFFER_SIZE;
    uint32_t outPos = 0;

    while (deltaPtr < deltaEnd) {
        uint8_t opcode = *deltaPtr++;
        
        if (opcode == 0x00) {  // ADD instruction
            uint32_t len = readVarint(deltaPtr);
            
            // Ensure output buffer has enough space
            while (outPos + len > outCapacity) {
                outCapacity *= 2;
                *outBuf = (uint8_t*)realloc(*outBuf, outCapacity);
                outPtr = *outBuf;
            }
            
            // Copy literal data
            memcpy(outPtr + outPos, deltaPtr, len);
            outPos += len;
            deltaPtr += len;
            
        } else if (opcode == 0x25) {  // COPY instruction
            uint32_t len = readVarint(deltaPtr);
            uint32_t addr = readVarint(deltaPtr);
            
            // Ensure output buffer has enough space
            while (outPos + len > outCapacity) {
                outCapacity *= 2;
                *outBuf = (uint8_t*)realloc(*outBuf, outCapacity);
                outPtr = *outBuf;
            }
            
            // Copy from base buffer
            if (addr + len <= baseSize) {
                memcpy(outPtr + outPos, baseBuf + addr, len);
                outPos += len;
            } else {
                // Handle error - invalid address/length
                return -1;
            }
        } else {
            // Unknown opcode
            return -1;
        }
    }

    *outSize = outPos;

#if PRINT_PERF
    clock_gettime(CLOCK_MONOTONIC, &tf1);
    fprintf(stderr, "gdecode took: %zdns\n", (tf1.tv_sec - tf0.tv_sec) * 1000000000 + tf1.tv_nsec - tf0.tv_nsec);
#endif

    return outPos;
}