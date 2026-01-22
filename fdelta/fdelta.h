#pragma once
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#define XXH_INLINE_ALL
#include <immintrin.h>
#include <string.h>
#include <x86intrin.h>

#include <chrono>
#include <cstring>
#include <unordered_map>

#include "../src/lz4/lz4.h"
#include "fdelta_commands.hpp"
#include "xxhash.h"

// #define DEBUG 1

#define hash_length 128

#define COMPRESSION_LEVEL 1

#define NUMBER_OF_CHUNKS 5
#define CHUNKS_MULTIPLIER 5

size_t minChunkSize = 1;
size_t maxChunkSize = 2048;
size_t window_size = 256;          // Default window size
size_t backward_window_size = 16;  // Default window size

constexpr size_t MaxChunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;

size_t sizeBaseChunk = 0;
size_t sizeInputChunk = 0;
using Hash64 = std::uint64_t;

static inline uint64_t load_u64(const unsigned char* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline bool memeq_8(const void* a, const void* b) {
    // memcpy is the safest unaligned load
    uint64_t x, y;
    std::memcpy(&x, a, 8);
    std::memcpy(&y, b, 8);
    return (x ^ y) == 0;
}

inline bool memeq_32(const void* a, const void* b) {
#if defined(__AVX2__)
    __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
    __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
    __m256i x = _mm256_xor_si256(va, vb);
    return _mm256_testz_si256(x, x);
#else
    const uint64_t* pa = reinterpret_cast<const uint64_t*>(a);
    const uint64_t* pb = reinterpret_cast<const uint64_t*>(b);
    // Unaligned 64-bit loads are okay on x86; if you target other arches, keep
    // memcpy
    return ((pa[0] ^ pb[0]) | (pa[1] ^ pb[1]) | (pa[2] ^ pb[2]) |
            (pa[3] ^ pb[3])) == 0;
#endif
}

inline bool memeq_64(const void* a, const void* b) {
#if defined(__AVX2__)
    const uint8_t* pa = static_cast<const uint8_t*>(a);
    const uint8_t* pb = static_cast<const uint8_t*>(b);
    return memeq_32(pa, pb) && memeq_32(pa + 32, pb + 32);
#else
    const uint64_t* pa = reinterpret_cast<const uint64_t*>(a);
    const uint64_t* pb = reinterpret_cast<const uint64_t*>(b);
    return ((pa[0] ^ pb[0]) | (pa[1] ^ pb[1]) | (pa[2] ^ pb[2]) |
            (pa[3] ^ pb[3]) | (pa[4] ^ pb[4]) | (pa[5] ^ pb[5]) |
            (pa[6] ^ pb[6]) | (pa[7] ^ pb[7])) == 0;
#endif
}

inline bool memeq_128(const void* a, const void* b) {
#if defined(__AVX512F__)
    __m512i va0 = _mm512_loadu_si512(a);
    __m512i vb0 = _mm512_loadu_si512(b);
    __mmask64 k = _mm512_cmpeq_epi8_mask(va0, vb0);
    return k == ~__mmask64(0);  // all bytes equal
#elif defined(__AVX2__)
    const uint8_t* pa = static_cast<const uint8_t*>(a);
    const uint8_t* pb = static_cast<const uint8_t*>(b);
    __m256i a0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 0));
    __m256i b0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 0));
    __m256i a1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 32));
    __m256i b1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 32));
    __m256i a2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 64));
    __m256i b2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 64));
    __m256i a3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 96));
    __m256i b3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 96));

    __m256i x0 = _mm256_xor_si256(a0, b0);
    __m256i x1 = _mm256_xor_si256(a1, b1);
    __m256i x2 = _mm256_xor_si256(a2, b2);
    __m256i x3 = _mm256_xor_si256(a3, b3);

    __m256i o01 = _mm256_or_si256(x0, x1);
    __m256i o23 = _mm256_or_si256(x2, x3);
    __m256i o = _mm256_or_si256(o01, o23);
    return _mm256_testz_si256(o, o);
#else
    // Scalar fallback
    return memeq_64(a, b) && memeq_64(static_cast<const uint8_t*>(a) + 64,
                                      static_cast<const uint8_t*>(b) + 64);
#endif
}

struct alignas(64) TinyMapSIMD {
    static constexpr uint32_t kCap = 32;

    uint64_t fp[kCap];   // fingerprints
    uint32_t off[kCap];  // offsets
    uint32_t count;      // 0..16

    inline TinyMapSIMD() { clear(); }

    inline void clear() {
        // We only need to reset count; fp/off content can remain.
        // To be extra safe against vector path matching garbage beyond count,
        // we keep the scalar find as default. If you want a vector path,
        // either guard by count or prefill sentinels here.
        count = 0;
    }

    // Insert or overwrite the existing fingerprint's offset.
    inline void upsert(uint64_t fingerprint, uint32_t offset) {
        fp[count] = fingerprint;
        off[count] = offset;
        ++count;
    }
    // Optional AVX2 finder (disabled by default). Enable if you want.
    inline bool find(uint64_t fingerprint, uint32_t& outOffset) const {
        for (uint32_t i = 0; i < count; ++i) {
            if (fp[i] == fingerprint) {
                outOffset = off[i];
                return true;
            }
        }
        return false;
    }
};

alignas(64) TinyMapSIMD baseChunks;

// -------------------- Chunker --------------------

#define SSE_REGISTER_SIZE_BITS 128
#define SSE_REGISTER_SIZE_BYTES 16

#ifdef __SSE3__
uint64_t num_vectors = window_size / SSE_REGISTER_SIZE_BYTES;
__m128i* sse_array = new __m128i[num_vectors]();
// sse_array2 = new __m128i[num_vectors2]();

#endif

#include <cstddef>
#include <cstdint>

#ifdef __SSE3__
const __m128i K_INV_ZERO = _mm_set1_epi8(0xFF);
#endif

#ifdef __SSE3__
inline __m128i Greater8uSSE(__m128i a, __m128i b) {
    return _mm_andnot_si128(_mm_cmpeq_epi8(_mm_min_epu8(a, b), a), K_INV_ZERO);
}

inline __m128i GreaterOrEqual8uSSE(__m128i a, __m128i b) {
    return _mm_cmpeq_epi8(_mm_max_epu8(a, b), a);
}

inline __m128i Lesser8uSSE(__m128i a, __m128i b) {
    return _mm_andnot_si128(_mm_cmpeq_epi8(_mm_max_epu8(a, b), a), K_INV_ZERO);
}

inline __m128i LesserOrEqual8uSSE(__m128i a, __m128i b) {
    return _mm_cmpeq_epi8(_mm_min_epu8(a, b), a);
}
inline __m128i NotEqual8uSSE(__m128i a, __m128i b) {
    return _mm_andnot_si128(_mm_cmpeq_epi8(a, b), K_INV_ZERO);
}
#endif

#if defined(__SSE3__)
uint8_t find_maximum_sse128(unsigned char* buff, uint64_t start_pos,
                            uint64_t end_pos, __m128i* xmm_array) {
    // Assume window_size is a multiple of SSE_REGISTER_SIZE_BYTES for now
    // Assume num_vectors is even for now - True for most common window sizes.
    // Can fix later via specific check

    uint64_t num_vectors = (end_pos - start_pos) / SSE_REGISTER_SIZE_BYTES;

    uint64_t step = 2;
    uint64_t half_step = 1;

    // Load contents into __m128i structures
    // Could be optimized later as only 16 xmm registers are avaiable per CPU in
    // 64-bit
    for (uint64_t i = 0; i < num_vectors; i++)
        xmm_array[i] = _mm_loadu_si128(
            (__m128i const*)(buff + start_pos + (SSE_REGISTER_SIZE_BYTES * i)));

    // Repeat vmaxu until a single register is remaining with maximum values
    // Each iteration calculates maximums between a pair of registers and moves
    // it into the first register in the pair Finally, only one will be left
    // with the maximum values from all pairs
    while (step <= num_vectors) {
        for (uint64_t i = 0; i < num_vectors; i += step)
            xmm_array[i] = _mm_max_epu8(xmm_array[i], xmm_array[i + half_step]);

        // Multiply step by 2
        half_step = step;
        step = step << 1;
    }

    // Move the final set of values from the xmm into local memory
    uint8_t result_store[SSE_REGISTER_SIZE_BYTES] = {0};

    _mm_storeu_si128((__m128i*)&result_store, xmm_array[0]);

    // Sequentially scan the last remaining bytes (128 in this case) to find the
    // max value
    uint8_t max_val = 0;
    for (uint64_t i = 0; i < SSE_REGISTER_SIZE_BYTES; i++) {
        if (result_store[i] > max_val) max_val = result_store[i];
    }

    // Return maximum value
    return max_val;
}

uint64_t range_scan_geq_sse128(unsigned char* buff, uint64_t start_position,
                               uint64_t end_position, uint8_t target_value) {
    uint64_t num_vectors =
        (end_position - start_position) / SSE_REGISTER_SIZE_BYTES;
    uint64_t curr_scan_start;
    uint64_t return_pos;

    // Structures to store bytes from data stream and comparison results in
    // 128-bit SSE format
    __m128i xmm_array, cmp_array;
    int cmp_mask;

    // Load max_value into xmm-format
    __m128i max_val_xmm = _mm_set1_epi8((char)target_value);

    for (uint64_t i = 0; i < num_vectors; i++) {
        curr_scan_start = start_position + (i * SSE_REGISTER_SIZE_BYTES);
        // Load data into xmm register
        xmm_array = _mm_loadu_si128((__m128i const*)(buff + curr_scan_start));

        /*
         Compare values with max_value. If a byte in xmm_array is geq
         max_val_xmm, ALL the corresponding bits of the corresponding byte in
         cmp_array are set to 1.
        */

        cmp_array = GreaterOrEqual8uSSE(xmm_array, max_val_xmm);

        // Create a mask using the most-significant bit of each byte value in
        // cmp_array
        cmp_mask = _mm_movemask_epi8(cmp_array);

        // Return index of first non-zero bit in mask
        // This corresponds to the first non-zero byte in cmp_array
        if (cmp_mask) {
            return_pos = curr_scan_start + (__builtin_ffs(cmp_mask) - 1);
            return return_pos;
        }
    }

    return end_position;
}
#endif

// size_t nextChunk(unsigned char* readBuffer, size_t buffBegin, size_t buffEnd)
// {
//     uint32_t i = 0;
//     size_t size = buffEnd - buffBegin;
//     unsigned char max_value = readBuffer[i + buffBegin];
//     i++;
//     if (size > maxChunkSize)
//         size = maxChunkSize;
//     else if (size < window_size)
//         return size;
// #ifdef __SSE3__
//     // If SIMD enabled, accelerate find_maximum() and slide depending on
//     // SIMD mode
//     max_value = find_maximum_sse128(readBuffer + buffBegin, 0, window_size,
//     sse_array);
//     return range_scan_geq_sse128(readBuffer + buffBegin, window_size, size,
//                                  max_value);
// #endif

//     return 0;
// }

// size_t nextChunk(unsigned char* readBuffer, size_t buffBegin, size_t buffEnd)
// {
//     uint64_t i = 0;
//     size_t size = buffEnd - buffBegin;
//     uint8_t max_value = *(uint8_t*)(readBuffer + buffBegin + i);
//     i++;
//     if (size > maxChunkSize)
//         size = maxChunkSize;
//     else if (size < window_size)
//         return size;

//     for (; i < window_size; i++) {
//         uint8_t value = *(uint8_t*)(readBuffer + buffBegin + i);
//         if (value > max_value) {
//             max_value = value;
//         }
//     }

//     for (; i < size; i++) {
//         uint8_t value = *(uint8_t*)(readBuffer + buffBegin + i);
//         if (value >= max_value) {
//             return i;
//         }
//     }

//     return size;
// }

uint32_t g[256] = {
    0x4b8fbc70, 0x95a75be0, 0x7fa97617, 0xdb51a0d,  0x7c71d5b3, 0x97842403,
    0x87d60b89, 0x10c081cb, 0x176c2faf, 0xc7392648, 0x2f15cf70, 0x842062ac,
    0x7d19bc1b, 0xc9a22b6d, 0x29f65703, 0x54f0a470, 0x4913c078, 0x91dd2661,
    0xce401296, 0x1080796b, 0x3c6ba084, 0x291fd606, 0x1be96ae,  0x5e104df3,
    0x953a3946, 0xd59e7f77, 0x9f4734d9, 0xf095c27c, 0xe4448418, 0x47ca4676,
    0xe9131404, 0xc6965ee3, 0x2f46c05a, 0x8ba2c750, 0x5fb2fec6, 0xaae07db7,
    0xf96ad818, 0xaed74a8d, 0x3b73296d, 0x42b47c83, 0x3f14fa76, 0xe775583b,
    0x8583a649, 0xe347effd, 0x1814ed33, 0xd747a499, 0x1241b4c,  0xc2ba1de6,
    0x9e459ecd, 0xf2cb801a, 0xb1575ba0, 0x57230ea3, 0xfd6e88ca, 0x1d0501a2,
    0xf33ed508, 0x531f339c, 0xece73621, 0x2171ee60, 0xc545563a, 0x3b68b071,
    0x480657e4, 0x69b955c1, 0x954a24b1, 0xc458ee33, 0x48958571, 0xccdd1652,
    0xa67ad0f0, 0xa93e0890, 0x8255fe6,  0xc190ff58, 0x6d2966a,  0x1e3e1a20,
    0x1649fd23, 0xa5452efe, 0x247ce55,  0xf4220d84, 0xf817eb4b, 0xab0ff6bf,
    0xd98c80da, 0xbb660cad, 0xce39b4a0, 0xebfa6bfe, 0xc4baba1b, 0x5cfd0930,
    0x7cdba262, 0xab2c4cdb, 0xdd1858e7, 0x7aa35c54, 0x36c7793b, 0xc3298957,
    0x5212e006, 0x772a10b3, 0x4b412801, 0x6348ea44, 0x90b7a8c,  0xf3903817,
    0x55ec8d5,  0xd39bca5e, 0xa19e4f8b, 0xf8472700, 0xd792025e, 0xcfdc83e0,
    0x576880c7, 0xcbaa6558, 0x692f5350, 0x1400ee49, 0x3a89aa8b, 0x46873d99,
    0x63ecb853, 0x9860b122, 0x31685257, 0x32874e3d, 0x6b44e063, 0x87525cd0,
    0xf0a5025c, 0x6ccb4020, 0xf1d8849d, 0x414f66e7, 0x6afed5a4, 0x9a9befeb,
    0xd6e18fdc, 0x27129a4c, 0x7d6bcf28, 0x37fa944a, 0xe9b09b54, 0x9159093,
    0x4a31242,  0x28d0d7d2, 0x97e8d65a, 0xfcf68625, 0x1800702,  0x39dbe43f,
    0x35da0210, 0xa6c68d9,  0x68500f4e, 0xc8a5ab6e, 0x8b2b756a, 0xd5fb25bd,
    0xb0c0405d, 0xc453b18c, 0xe2e66ea4, 0xda553370, 0x7dbc5759, 0x886eeab,
    0x9fafd0e5, 0x4e430b,   0x8331c2e5, 0x5a5d06b4, 0xe43f7b9d, 0xf3ec3644,
    0x6a46d11,  0xcd41e503, 0xa18a2e19, 0xadde1ea1, 0x36f7488e, 0x455255bd,
    0x8075175e, 0x88d32d58, 0x9a0a25b0, 0x970e0c67, 0x77b009e,  0x12d5bf75,
    0xdc7fd18,  0xde77a2bb, 0x6ab176e0, 0xa6ab6c09, 0x6210efa8, 0xd947e049,
    0x5fb01605, 0xde4bd448, 0x5b2af7ba, 0xd2b49fe9, 0x1783f531, 0x66fffd39,
    0x59e8a9ba, 0xc2ee818b, 0x1f3b0f3c, 0x4cd0e620, 0xeaf278ba, 0x92045e3d,
    0x64e6ebca, 0x9b70573,  0x3aaf57a4, 0xe287e26,  0x54dcafbb, 0x8c25a8,
    0x99642381, 0x5c8de9f0, 0x51fda6c0, 0x910e0b6d, 0xf6328e12, 0xec5e5389,
    0x8ed195f,  0xe80f0583, 0xa7185a25, 0xa83c3268, 0xa4e79433, 0xe6def4b,
    0x77065b1f, 0x43678dc7, 0x7e506399, 0x88503f60, 0x62c4983d, 0x70ece12,
    0xe0d421a4, 0x71821295, 0x801310e8, 0x3bd7bf4b, 0xe42779be, 0xefee3449,
    0xd6aaaffc, 0xa5608bae, 0xf6e0b8ae, 0xdd79aa9f, 0x8bd6606d, 0x3b83977a,
    0x512a0c70, 0x2c6de88,  0x25f5ea97, 0x68ceb140, 0xdaa87097, 0x602b8497,
    0x7767467a, 0xfee216a8, 0x14512325, 0x142c4e04, 0x7bf5ac09, 0x384364d5,
    0xf58188a8, 0xa410598,  0x799223f,  0xc1b76b42, 0xd828d74b, 0x931ecbb1,
    0x41922ad3, 0x787fb489, 0xf41c1d00, 0x3dbc3f88, 0x12a70e39, 0x26ae363d,
    0x47f7274,  0x86385074, 0x2ffb7263, 0xb8e3de33, 0x9496a61,  0x92025809,
    0xbf8b296d, 0xf1a57003, 0xa8057fb6, 0x2ce2e565, 0x56d7a64a, 0xa6e30007,
    0xe0562996, 0xabec18bd, 0x6b8c68ed, 0x0b1c1af1};

size_t nextChunk(unsigned char* readBuffer, size_t buffBegin, size_t buffEnd) {
    uint64_t i = 1;
    uint32_t hash = 0;
    size_t size = buffEnd - buffBegin;
    if (size > maxChunkSize) size = maxChunkSize;
    for (; i + 1 < size; i += 2) {
        uint8_t byte = *(uint8_t*)(readBuffer + buffBegin + i);
        uint8_t byte2 = *(uint8_t*)(readBuffer + buffBegin + i + 1);
        hash = (hash << 2) + (g[byte]) + (g[byte2]);
        // hash = (hash << 1) + (g[byte]);
        if (!(hash & 0x18035100)) {
        // if (!(hash & 0x1804110)) {
            return i;
        }
    }

    return size;
}

size_t nextChunkBackward(unsigned char* readBuffer, size_t buffBegin,
                         size_t buffEnd) {
    uint64_t i = 0;
    size_t size = buffEnd - buffBegin;
    if (size == 0) return 0;

    if (size > maxChunkSize)
        size = maxChunkSize;
    else if (size < backward_window_size)
        return size;

    uint8_t max_value = *(uint8_t*)(readBuffer + buffEnd - 1 - i);
    i++;

    for (; i < backward_window_size; i++) {
        uint8_t value = *(uint8_t*)(readBuffer + buffEnd - 1 - i);
        if (value > max_value) {
            max_value = value;
        }
    }

    for (; i < size; i++) {
        uint8_t value = *(uint8_t*)(readBuffer + buffEnd - 1 - i);
        if (value >= max_value) {
            return i;
        }
    }

    return size;
}
