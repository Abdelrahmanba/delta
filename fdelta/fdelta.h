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
#include "xxhash.h"
#include "fdelta_commands.hpp"

// #define DEBUG 1

#define hash_length 32

#define COMPRESSION_LEVEL 1

#define NUMBER_OF_CHUNKS 5
#define CHUNKS_MULTIPLIER 5

size_t minChunkSize = 1;
size_t maxChunkSize = 1024;
size_t window_size = 256;  // Default window size
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
#if defined(__AVX2__)

        // Load up to 16 lanes (4 loads of 4x64). Safe because arrays are
        // full-size.
        __m256i key = _mm256_set1_epi64x((long long)fingerprint);

        const __m256i a0 =
            _mm256_loadu_si256((const __m256i*)&fp[0]);  // lanes 0..3
        const __m256i a1 = _mm256_loadu_si256((const __m256i*)&fp[4]);  // 4..7
        const __m256i a2 = _mm256_loadu_si256((const __m256i*)&fp[8]);  // 8..11
        const __m256i a3 =
            _mm256_loadu_si256((const __m256i*)&fp[12]);  // 12..15
        const __m256i a4 =
            _mm256_loadu_si256((const __m256i*)&fp[16]);  // 16..19

        const __m256i m0 = _mm256_cmpeq_epi64(a0, key);
        const __m256i m1 = _mm256_cmpeq_epi64(a1, key);
        const __m256i m2 = _mm256_cmpeq_epi64(a2, key);
        const __m256i m3 = _mm256_cmpeq_epi64(a3, key);
        const __m256i m4 = _mm256_cmpeq_epi64(a4, key);

        // Convert masks to per-lane hits:
        // movemask gives 32 bits; every equal 64-bit lane sets 8 bits to 1.
        uint32_t mm0 = (uint32_t)_mm256_movemask_epi8(m0);
        uint32_t mm1 = (uint32_t)_mm256_movemask_epi8(m1);
        uint32_t mm2 = (uint32_t)_mm256_movemask_epi8(m2);
        uint32_t mm3 = (uint32_t)_mm256_movemask_epi8(m3);
        uint32_t mm4 = (uint32_t)_mm256_movemask_epi8(m4);

        // Helper to scan first hit within a 4-lane group
        auto first_lane = [](uint32_t mm) -> int {
            // Each 64-bit lane corresponds to 8 mask bits.
            for (int lane = 0; lane < 4; ++lane) {
                if (mm & (0xFFu << (lane * 8))) return lane;
            }
            return -1;
        };

        int grp = -1, lane = -1;

        if (mm0) {
            grp = 0;
            lane = first_lane(mm0);
        } else if (mm1) {
            grp = 1;
            lane = first_lane(mm1);
        } else if (mm2) {
            grp = 2;
            lane = first_lane(mm2);
        } else if (mm3) {
            grp = 3;
            lane = first_lane(mm3);
        } else if (mm4) {
            grp = 4;
            lane = first_lane(mm4);
        }

        if (lane >= 0) {
            uint32_t idx = (uint32_t)(grp * 4 + lane);
            if (idx < count && fp[idx] == fingerprint) {
                outOffset = off[idx];
                return true;
            }
        }
        return false;
#else
        for (uint32_t i = 0; i < count; ++i) {
            if (fp[i] == fingerprint) {
                outOffset = off[i];
                return true;
            }
        }
        return false;
#endif
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

size_t nextChunk(unsigned char* readBuffer, size_t buffBegin, size_t buffEnd) {
    uint64_t i = 0;
    size_t size = buffEnd - buffBegin;
    uint8_t max_value = *(uint8_t*)(readBuffer + buffBegin + i);
    i++;
    if (size > maxChunkSize)
        size = maxChunkSize;
    else if (size < window_size)
        return size;

    for (; i < window_size; i++) {
        uint8_t value = *(uint8_t*)(readBuffer + buffBegin + i);
        if (value > max_value) {
            max_value = value;
        }
    }

    for (; i < size; i++) {
        uint8_t value = *(uint8_t*)(readBuffer + buffBegin + i);
        if (value >= max_value) {
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
