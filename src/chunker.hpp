#pragma once

#include <x86intrin.h>

#include <cstddef>
#include <cstdint>

#define SSE_REGISTER_SIZE_BITS 128
#define SSE_REGISTER_SIZE_BYTES 16


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

extern uint64_t fingerprint;

class Chunker {
   public:
    Chunker();
    ~Chunker();

    size_t nextChunk(char *readBuffer, size_t buffBegin,
                     size_t buffEnd);
    size_t nextChunkBig(char *readBuffer, size_t buffBegin,
                        size_t buffEnd);
    uint8_t find_maximum_sse128(char *buff, uint64_t start_pos,
                                uint64_t end_pos, __m128i *xmm_array);
    uint64_t range_scan_geq_sse128(char *buff, uint64_t start_position,
                                   uint64_t end_position, uint8_t target_value);
    #ifdef __SSE3__
      __m128i *sse_array;
    #endif
   private:
    size_t minChunkSize;
    size_t maxChunkSize;
    size_t avgChunkSize;
    size_t window_size; // Size of the sliding window for chunking
};
