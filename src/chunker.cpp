

#include "chunker.hpp"

#include <chrono>
#include <iostream>
#include <map>


Chunker::Chunker() {
    minChunkSize = 256;
    maxChunkSize = 2048;
    avgChunkSize = 1024;
    window_size = 770;  // Default window size
#ifdef __SSE3__
    uint64_t num_vectors = window_size / SSE_REGISTER_SIZE_BYTES;
    sse_array = new __m128i[num_vectors]();
#endif
}

#if defined(__SSE3__)
uint8_t Chunker::find_maximum_sse128(char *buff, uint64_t start_pos,
                                     uint64_t end_pos, __m128i *xmm_array) {
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
        xmm_array[i] = _mm_loadu_si128((
            __m128i const *)(buff + start_pos + (SSE_REGISTER_SIZE_BYTES * i)));

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

    _mm_storeu_si128((__m128i *)&result_store, xmm_array[0]);

    // Sequentially scan the last remaining bytes (128 in this case) to find the
    // max value
    uint8_t max_val = 0;
    for (uint64_t i = 0; i < SSE_REGISTER_SIZE_BYTES; i++) {
        if (result_store[i] > max_val) max_val = result_store[i];
    }

    // Return maximum value
    return max_val;
}

uint64_t Chunker::range_scan_geq_sse128(char *buff, uint64_t start_position,
                                        uint64_t end_position,
                                        uint8_t target_value) {
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
        xmm_array = _mm_loadu_si128((__m128i const *)(buff + curr_scan_start));

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

size_t Chunker::nextChunk(char *readBuffer, size_t buffBegin, size_t buffEnd) {
    uint32_t i = 0;
    size_t size = buffEnd - buffBegin;
    uint8_t max_value = (uint8_t)readBuffer[i];
    i++;
    if (size > maxChunkSize)
        size = maxChunkSize;
    else if (size < window_size)
        return size;
#ifdef __SSE3__
    // If SIMD enabled, accelerate find_maximum() and slide depending on chosen
    // SIMD mode
    max_value = find_maximum_sse128(readBuffer + buffBegin, 0, window_size,
    sse_array);
    return range_scan_geq_sse128(readBuffer + buffBegin, window_size, size,
                                 max_value);
#endif

    return 0;
}



Chunker::~Chunker() {}