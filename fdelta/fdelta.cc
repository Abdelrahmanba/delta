
#include "fdelta.h"
#include "fdelta_interface.h"
uint64_t fencode(unsigned char* inputBuf, uint64_t inputSize, unsigned char* baseBuf,
                 uint64_t baseSize, unsigned char* outputBuf) {
    constexpr uint64_t CMP_LENGTH = 128;
    constexpr uint64_t CMP_LENGTH_SHORT = 8;

    const unsigned char* const in = (const unsigned char*)inputBuf;
    const unsigned char* const base = (const unsigned char*)baseBuf;
    deltaPtr = outputBuf;

    const uint64_t inSize = inputSize;

    const unsigned char* const inBeg = in;
    const unsigned char* const baseBeg = base;
    const unsigned char* const inEnd = in + inSize;
    const unsigned char* const baseEnd = base + baseSize;

    // absolute last positions where an N‑byte compare is still valid
    const unsigned char* const inEnd128Abs =
        (inSize >= CMP_LENGTH) ? inEnd - CMP_LENGTH : inBeg;
    const unsigned char* const baseEnd128Abs =
        (baseSize >= CMP_LENGTH) ? baseEnd - CMP_LENGTH : baseBeg;
    const unsigned char* const inEnd8Abs =
        (inSize >= CMP_LENGTH_SHORT) ? inEnd - CMP_LENGTH_SHORT : inBeg;
    const unsigned char* const baseEnd8Abs =
        (baseSize >= CMP_LENGTH_SHORT) ? baseEnd - CMP_LENGTH_SHORT : baseBeg;

    auto start = std::chrono::high_resolution_clock::now();

    uint64_t offset = 0;  // canonical positions
    uint64_t baseOffset = 0;

    // main loop
    for (;;) {
        // Stop if either stream cannot sustain another 128‑byte probe.
        if ((inBeg + offset) > inEnd128Abs ||
            (baseBeg + baseOffset) > baseEnd128Abs) {
            break;
        }

        // ---- forward match with fixed sizes (128 then 8) ----
        const unsigned char* pIn = inBeg + offset;
        const unsigned char* pBase = baseBeg + baseOffset;

        while (pIn <= inEnd128Abs && pBase <= baseEnd128Abs &&
               //    std::memcmp(pIn, pBase, CMP_LENGTH) == 0)
               (memeq_128(pIn, pBase))) {
            pIn += CMP_LENGTH;
            pBase += CMP_LENGTH;
        }
        while (pIn <= inEnd8Abs && pBase <= baseEnd8Abs &&
               (*(uint64_t*)pIn ^ *(uint64_t*)pBase) == 0) {
            pIn += CMP_LENGTH_SHORT;
            pBase += CMP_LENGTH_SHORT;
        }

        // if we advanced, emit COPY for the matched run
        {
            uint64_t advanced = static_cast<uint64_t>(pIn - (inBeg + offset));
            if (advanced != 0) {
                emitCOPY(baseOffset, advanced);
                offset += advanced;
                baseOffset += advanced;
            }
        }

        // If we ran out of room for more 128‑byte compares or one stream ended,
        // stop.
        if (UNLIKELY((inBeg + offset) > inEnd128Abs ||
                     (baseBeg + baseOffset) > baseEnd128Abs ||
                     (inBeg + offset) >= inEnd ||
                     (baseBeg + baseOffset) >= baseEnd)) {
            break;
        }

        // ---- build tiny index over next base chunks (NUMBER_OF_CHUNKS == 16)
        // ----
        uint64_t loopBaseOffset = baseOffset;
        baseChunks.clear();

        {
            uint64_t n = NUMBER_OF_CHUNKS;
            while (loopBaseOffset < baseSize && n > 0) {
                uint64_t nextBaseChunkSize =
                    nextChunk(baseBuf, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(
                    baseBuf + loopBaseOffset - hash_length, hash_length);
                baseChunks.upsert(fp, loopBaseOffset - 8);
                --n;
                _mm_prefetch((const unsigned char*)(baseBuf + loopBaseOffset + 256),
                             _MM_HINT_T0);
            }
        }

        // ---- probe input chunks against the tiny index ----
        uint64_t loopOffset = offset;
        uint32_t matchedBaseOffset = baseOffset;
        {
            uint64_t n = NUMBER_OF_CHUNKS;
            while (loopOffset < inSize && n > 0) {
                uint64_t nextInputChunkSize =
                    nextChunk(inputBuf, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(
                    inputBuf + loopOffset - hash_length, hash_length);

                if (baseChunks.find(fp, matchedBaseOffset)) {
                    // align to start of match
                    loopOffset -= 8;
                    break;
                }
                --n;
            }
        }

        if (matchedBaseOffset != baseOffset) {
            // ---- backtrace from the found match to extend backwards ----
            const unsigned char* lowerIn = inBeg + offset;
            const unsigned char* lowerBase = baseBeg + baseOffset;

            const unsigned char* qIn = inBeg + loopOffset;
            const unsigned char* qBase = baseBeg + matchedBaseOffset;
            // step back by 8B chunks
            while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
                   qBase >= lowerBase + CMP_LENGTH_SHORT &&
                   (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0) {
                qIn -= CMP_LENGTH_SHORT;
                qBase -= CMP_LENGTH_SHORT;
            }
            // step back byte‑by‑byte
            while (qIn > lowerIn && qBase > baseBeg) {
                const unsigned char a = *(qIn - 1);
                const unsigned char b = *(qBase - 1);
                if (a != b) break;
                --qIn;
                --qBase;
            }

            // emit ADD for the insertion gap, if any
            if (qIn > lowerIn) {
                emitADD(lowerIn, static_cast<size_t>(qIn - lowerIn));
            }
            // emit COPY for the matched backward extension
            if (qBase != (baseBeg + matchedBaseOffset)) {
                emitCOPY(
                    static_cast<size_t>(qBase - baseBeg),
                    static_cast<size_t>((baseBeg + matchedBaseOffset) - qBase));
            }

            // advance canonical offsets to the forward match positions
            offset = loopOffset;
            baseOffset = matchedBaseOffset;
            continue;  // next outer iteration
        }

        // -----------------------------
        // No match found in tiny index: try big‑chunk search, then fallback to
        // ADD.
        // -----------------------------

        // more base chunks
        {
            uint64_t n =
                NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER - NUMBER_OF_CHUNKS;
            while (loopBaseOffset < baseSize && n > 0) {
                uint64_t nextBaseChunkSize =
                    nextChunk(baseBuf, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(
                    baseBuf + loopBaseOffset - hash_length, hash_length);
                baseChunks.upsert(fp, loopBaseOffset - 8);
                --n;

#if defined(__x86_64__) || defined(_M_X64)
                _mm_prefetch(reinterpret_cast<const unsigned char*>(
                                 baseBuf + loopBaseOffset + 512),
                             _MM_HINT_T0);
#endif
            }
        }

        // re‑probe input with big chunks
        {
            uint64_t n = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;
            loopOffset = offset;             // reset
            matchedBaseOffset = baseOffset;  // reset

            while (loopOffset < inSize && n > 0) {
                uint64_t nextInputChunkSize =
                    nextChunk(inputBuf, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(
                    inputBuf + loopOffset - hash_length, hash_length);

                if (baseChunks.find(fp, matchedBaseOffset)) {
                    loopOffset -= 8;
                    break;
                }
                --n;

#if defined(__x86_64__) || defined(_M_X64)
                _mm_prefetch(reinterpret_cast<const unsigned char*>(inputBuf +
                                                           loopOffset + 512),
                             _MM_HINT_T0);
#endif
            }

            const unsigned char* lowerIn = inBeg + offset;
            const unsigned char* lowerBase = baseBeg + baseOffset;

            if (matchedBaseOffset != baseOffset) {
                // backtrace with larger steps first (128 then 8) to be
                // symmetrical with big search
                const unsigned char* qIn = inBeg + loopOffset;
                const unsigned char* qBase = baseBeg + matchedBaseOffset;

                while (qIn >= lowerIn + CMP_LENGTH &&
                       qBase >= lowerBase + CMP_LENGTH &&
                       memeq_128(qIn - CMP_LENGTH, qBase - CMP_LENGTH)) {
                    qIn -= CMP_LENGTH;
                    qBase -= CMP_LENGTH;
                }
                while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
                       qBase >= lowerBase + CMP_LENGTH_SHORT &&
                       (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0) {
                    qIn -= CMP_LENGTH_SHORT;
                    qBase -= CMP_LENGTH_SHORT;
                }

                if (qIn > lowerIn) {
                    emitADD(lowerIn, static_cast<size_t>(qIn - lowerIn));
                }
                if (qBase != (baseBeg + matchedBaseOffset)) {
                    emitCOPY(static_cast<size_t>(qBase - baseBeg),
                             static_cast<size_t>((baseBeg + matchedBaseOffset) -
                                                 qBase));
                }

                offset = loopOffset;
                baseOffset = matchedBaseOffset;
                continue;
            } else {
                // still no match → emit ADD for what we advanced (if any), then
                // advance both streams
                if (loopOffset > offset) {
                    emitADD(inBeg + offset,
                            static_cast<size_t>(loopOffset - offset));
                    offset = loopOffset;
                    baseOffset = loopBaseOffset;  // progress base along with
                                                  // what we indexed
                } else {
                    // ensure forward progress to avoid infinite loop
                    emitADD(inBeg + offset, 1);
                    ++offset;
                    ++baseOffset;
                }
                continue;
            }
        }
    }  // end for(;;)

    // Tail: emit remaining input
    if (LIKELY(offset < inSize)) {
        emitADD(inBeg + offset, static_cast<size_t>(inSize - offset));
    }
    size_t deltaSize = deltaPtr - outputBuf;
    return deltaSize;   

}
