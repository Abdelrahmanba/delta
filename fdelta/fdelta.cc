
#include "fdelta.h"

#include "fdelta_interface.h"
constexpr uint64_t CMP_LENGTH = 128;
constexpr uint64_t CMP_LENGTH_SHORT = 8;

uint64_t fencode(unsigned char* inputBuf, uint64_t inputSize,
                 unsigned char* baseBuf, uint64_t baseSize,
                 unsigned char* outputBuf) {
    const unsigned char* const in = (const unsigned char*)inputBuf;
    const unsigned char* const base = (const unsigned char*)baseBuf;
    deltaPtr = outputBuf;

    const unsigned char* const inBeg = in;
    const unsigned char* const baseBeg = base;
    uint64_t curInputSize = inputSize;
    uint64_t curBaseSize = baseSize;
    const unsigned char* inEnd = in + curInputSize;
    const unsigned char* baseEnd = base + curBaseSize;

    // absolute last positions where an N‑byte compare is still valid
    const unsigned char* inEnd128Abs =
        (curInputSize >= CMP_LENGTH) ? inEnd - CMP_LENGTH : inBeg;
    const unsigned char* baseEnd128Abs =
        (curBaseSize >= CMP_LENGTH) ? baseEnd - CMP_LENGTH : baseBeg;
    const unsigned char* inEnd8Abs =
        (curInputSize >= CMP_LENGTH_SHORT) ? inEnd - CMP_LENGTH_SHORT : inBeg;
    const unsigned char* baseEnd8Abs =
        (curBaseSize >= CMP_LENGTH_SHORT) ? baseEnd - CMP_LENGTH_SHORT
                                          : baseBeg;

    auto start = std::chrono::high_resolution_clock::now();

    uint64_t offset = 0;  // canonical positions
    uint64_t baseOffset = 0;
    uint64_t suffixLen = 0;
    uint64_t suffixBaseOffset = 0;

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

        // ---- backward match from the end of both chunks ----
        if (suffixLen == 0) {
            const unsigned char* lowerIn = inBeg + offset;
            const unsigned char* lowerBase = baseBeg + baseOffset;
            const unsigned char* tailIn = inEnd;
            const unsigned char* tailBase = baseEnd;

            while (tailIn >= lowerIn + CMP_LENGTH &&
                   tailBase >= lowerBase + CMP_LENGTH &&
                   memeq_128(tailIn - CMP_LENGTH, tailBase - CMP_LENGTH)) {
                tailIn -= CMP_LENGTH;
                tailBase -= CMP_LENGTH;
            }
            while (tailIn >= lowerIn + 8 && tailBase >= lowerBase + 8) {
                uint64_t a = load_u64(tailIn - 8);
                uint64_t b = load_u64(tailBase - 8);
                if (a != b) break;
                tailIn -= 8;
                tailBase -= 8;
            }
            while (tailIn > lowerIn && tailBase > lowerBase) {
                const unsigned char a = *(tailIn - 1);
                const unsigned char b = *(tailBase - 1);
                if (a != b) break;
                --tailIn;
                --tailBase;
            }

            uint64_t newSuffixLen =
                static_cast<uint64_t>(inEnd - tailIn);
            if (newSuffixLen != 0) {
                suffixLen = newSuffixLen;
                suffixBaseOffset =
                    static_cast<uint64_t>(tailBase - baseBeg);
                curInputSize = static_cast<uint64_t>(tailIn - inBeg);
                curBaseSize = static_cast<uint64_t>(tailBase - baseBeg);
                inEnd = tailIn;
                baseEnd = tailBase;
                inEnd128Abs = (curInputSize >= CMP_LENGTH)
                                  ? inEnd - CMP_LENGTH
                                  : inBeg;
                baseEnd128Abs = (curBaseSize >= CMP_LENGTH)
                                    ? baseEnd - CMP_LENGTH
                                    : baseBeg;
                inEnd8Abs = (curInputSize >= CMP_LENGTH_SHORT)
                                ? inEnd - CMP_LENGTH_SHORT
                                : inBeg;
                baseEnd8Abs = (curBaseSize >= CMP_LENGTH_SHORT)
                                  ? baseEnd - CMP_LENGTH_SHORT
                                  : baseBeg;
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
            while (loopBaseOffset < curBaseSize && n > 0) {
                uint64_t nextBaseChunkSize =
                    nextChunk(baseBuf, loopBaseOffset, curBaseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(baseBuf + loopBaseOffset - hash_length,
                                        hash_length);
                baseChunks.upsert(fp, loopBaseOffset - 8);
                --n;
                _mm_prefetch(
                    (const unsigned char*)(baseBuf + loopBaseOffset + 256),
                    _MM_HINT_T0);
            }
        }

        // ---- probe input chunks against the tiny index ----
        uint64_t loopOffset = offset;
        uint32_t matchedBaseOffset = baseOffset;
        {
            uint64_t n = NUMBER_OF_CHUNKS;
            while (loopOffset < curInputSize && n > 0) {
                uint64_t nextInputChunkSize =
                    nextChunk(inputBuf, loopOffset, curInputSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(inputBuf + loopOffset - hash_length,
                                          hash_length);

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
            // while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
            //        qBase >= lowerBase + CMP_LENGTH_SHORT &&
            //        (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0) {
            //     qIn -= CMP_LENGTH_SHORT;
            //     qBase -= CMP_LENGTH_SHORT;
            // }
            // while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
            //        qBase >= lowerBase + CMP_LENGTH_SHORT) {
            //     uint64_t a, b;
            //     std::memcpy(&a, qIn - CMP_LENGTH_SHORT, CMP_LENGTH_SHORT);
            //     std::memcpy(&b, qBase - CMP_LENGTH_SHORT, CMP_LENGTH_SHORT);
            //     if ((a ^ b) != 0) break;
            //     qIn -= CMP_LENGTH_SHORT;
            //     qBase -= CMP_LENGTH_SHORT;
            // }

            while (qIn >= lowerIn + 8 && qBase >= lowerBase + 8) {
                uint64_t a = load_u64(qIn - 8);
                uint64_t b = load_u64(qBase - 8);
                if (a != b) break;
                qIn -= 8;
                qBase -= 8;
            }
            // step back byte‑by‑byte
            while (qIn > lowerIn && qBase > baseBeg && qBase > lowerBase) {
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
            while (loopBaseOffset < curBaseSize && n > 0) {
                uint64_t nextBaseChunkSize =
                    nextChunk(baseBuf, loopBaseOffset, curBaseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(baseBuf + loopBaseOffset - hash_length,
                                        hash_length);
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

            while (loopOffset < curInputSize && n > 0) {
                uint64_t nextInputChunkSize =
                    nextChunk(inputBuf, loopOffset, curInputSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(inputBuf + loopOffset - hash_length,
                                          hash_length);

                if (baseChunks.find(fp, matchedBaseOffset)) {
                    loopOffset -= 8;
                    break;
                }
                --n;

#if defined(__x86_64__) || defined(_M_X64)
                _mm_prefetch(reinterpret_cast<const unsigned char*>(
                                 inputBuf + loopOffset + 512),
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
                // while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
                //        qBase >= lowerBase + CMP_LENGTH_SHORT &&
                //        (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0) {
                //     qIn -= CMP_LENGTH_SHORT;
                //     qBase -= CMP_LENGTH_SHORT;
                // }

                while (qIn >= lowerIn + 8 && qBase >= lowerBase + 8) {
                    uint64_t a = load_u64(qIn - 8);
                    uint64_t b = load_u64(qBase - 8);
                    if (a != b) break;
                    qIn -= 8;
                    qBase -= 8;
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
    if (LIKELY(offset < curInputSize)) {
        emitADD(inBeg + offset, static_cast<size_t>(curInputSize - offset));
    }
    if (suffixLen != 0) {
        emitCOPY(static_cast<size_t>(suffixBaseOffset),
                 static_cast<size_t>(suffixLen));
    }
    size_t deltaSize = deltaPtr - outputBuf;
    return deltaSize;
}

uint64_t fdecode(unsigned char* deltaBuf, uint64_t deltaSize,
                 unsigned char* baseBuf, uint64_t baseSize,
                 unsigned char* outputBuf) {
    if (!deltaBuf || !baseBuf || !outputBuf)
        throw std::invalid_argument("null pointer argument");

    const unsigned char* p = deltaBuf;
    const unsigned char* end = deltaBuf + deltaSize;
    unsigned char* out = outputBuf;

    int op_id = 0;

    while (p < end) {
        uint8_t header = static_cast<uint8_t>(*p++);
        uint8_t type = header & 0xC0u;
        uint32_t len = header & INLINE_LEN_MAX;

        if (len == INLINE_LEN_MAX) {
            len = INLINE_LEN_MAX + readVarint(p, end);
        }

        if (type == T_ADD) {
            if (static_cast<std::size_t>(end - p) < len)
                throw std::runtime_error("truncated ADD data");
            std::memcpy(out, p, len);
            out += len;
            p += len;
#ifdef DEBUG
            std::cout << "#" << op_id++ << " ADD len=" << len << '\n';
            for (uint32_t i = 0; i < len; i++) {
                std::cout << *(out - len + i);
            }
            std::cout << std::dec << "\n";
#endif
            continue;
        }

        uint32_t addr = 0;
        if (type == T_COPY_A8) {
            if (p >= end) throw std::runtime_error("truncated COPY address");
            addr = static_cast<uint32_t>(*p++);
        } else if (type == T_COPY_A16) {
            if (static_cast<std::size_t>(end - p) < 2)
                throw std::runtime_error("truncated COPY address");
            addr = static_cast<uint32_t>(p[0]) |
                   (static_cast<uint32_t>(p[1]) << 8);
            p += 2;
        } else if (type == T_COPY_V) {
            addr = readVarint(p, end);
        } else {
            throw std::runtime_error("invalid opcode");
        }

        if (addr + len > baseSize)
            throw std::runtime_error("COPY out of bounds");

#ifdef DEBUG
        std::cout << "#" << op_id++ << " COPY addr=" << addr << " len=" << len
                  << '\n';
        for (uint32_t i = 0; i < len; i++) {
            std::cout << *(baseBuf + addr + i);
        }
        std::cout << std::dec << "\n";
#endif
        std::memcpy(out, baseBuf + addr, len);
        out += len;
    }

    return static_cast<uint64_t>(out - outputBuf);
}
