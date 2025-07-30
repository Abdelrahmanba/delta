#include "delta.h"

static fs::path DATA_DIR =
// "/mnt/data/test_data/storage";  // default data directory
// "/mnt/data/test_data/vm_fin_Storage";  // default data directory
// "/mnt/data/test_data/odess_docker_storage";  // default data directory
// "/mnt/data/test_data/odess_debian_storage";  // default data directory
// "/mnt/data/test_data/odess_vm_storage";  // default data directory
// "/mnt/data/test_data/odess_gnu_storage";  // default data directory
"/mnt/data/test_data/linux_storage";  // default data directory


static fs::path delta_map =
// "./delta_map_failing.csv";  // default delta map file
// "/mydata/fdedup/test_data/delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/vm_vn_delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_docker_delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_docker_delta_map.csv";  // default delta map file
// "/mnt/data/test_data/odess_gnu_delta_map.csv";  // default delta map file
"./delta_map_full.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_debian_delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_vm_delta_map.csv";  // default delta map file



void deltaCompressGdelta(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t dSize = 0;
    gencode(reinterpret_cast<uint8_t*>(bufInputChunk), sizeInputChunk,
            reinterpret_cast<uint8_t*>(bufBaseChunk), sizeBaseChunk,
            reinterpret_cast<uint8_t**>(&deltaPtr), &dSize);         // base offset is 0 for now
    deltaPtr += dSize;  // advance delta pointer


    // if (dSize > 1024) {
    //     int result = LZ4_compress_fast(reinterpret_cast<const char*>(bufDeltaChunk),
    //         lz4ptr, dSize, 65536, COMPRESSION_LEVEL);
    //     deltaPtr = lz4ptr + result;  // update delta pointer to the end of compressed data

    // }
    // else {
    //     lz4ptr = bufDeltaChunk;  // if delta is small, use the original buffer
    // }
    auto end = std::chrono::high_resolution_clock::now();

    durationGdelta +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
        .count();
        // writeDeltaChunk();
}


void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);

    constexpr uint32_t CMP_LENGTH = 128;
    constexpr uint32_t CMP_LENGTH_SHORT = 8;

    const char* const in = bufInputChunk;
    const char* const base = bufBaseChunk;

    const uint32_t inSize = sizeInputChunk;
    const uint32_t baseSize = sizeBaseChunk;

    const char* const inBeg = in;
    const char* const baseBeg = base;
    const char* const inEnd = in + inSize;
    const char* const baseEnd = base + baseSize;

    // absolute last positions where an N‑byte compare is still valid
    const char* const inEnd128Abs = (inSize >= CMP_LENGTH) ? inEnd - CMP_LENGTH : inBeg;
    const char* const baseEnd128Abs = (baseSize >= CMP_LENGTH) ? baseEnd - CMP_LENGTH : baseBeg;
    const char* const inEnd8Abs = (inSize >= CMP_LENGTH_SHORT) ? inEnd - CMP_LENGTH_SHORT : inBeg;
    const char* const baseEnd8Abs = (baseSize >= CMP_LENGTH_SHORT) ? baseEnd - CMP_LENGTH_SHORT : baseBeg;

    auto start = std::chrono::high_resolution_clock::now();

    uint32_t offset = 0; // canonical positions
    uint32_t baseOffset = 0;

    // main loop
    for (;;) {
        // Stop if either stream cannot sustain another 128‑byte probe.
        if ((inBeg + offset) > inEnd128Abs ||
            (baseBeg + baseOffset) > baseEnd128Abs) {
            break;
        }

        // ---- forward match with fixed sizes (128 then 8) ----
        const char* pIn = inBeg + offset;
        const char* pBase = baseBeg + baseOffset;

        while (pIn <= inEnd128Abs &&
               pBase <= baseEnd128Abs &&
            //    std::memcmp(pIn, pBase, CMP_LENGTH) == 0)
               (memeq_128(pIn, pBase))
            )
        {
            pIn += CMP_LENGTH;
            pBase += CMP_LENGTH;
        }
        while (pIn <= inEnd8Abs &&
               pBase <= baseEnd8Abs &&
               (*(uint64_t*)pIn ^ *(uint64_t*)pBase) == 0)
        {
            pIn += CMP_LENGTH_SHORT;
            pBase += CMP_LENGTH_SHORT;
        }

        // if we advanced, emit COPY for the matched run
        {
            uint32_t advanced = static_cast<uint32_t>(pIn - (inBeg + offset));
            if (advanced != 0) {
                emitCOPY(baseOffset, advanced);
                offset += advanced;
                baseOffset += advanced;
            }
        }

        // If we ran out of room for more 128‑byte compares or one stream ended, stop.
        if (UNLIKELY((inBeg + offset) > inEnd128Abs ||
            (baseBeg + baseOffset) > baseEnd128Abs ||
            (inBeg + offset) >= inEnd ||
            (baseBeg + baseOffset) >= baseEnd)) {
            break;
        }

        // ---- build tiny index over next base chunks (NUMBER_OF_CHUNKS == 16) ----
        uint32_t loopBaseOffset = baseOffset;
        baseChunks.clear();

        {
            uint32_t n = NUMBER_OF_CHUNKS;
            while (loopBaseOffset < baseSize && n > 0) {
                uint32_t nextBaseChunkSize = nextChunk(bufBaseChunk, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(bufBaseChunk + loopBaseOffset - hash_length, hash_length);
                baseChunks.upsert(fp, loopBaseOffset - 8);
                --n;
                _mm_prefetch((const char*)(bufBaseChunk + loopBaseOffset + 256), _MM_HINT_T0);

            }
        }

        // ---- probe input chunks against the tiny index ----
        uint32_t loopOffset = offset;
        uint32_t matchedBaseOffset = baseOffset;
        {
            uint32_t n = NUMBER_OF_CHUNKS;
            while (loopOffset < inSize && n > 0) {
                uint32_t nextInputChunkSize = nextChunk(bufInputChunk, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(bufInputChunk + loopOffset - hash_length, hash_length);


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
            const char* lowerIn = inBeg + offset;
            const char* lowerBase = baseBeg + baseOffset;

            const char* qIn = inBeg + loopOffset;
            const char* qBase = baseBeg + matchedBaseOffset;

            // step back by 8B chunks
            while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
                   qBase >= lowerBase + CMP_LENGTH_SHORT &&
                   (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0)
            {
                qIn -= CMP_LENGTH_SHORT;
                qBase -= CMP_LENGTH_SHORT;
            }
            // step back byte‑by‑byte
            while (qIn > lowerIn && qBase > baseBeg) {
                const char a = *(qIn - 1);
                const char b = *(qBase - 1);
                if (a != b) break;
                --qIn; --qBase;
            }

            // emit ADD for the insertion gap, if any
            if (qIn > lowerIn) {
                emitADD(lowerIn, static_cast<size_t>(qIn - lowerIn));
            }
            // emit COPY for the matched backward extension
            if (qBase != (baseBeg + matchedBaseOffset)) {
                emitCOPY(static_cast<size_t>(qBase - baseBeg),
                         static_cast<size_t>((baseBeg + matchedBaseOffset) - qBase));
            }

            // advance canonical offsets to the forward match positions
            offset = loopOffset;
            baseOffset = matchedBaseOffset;
            continue; // next outer iteration
        }

        // -----------------------------
        // No match found in tiny index: try big‑chunk search, then fallback to ADD.
        // -----------------------------

        // more base chunks
        {
            uint32_t n = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER - NUMBER_OF_CHUNKS;
            while (loopBaseOffset < baseSize && n > 0) {
                uint32_t nextBaseChunkSize = nextChunk(bufBaseChunk, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(bufBaseChunk + loopBaseOffset - hash_length, hash_length);
                baseChunks.upsert(fp, loopBaseOffset - 8);
                --n;

#if defined(__x86_64__) || defined(_M_X64)
                _mm_prefetch(reinterpret_cast<const char*>(bufBaseChunk + loopBaseOffset + 512), _MM_HINT_T0);
#endif
            }
        }

        // re‑probe input with big chunks
        {
            uint32_t n = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;
            loopOffset = offset;                 // reset
            matchedBaseOffset = baseOffset;      // reset

            while (loopOffset < inSize && n > 0) {
                uint32_t nextInputChunkSize = nextChunk(bufInputChunk, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(bufInputChunk + loopOffset - hash_length, hash_length);

                if (baseChunks.find(fp, matchedBaseOffset)) {
                    loopOffset -= 8;
                    break;
                }
                --n;

#if defined(__x86_64__) || defined(_M_X64)
                _mm_prefetch(reinterpret_cast<const char*>(bufInputChunk + loopOffset + 512), _MM_HINT_T0);
#endif
            }

            const char* lowerIn = inBeg + offset;
            const char* lowerBase = baseBeg + baseOffset;

            if (matchedBaseOffset != baseOffset) {
                // backtrace with larger steps first (128 then 8) to be symmetrical with big search
                const char* qIn = inBeg + loopOffset;
                const char* qBase = baseBeg + matchedBaseOffset;

                while (qIn >= lowerIn + CMP_LENGTH &&
                       qBase >= lowerBase + CMP_LENGTH &&
                       memeq_128(qIn - CMP_LENGTH, qBase - CMP_LENGTH))
                {
                    qIn -= CMP_LENGTH;
                    qBase -= CMP_LENGTH;
                }
                while (qIn >= lowerIn + CMP_LENGTH_SHORT &&
                       qBase >= lowerBase + CMP_LENGTH_SHORT &&
                       (*(uint64_t*)qIn ^ *(uint64_t*)qBase) == 0)
                {
                    qIn -= CMP_LENGTH_SHORT;
                    qBase -= CMP_LENGTH_SHORT;
                }

                if (qIn > lowerIn) {
                    emitADD(lowerIn, static_cast<size_t>(qIn - lowerIn));
                }
                if (qBase != (baseBeg + matchedBaseOffset)) {
                    emitCOPY(static_cast<size_t>(qBase - baseBeg),
                             static_cast<size_t>((baseBeg + matchedBaseOffset) - qBase));
                }

                offset = loopOffset;
                baseOffset = matchedBaseOffset;
                continue;
            }
            else {
             // still no match → emit ADD for what we advanced (if any), then advance both streams
                if (loopOffset > offset) {
                    emitADD(inBeg + offset, static_cast<size_t>(loopOffset - offset));
                    offset = loopOffset;
                    baseOffset = loopBaseOffset; // progress base along with what we indexed
                }
                else {
                 // ensure forward progress to avoid infinite loop
                    emitADD(inBeg + offset, 1);
                    ++offset;
                    ++baseOffset;
                }
                continue;
            }
        }
    } // end for(;;)

    // Tail: emit remaining input
    if (LIKELY(offset < inSize)) {
        emitADD(inBeg + offset, static_cast<size_t>(inSize - offset));
    }
    size_t deltaSize = deltaPtr - bufDeltaChunk;

    // if (deltaSize > 1024) {  // if delta is large enough, compress it
    //     int result = LZ4_compress_fast(reinterpret_cast<const char*>(bufDeltaChunk),
    //         lz4ptr, deltaSize, 65536, COMPRESSION_LEVEL);
    //     deltaPtr = lz4ptr + result;  // update delta pointer to the end of compressed data
    // }
    // else {
    //     lz4ptr = bufDeltaChunk;  // if delta is small, use the original buffer
    // // }



    // std::cout << "LZ4 compression result: " << result << " original size: "
    //           << (deltaPtr - bufDeltaChunk) << " \n";


    auto end = std::chrono::high_resolution_clock::now();
    durationDelta += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    // writeDeltaChunk();

}


// void deltaCompressEDelta(const fs::path& origPath, const fs::path& basePath) {
//     readBaseChunk(basePath);
//     readInputChunk(origPath);
//     auto start = std::chrono::high_resolution_clock::now();
//     uint32_t dSize = 0;
//     EDeltaEncode(reinterpret_cast<uint8_t*>(bufInputChunk), sizeInputChunk,
//                   reinterpret_cast<uint8_t*>(bufBaseChunk), sizeBaseChunk,
//                   reinterpret_cast<uint8_t*>(bufDeltaChunk), &dSize);
//     deltaPtr += dSize;  // advance delta pointer
//     // std::cout << "Generated delta of size: " << dSize << '\n';
//     auto end = std::chrono::high_resolution_clock::now();

//     duration +=
//         std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
//         .count();
//     // writeDeltaChunk();
// }


int main(int argc, char* argv[]) try {

    const fs::path inCsv = argc > 1 ? fs::path{ argv[1] } : delta_map;
    if (argc > 3) DATA_DIR = fs::path{ argv[3] };  // optional override

    std::ifstream in(inCsv);
    if (!in) {
        std::cerr << "Cannot open " << inCsv << '\n';
        return 1;
    }

    std::string header, line;
    std::getline(in, header);  // skip original header line
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto fields = splitCSV(line);
        if (fields.size() < 6) {
            std::cerr << "Bad CSV line: " << line << '\n';
            continue;
        }

        const std::string& delta_id = fields[0];
        const fs::path origPath = DATA_DIR / fields[1];
        const fs::path basePath = DATA_DIR / fields[2];
#ifdef VORBSE
        std::cout << "Processing delta ID: " << delta_id
            << ", Original: " << origPath << ", Base: " << basePath
            << '\n';
#endif



                // deltaPtr = bufDeltaChunk;  // reset delta pointer
                // lz4ptr = lz4Buff;
                // deltaCompressGdelta(origPath, basePath);
                // totalGDeltaSize += (deltaPtr - bufDeltaChunk);  // accumulate new delta size

                // totalGDeltaSize += (deltaPtr - lz4ptr);  // accumulate new delta size




        deltaPtr = bufDeltaChunk;  // reset delta pointer
        lz4ptr = lz4Buff;
        deltaCompress(origPath, basePath);
        totalDeltaSize += (deltaPtr - bufDeltaChunk);  // accumulate new delta size

        // totalDeltaSize += (deltaPtr - lz4ptr);  // accumulate new delta size





#ifdef VORBSE
        std::cout << "Generated delta of size of: " << (deltaPtr - bufDeltaChunk)
            << " bytes\n";
#endif
// if (decode(bufDeltaChunk, deltaPtr - bufDeltaChunk, bufBaseChunk,
//     sizeBaseChunk, bufInputChunk, sizeInputChunk)) {
//     std::cout << "Decoded output matches the input chunk.\n";
// }
// else {
//     std::cerr << "Decoded output does NOT match the input chunk!\n";
//     // return 1;
// }

    }
    std::cout << "GDELTA DCE : " << ((1 - (static_cast<double>(totalGDeltaSize) / (totalSize))) * 100.0) << "%\n";
    std::cout << "GDELTA DCR: " << (static_cast<double>(totalSize) / totalGDeltaSize) << '\n';
    double gthroughput = (static_cast<double>(totalSize) / (1024 * 1024)) / (durationGdelta / 1000000000.0);  // MB/s
    std::cout << "GDELTA Throughput: " << gthroughput << " MB/s" << std::endl;

    std::cout << "DELTA DCE : " << ((1 - (static_cast<double>(totalDeltaSize) / (totalSize))) * 100.0) << "%\n";
    std::cout << "DELTA DCR: " << (static_cast<double>(totalSize) / totalDeltaSize) << '\n';
    double dthroughput = (static_cast<double>(totalSize) / (1024 * 1024)) / (durationDelta / 1000000000.0);  // MB/s
    std::cout << "DELTA Throughput: " << dthroughput << " MB/s" << std::endl;

    return 0;
}
catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
