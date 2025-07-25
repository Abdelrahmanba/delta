#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "chunker.hpp"
#define XXH_INLINE_ALL
#include <string.h>

#include <chrono>
#include <cstring>
#include <unordered_map>

#include "decode.hpp"
#include "gdelta.h"
#include "xxhash.h"

#define hash_length 48
// #define DEBUG
#define NUMBER_OF_CHUNKS 4
#define CHUNKS_MULTIPLIER 6


namespace fs = std::filesystem;

double duration = 0.0;  // global duration for performance measurement
size_t totalSize = 0;
size_t totalDeltaSize = 0;  // total size of all input chunks

// ─────────────────────────────────────── Config
// ──────────────────────────────────────
static fs::path DATA_DIR =
"../../storage";  // default data directory
// static fs::path DATA_DIR = "/mnt/data/delta/build/";

static fs::path delta_map =
"./delta_map_failing.csv";  // default delta map file

Chunker* chunker = new Chunker();  // global chunker instance

// Handy POD to keep per-file stats
struct Stats {
    std::size_t chunks_orig{ 0 };
    std::size_t chunks_base{ 0 };
    std::size_t matched{ 0 };
    std::size_t unique{ 0 };
    std::uintmax_t bytes_orig{ 0 };
    std::uintmax_t bytes_base{ 0 };
};

using Hash64 = std::uint64_t;

// Split a CSV line on commas, preserving empty fields
std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> out;
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, ',')) out.push_back(std::move(cell));
    return out;
}

char bufBaseChunk[1024 * 1024];
char bufInputChunk[1024 * 1024];

char bufDeltaChunk[1024 * 1024];
char* deltaPtr =
bufDeltaChunk;  // pointer to the current position in the delta buffer

size_t sizeBaseChunk = 0;
size_t sizeInputChunk = 0;

void readBaseChunk(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    char* data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(bufBaseChunk)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(bufBaseChunk, size);
    sizeBaseChunk = static_cast<size_t>(size);
}

void readInputChunk(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    char* data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(bufInputChunk)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(bufInputChunk, size);
    sizeInputChunk = static_cast<size_t>(size);
    totalSize += sizeInputChunk;
}
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

    // Print emitted bytes as chars
    #ifdef DEBUG
    std::cout << "ADD instruction bytes: ";
    for (size_t i = 0; i < len; i++) {
        char c = data[i];
        if (isprint(c)) {
            std::cout << c;
        } else {
            std::cout << '.';
        }
    }
    std::cout << std::endl;
    #endif
}

inline void emitCOPY(size_t addr, size_t len) {
    *deltaPtr++ = 0x25;  // COPY (var-size, mode 0)
    writeVarint(static_cast<uint32_t>(len));
    writeVarint(static_cast<uint32_t>(addr));  // dictionary offset
}
// -----------------------------------------------------------------
struct chunk {
    uint64_t offset;  // offset in the original data
    size_t size;      // length of the chunk
};

void writeDeltaChunk() {
    size_t sizeDelta = static_cast<size_t>(deltaPtr - bufDeltaChunk);

    auto hash = XXH3_64bits(bufDeltaChunk, sizeDelta);

    std::ofstream deltaOut("./deltas/" + std::to_string(hash),
                           std::ios::binary);
    if (deltaOut) {
        deltaOut.write(bufDeltaChunk, sizeDelta);
        #ifdef VORBSE
        std::cout << "Delta file written: "
            << "./deltas/" + std::to_string(hash)
            << " with size: " << sizeDelta << '\n';
        #endif
    }
    else
        std::cerr << "Cannot write delta file\n";
}

std::unordered_map<uint64_t, uint64_t> deltaChunks;  // map of delta chunks

void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);

    auto start = std::chrono::high_resolution_clock::now();

    size_t offset = 0;
    size_t baseOffset = 0;
    size_t size = sizeInputChunk > sizeBaseChunk ? sizeBaseChunk : sizeInputChunk;
    while (offset < size) {
#ifdef DEBUG
        std::cout << "Processing offset: " << offset
            << ", baseOffset: " << baseOffset << '\n';
#endif
        uint64_t baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + baseOffset));
        uint64_t inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + offset));
        size_t loopOffset = offset;  // save the original offset for later use
        size_t loopBaseOffset = baseOffset;  // save the original base offset
        while (inputValue == baseValue && loopOffset + 8 <= sizeInputChunk
            && loopBaseOffset + 8 <= sizeBaseChunk) {
#ifdef DEBUG
            std::cout << "Matching 8 bytes at offset: " << loopOffset << ", base offset: " << loopBaseOffset << ", base value: " << baseValue << ", input value: " << inputValue << '\n';
#endif
            loopBaseOffset += 8;
            baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + loopBaseOffset));
            loopOffset += 8;
            inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
        }
        if (offset != loopOffset) {
            emitCOPY(baseOffset, loopOffset - offset);
#ifdef DEBUG
            std::cout << "Emitted COPY at of size: " << loopOffset - offset
                << " from offset: " << offset
                << " to offset: " << loopOffset << '\n';
#endif
        }
        offset = loopOffset;          // update offset to the new position
        baseOffset = loopBaseOffset;  // update baseOffset to the new position

        // Region of change. We try to find a data anchor

        // before that let's make sure the base buffer is not exhausted
        if (baseOffset + 8 >= sizeBaseChunk) {
            // std::cout << "Base chunk stream ended.\n";
            break;  // no more base chunks to match against
        }
        std::unordered_map<uint64_t, size_t> baseChunks;
        size_t numberOfchunks = NUMBER_OF_CHUNKS;
        while (loopBaseOffset < sizeBaseChunk && numberOfchunks > 0) {
            size_t nextBaseChunkSize =
                chunker->nextChunk(bufBaseChunk, loopBaseOffset, sizeBaseChunk);
            loopBaseOffset += nextBaseChunkSize;
            baseChunks[fingerprint] = loopBaseOffset - 8;
            numberOfchunks--;
#ifdef DEBUG
            std::cout << "[first] Next base chunk size: " << nextBaseChunkSize
                << ", loop base offset: " << loopBaseOffset << '\n';
#endif
        }
        // this means the base chunk stream has ended. TODO: Don't bother with
        // the rest. emit Add and return.
        if (numberOfchunks != 0) {
#ifdef DEBUG
            std::cout << "Base chunk stream ended before reaching 3 chunks.\n";
#endif
        }
        numberOfchunks = NUMBER_OF_CHUNKS;

        // init the matchedOffset
        size_t matchedBaseOffset = baseOffset;
        while (loopOffset < sizeInputChunk && numberOfchunks > 0) {
            size_t nextInputChunkSize = chunker->nextChunk(bufInputChunk, loopOffset, sizeInputChunk);
            loopOffset += nextInputChunkSize;
#ifdef DEBUG
            std::cout << "[first] Next input chunk size: " << nextInputChunkSize
                << ", loop offset: " << loopOffset << '\n';
#endif
            // query base index for a match
            auto it = baseChunks.find(fingerprint);
            if (it != baseChunks.end()) {
                matchedBaseOffset = it->second;
                loopOffset -= 8;  // to the start of the mattch
                // stick with the first match TODO: verify that its a good matching point
#ifdef DEBUG
                std::cout << "[FIRST] Matched base offset: " << matchedBaseOffset
                    << ", loop offset: " << loopOffset << '\n';
#endif
                break;
            }
            numberOfchunks--;
        }
        // a match is found. back trace then update offsets.
        size_t currOff = loopOffset;
        size_t currBaseOffset = matchedBaseOffset;
        if (matchedBaseOffset != baseOffset) {
#ifdef DEBUG
            std::cout << "Matched base offset: " << matchedBaseOffset
                << ", curr offset: " << currOff << '\n';
#endif
            // pointers to the matched offsets
            baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
            inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
            while (inputValue == baseValue && loopOffset >= (offset + 8) &&
                matchedBaseOffset >= 8) {
#ifdef DEBUG
                std::cout << "Backtracing to offset: " << loopOffset
                    << ", matched base offset: " << matchedBaseOffset
                    << ", base value: " << baseValue
                    << ", input value: " << inputValue << '\n';
#endif
                loopOffset -= 8;  // backtrace by 8 bytes
                matchedBaseOffset -= 8;
                inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
                baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
            }
#ifdef DEBUG
            std::cout << "Backtraced to offset: " << loopOffset
                << ", matched base offset: " << matchedBaseOffset
                << ", current offset: " << loopOffset
                << ", current base offset: " << currBaseOffset << '\n';
#endif
            if (matchedBaseOffset != currBaseOffset) {
                loopOffset += 8;             // advance offset by 8 bytes
                matchedBaseOffset += 8;  // advance base offset by 8 bytes
            }
            // finer grain backtrace
            uint8_t inputValue_8 =
                *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
            uint8_t baseValue_8 =
                *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
            size_t beforeLoopOffset = loopOffset;
            while (inputValue_8 == baseValue_8 && matchedBaseOffset > 0) {
#ifdef DEBUG
                std::cout << "Backtracing to offset: " << loopOffset
                    << ", matched base offset: " << matchedBaseOffset
                    << ", base offset: " << baseOffset
                    << ", current Base offset: " << currBaseOffset
                    << ", matched base offset: " << matchedBaseOffset << "\n";
#endif
                if (loopOffset == offset) {
                    break;  // we are back at the current offset
                }
                loopOffset -= 1;  // backtrace by 1 byte
                matchedBaseOffset -= 1;
                inputValue_8 = *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
                baseValue_8 = *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
            }
            if (loopOffset != beforeLoopOffset) {
                loopOffset += 1;             // advance offset by 1 byte
                matchedBaseOffset += 1;  // advance base offset by 1 byte
            }
#ifdef DEBUG
            std::cout << "Backtraced to offset: " << loopOffset
                << ", matched base offset: " << matchedBaseOffset
                << ", base value: " << baseValue
                << ", input value: " << inputValue << '\n';
#endif

// either we are back at offset (if no inserations happend in input chunk) or we found a mismatch:
// 1. emit add from offset to loopOffset
// 2. emit copy from to loopOffset to offset

// insertation happened
            if (loopOffset > offset) {
#ifdef DEBUG
                std::cout << "inseration happend offset (EMIT ADD): " << offset
                    << ", currentOffset: " << loopOffset << " of size "
                    << (loopOffset - offset) << '\n';
#endif
                emitADD(bufInputChunk + offset, loopOffset - offset);
            }

            if (matchedBaseOffset != currBaseOffset) {
#ifdef DEBUG
                std::cout << "Emitting COPY from base offset: " << matchedBaseOffset
                    << ", to current base offset: " << currBaseOffset
                    << ", size: " << (currBaseOffset - matchedBaseOffset)
                    << '\n';
#endif     
                emitCOPY(matchedBaseOffset, currBaseOffset - matchedBaseOffset);
            }
            if(currOff !=  loopOffset){
                offset = currOff;
            }
            // prepare offset and baseOffset for next iteration
            baseOffset = currBaseOffset;
        }
        // if no match was found, we just emit ADD. This is what i will optimize now
        else {
            // try with more chunks
#ifdef DEBUG
            std::cout << "[FIRST] No match was found at offset: " << offset << '\n';
#endif

            numberOfchunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER - NUMBER_OF_CHUNKS;
            while (loopBaseOffset < sizeBaseChunk && numberOfchunks > 0) {
                size_t nextBaseChunkSize =
                    chunker->nextChunk(bufBaseChunk, loopBaseOffset, sizeBaseChunk);
                loopBaseOffset += nextBaseChunkSize;
#ifdef DEBUG
                std::cout << "[SECOND] Next base chunk size: " << nextBaseChunkSize
                    << ", loop base offset: " << loopBaseOffset << " Fingerprint: " << fingerprint << '\n';
#endif
                baseChunks[fingerprint] = loopBaseOffset - 8;
                numberOfchunks--;
            }
            // this means the base chunk stream has ended. TODO: Don't bother with
            // the rest. emit Add and return.
            if (numberOfchunks != 0) {
#ifdef DEBUG
                std::cout << "Base chunk stream ended before reaching 3 chunks.\n";
#endif
            }
            numberOfchunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;  // reset for input chunk
            loopOffset = offset;  // reset loopOffset
            // init the matchedOffset
            size_t matchedBaseOffset = baseOffset;
            while (loopOffset < sizeInputChunk && numberOfchunks > 0) {
                size_t nextInputChunkSize = chunker->nextChunk(bufInputChunk, loopOffset, sizeInputChunk);
                loopOffset += nextInputChunkSize;
#ifdef DEBUG
                std::cout << "[SECOND] Next input chunk size: " << nextInputChunkSize <<
                    ", loop offset: " << loopOffset << " Fingerprint: " << fingerprint << '\n';
#endif
// query base index for a match
                auto it = baseChunks.find(fingerprint);
                if (it != baseChunks.end()) {
                    matchedBaseOffset = it->second;
                    loopOffset -= 8;  // to the start of the mattch
#ifdef DEBUG
                    std::cout << "[SECOND] Matched base offset: " << matchedBaseOffset
                        << ", loop offset: " << loopOffset << '\n';
#endif
                    // stick with the first match TODO: verify that its a good matching point
                    break;
                }
                numberOfchunks--;
            }

            size_t currOff = loopOffset;
            size_t currBaseOffset = matchedBaseOffset;

            // [SECOND] if we found a match, we need to backtrace to the
            if (matchedBaseOffset != baseOffset) {
            // pointers to the matched offsets
                baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
                inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
                while (inputValue == baseValue && loopOffset >= (offset + 8) &&
                matchedBaseOffset >= 8) {
#ifdef DEBUG
                    std::cout << "[SECOND] Backtracing to offset: " << loopOffset
                        << ", matched base offset: " << matchedBaseOffset
                        << ", base value: " << baseValue
                        << ", input value: " << inputValue << '\n';
#endif
                    loopOffset -= 8;  // backtrace by 8 bytes
                    matchedBaseOffset -= 8;
                    inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
                    baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
                }
#ifdef DEBUG
                std::cout << "[SECOND] Backtraced to offset: " << offset
                    << ", matched base offset: " << matchedBaseOffset
                    << ", current offset: " << loopOffset
                    << ", current base offset: " << currBaseOffset << '\n';
#endif


                if (matchedBaseOffset != currBaseOffset) {
                    loopOffset += 8;             // advance offset by 8 bytes
                    matchedBaseOffset += 8;  // advance base offset by 8 bytes
                }
                // finer grain backtrace
                uint8_t inputValue_8 =
                    *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
                uint8_t baseValue_8 =
                    *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
                size_t beforeLoopOffset = loopOffset;
                while (inputValue_8 == baseValue_8 && matchedBaseOffset > 0) {
#ifdef DEBUG
                    std::cout << "[SECOND] Backtracing to offset: " << offset
                        << ", matched base offset: " << matchedBaseOffset
                        << ", base value: " << baseValue_8
                        << ", input value: " << inputValue_8 << '\n';
#endif
                    if (loopOffset == offset) {
                        break;  // we are back at the current offset
                    }
                    loopOffset -= 1;  // backtrace by 1 byte
                    matchedBaseOffset -= 1;
                    inputValue_8 = *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
                    baseValue_8 = *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
                }
                if (loopOffset != beforeLoopOffset) {
                    loopOffset += 1;             // advance offset by 1 byte
                    matchedBaseOffset += 1;  // advance base offset by 1 byte
                }
#ifdef DEBUG
                std::cout << "[SECOND] Backtraced to offset: " << offset
                    << ", matched base offset: " << matchedBaseOffset
                    << ", base value: " << baseValue
                    << ", input value: " << inputValue << '\n';
#endif

                // either we are back at offset (if no inserations happend in input chunk) or we found a mismatch:
                // 1. emit add from offset to loopOffset
                // 2. emit copy from to loopOffset to offset

                // insertation happened
                if (loopOffset > offset) {
#ifdef DEBUG
                    std::cout << "[SECOND] inseration happend offset (EMIT ADD): " << offset
                        << ", currentOffset: " << loopOffset << " of size "
                        << (loopOffset - offset) << '\n';
#endif
                    emitADD(bufInputChunk + offset, loopOffset - offset);
                }
                if (matchedBaseOffset != currBaseOffset) {
                    #ifdef DEBUG
                                    std::cout << "Emitting COPY from base offset: " << matchedBaseOffset
                                        << ", to current base offset: " << currBaseOffset
                                        << ", size: " << (currBaseOffset - matchedBaseOffset)
                                        << '\n';
                    #endif
                    
                    emitCOPY(matchedBaseOffset, currBaseOffset - matchedBaseOffset);
                }
                    // emit COPY instruction

                // prepare offset and baseOffset for next iteration
                // std:: cout << "offset : " << offset << ", baseOffset: " << baseOffset
                //     << ", currOff: " << currOff << ", currBaseOffset: " << currBaseOffset << " loopOffset: " << loopOffset
                //     << ", loopBaseOffset: " << loopBaseOffset << '\n';
                    if(currOff !=  loopOffset){
                        offset = currOff;
                    }
                    baseOffset = currBaseOffset;
            }
            else {
               // [SECOND] no match was found
#ifdef DEBUG
                std::cout << "[SECOND] No match found at offset: " << offset << '\n';
#endif
                if (loopOffset > offset) {
#ifdef DEBUG
                    std::cout << "inseration happend offset (insert ADD): " << offset
                        << ", currentOffset: " << loopOffset << " of size "
                        << (loopOffset - offset) << '\n';
#endif
                    emitADD(bufInputChunk + offset, loopOffset - offset);
                    offset = loopOffset;  // advance offset to the new position
                    baseOffset = loopBaseOffset;  // advance base offset to the new position
                }
            }
        } //ended else 
    } // end while loop

    // emit ADD at the end of the input chunk if any
    if (offset < sizeInputChunk) {
#ifdef DEBUG
        std::cout << "Emitting ADD for remaining input chunk from offset: "
                  << offset << ", size: " << (sizeInputChunk - offset) << "\n";
#endif
        emitADD(bufInputChunk + offset, sizeInputChunk - offset);
    }
    auto end = std::chrono::high_resolution_clock::now();
    duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
        .count();
    #ifdef VORBSE
    std::cout << "generated delta of size: " << (deltaPtr - bufDeltaChunk)
        << " bytes\n";
    #endif
    // writeDeltaChunk();
}

void deltaCompressOriginal(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t dSize = 0;
    gencode(reinterpret_cast<uint8_t*>(bufInputChunk), sizeInputChunk,
            reinterpret_cast<uint8_t*>(bufBaseChunk), sizeBaseChunk,
            reinterpret_cast<uint8_t**>(&deltaPtr), &dSize);         // base offset is 0 for now
    deltaPtr += dSize;  // advance delta pointer
    // std::cout << "Generated delta of size: " << dSize << '\n';
    auto end = std::chrono::high_resolution_clock::now();

    duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
        .count();
    // writeDeltaChunk();
}
int main(int argc, char* argv[]) try {
    const fs::path inCsv = argc > 1 ? fs::path{ argv[1] } : delta_map;
    const fs::path outCsv = argc > 2 ? fs::path{ argv[2] } : "stats.csv";
    if (argc > 3) DATA_DIR = fs::path{ argv[3] };  // optional override

    std::ifstream in(inCsv);
    if (!in) {
        std::cerr << "Cannot open " << inCsv << '\n';
        return 1;
    }
    std::ofstream out(outCsv);
    if (!out) {
        std::cerr << "Cannot write " << outCsv << '\n';
        return 1;
    }

    out << "chunks_original,"
        "chunks_base,"
        "matched_chunks,"
        "unique_chunks,"
        "size_original_bytes,"
        "size_base_bytes,"
        "new_dce,"
        "new_delta_size,"
        "original_dce,"
        "original_delta_size"
        "\n";

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
        deltaPtr = bufDeltaChunk;  // reset delta pointer
        #ifdef VORBSE
        std::cout << "Generated delta of size of: " << dSize << '\n';
        #endif
        deltaCompressOriginal(origPath, basePath);
        // deltaCompress(origPath, basePath);
        // if (decode(bufDeltaChunk, deltaPtr - bufDeltaChunk, bufBaseChunk,
        //     sizeBaseChunk, bufInputChunk, sizeInputChunk)) {
        //     std::cout << "Decoded output matches the input chunk.\n";
        // }
        // else {
        //     std::cerr << "Decoded output does NOT match the input chunk!\n";
        //     return 1;
        // }

        // std::cout << "delta size: " << (deltaPtr - bufDeltaChunk)
        //           << " for chunk: " << fields[1] << '\n';
        totalDeltaSize += (deltaPtr - bufDeltaChunk);
    }
    std::cout << "delta compression ratio : "
        << ((1 - (static_cast<double>(totalDeltaSize) / totalSize)) *
            100.0)
        << "%\n";
    std::cout << "DCR: "
        << (static_cast<double>(totalSize) / totalDeltaSize);
    double throughput = (static_cast<double>(totalSize) / (1024 * 1024)) /
        (duration / 1000000000.0);  // MB/s
    std::cout << " Throughput: " << throughput << " MB/s" << std::endl;

    std::cout << "Done. Results in " << outCsv << '\n';
    return 0;
}
catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
