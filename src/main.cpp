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

#include "gdelta.h"
#include "xxhash.h"

#define hash_length 48

namespace fs = std::filesystem;

double duration = 0.0;  // global duration for performance measurement
size_t totalSize = 0;
size_t totalDeltaSize = 0;  // total size of all input chunks

// ─────────────────────────────────────── Config
// ──────────────────────────────────────
static fs::path DATA_DIR =
    "/mnt/data/fdedup/test_data/storage";  // default data directory
// static fs::path DATA_DIR = ".";

static fs::path delta_map =
    "/mnt/data/delta/build/delta_map.csv";  // default delta map file

Chunker* chunker = new Chunker();  // global chunker instance

// Handy POD to keep per-file stats
struct Stats {
    std::size_t chunks_orig{0};
    std::size_t chunks_base{0};
    std::size_t matched{0};
    std::size_t unique{0};
    std::uintmax_t bytes_orig{0};
    std::uintmax_t bytes_base{0};
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
        std::cout << "Delta file written: "
                  << "./deltas/" + std::to_string(hash)
                  << " with size: " << sizeDelta << '\n';
    } else
        std::cerr << "Cannot write delta file\n";
}

void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);
    auto start = std::chrono::high_resolution_clock::now();
    std::unordered_map<uint64_t, chunk>
        baseChunks;  // use map to keep chunks with their hashes
    std::unordered_map<uint64_t, chunk>
        baseChunksfarword;           // use map to keep chunks with their hashes
    if (sizeBaseChunk == 0) return;  // empty input
    size_t offset = 0;
    while (offset < sizeBaseChunk) {
        size_t chunkSize =
            chunker->nextChunk(bufBaseChunk, offset, sizeBaseChunk);
        auto hash = XXH3_64bits(bufBaseChunk + offset + chunkSize - hash_length,
                                hash_length);
        // auto hashForward = XXH3_64bits(bufBaseChunk + offset, hash_length);
        // struct chunk cf;
        // cf.offset = offset;
        // cf.size = chunkSize;
        // baseChunksfarword[hashForward] = cf;  // store forward hash

        struct chunk c;
        c.offset = offset;
        c.size = chunkSize;
        baseChunks[hash] = c;

        // std::cout << "Base chunk: " << hash
        //           << " at offset: " << c.offset
        //           << " with size: " << c.size << '\n';

        offset += chunkSize;
    }

    // process input chunk
    if (sizeInputChunk == 0) return;  // empty input
    offset = 0;
    while (offset < sizeInputChunk) {
        size_t chunkSize =
            chunker->nextChunk(bufInputChunk, offset, sizeInputChunk);
        auto hash = XXH3_64bits(
            bufInputChunk + offset + chunkSize - hash_length, hash_length);
        auto baseChunk = baseChunks.find(hash);
        // std::cout << "Input chunk: " << hash
        //           << " at offset: " << offset
        //           << " with size: " << chunkSize << '\n';
        if (baseChunk != baseChunks.end()) {
            struct chunk c;
            c.offset = offset;
            c.size = chunkSize;
            // std::cout << "Input chunk: " << hash
            //           << " at offset: " << c.offset
            //           << " with size: " << c.size << '\n';
            if (c.size == baseChunk->second.size &&
                std::memcmp(bufInputChunk + c.offset,
                            bufBaseChunk + baseChunk->second.offset,
                            c.size) == 0) {
                emitCOPY(baseChunk->second.offset,
                         c.size);  // <-- only two args now
                // std::cout << "Matched chunk: " << hash <<
                //              " at offset: " << c.offset
                //           << " with size: " << c.size << '\n';
            } else {
                uint32_t dSize = 0;
                size_t inputSegmentSize = c.size;
                size_t baseSegmentSize = baseChunk->second.size;
                // std::cout << "Generating delta for chunk: " << hash
                //           << " at offset: " << c.offset
                //           << " with size: " << c.size
                //           << " | base chunk: " << baseChunk->first
                //           << " at offset: " << baseChunk->second.offset
                //           << " with size: " << baseChunk->second.size
                //           << '\n';
                // gencode(reinterpret_cast<uint8_t*>(bufInputChunk + c.offset),
                //         inputSegmentSize,
                //         reinterpret_cast<uint8_t*>(bufBaseChunk +
                //                                    baseChunk->second.offset),
                //         baseSegmentSize, reinterpret_cast<uint8_t**>(&deltaPtr),
                //         &dSize, baseChunk->second.offset);
                deltaPtr += dSize;  // advance delta pointer
                // std::cout << "Generated delta of size: " << dSize << '\n';
            }

        } else {
            auto hashForward = XXH3_64bits(bufInputChunk + offset, hash_length);
            auto baseChunkForward = baseChunksfarword.find(hashForward);
            if (baseChunkForward != baseChunksfarword.end()) {
                struct chunk c;
                c.offset = offset;
                c.size = chunkSize;
                if (c.size ==
                    baseChunkForward->second.size) {  // compare sizes
                    if (std::memcmp(bufInputChunk + c.offset,
                                    bufBaseChunk +
                                        baseChunkForward->second.offset,
                                    c.size) == 0) {
                        emitCOPY(baseChunkForward->second.offset,
                                 c.size);  // <-- only two args now
                        // std::cout << "farowed chunk: " << hashForward
                        //           << " at offset: " << c.offset
                        //           << " with size: " << c.size << '\n';
                    } else {
                        // uint32_t dSize = 0;
                        // gencode(
                        //     reinterpret_cast<uint8_t*>(bufInputChunk + c.offset),
                        //     c.size,
                        //     reinterpret_cast<uint8_t*>(bufBaseChunk +
                        //                                baseChunkForward->second
                        //                                    .offset),
                        //     baseChunkForward->second.size,
                        //     reinterpret_cast<uint8_t**>(&deltaPtr), &dSize,
                        //     baseChunkForward->second.offset);
                        // deltaPtr += dSize;  // advance delta pointer
                        // std::cout << "Generated delta of size: " << dSize
                        //           << " for farowed chunk: " << hashForward
                        //           << " at offset: " << c.offset << '\n';

                    }
                } else {
                    // Unique chunk, emit ADD
                    emitADD(bufInputChunk + offset, chunkSize);
                }                
            }

            // Unique chunk, emit ADD
            // std::cout << "Unique chunk: " << hash
            //           << " at offset: " << offset
            //           << " with size: " << chunkSize << '\n';
            // emitADD(bufInputChunk + offset, chunkSize);
        }
        offset += chunkSize;
    }
    auto end = std::chrono::high_resolution_clock::now();

    duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();

    // writeDeltaChunk();
}

void deltaCompressOriginal(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t dSize = 0;
    gencode(reinterpret_cast<uint8_t*>(bufInputChunk), sizeInputChunk,
            reinterpret_cast<uint8_t*>(bufBaseChunk), sizeBaseChunk,
            reinterpret_cast<uint8_t**>(&deltaPtr), &dSize,
            0);         // base offset is 0 for now
    deltaPtr += dSize;  // advance delta pointer
    std::cout << "Generated delta of size: " << dSize << '\n';
    auto end = std::chrono::high_resolution_clock::now();

    duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    // writeDeltaChunk();
}
int main(int argc, char* argv[]) try {
    const fs::path inCsv = argc > 1 ? fs::path{argv[1]} : delta_map;
    const fs::path outCsv = argc > 2 ? fs::path{argv[2]} : "stats.csv";
    if (argc > 3) DATA_DIR = fs::path{argv[3]};  // optional override

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

        deltaPtr = bufDeltaChunk;  // reset delta pointer

        // std::cout << "Generated delta of size of: " << dSize << '\n';
        deltaCompressOriginal(origPath, basePath);
        // deltaCompress(origPath, basePath);

        // std::cout << "delta size: " << (deltaPtr - bufDeltaChunk)
        //           << " for chunk: " << fields[1] << '\n';
        totalDeltaSize += (deltaPtr - bufDeltaChunk);
    }
    std::cout << "delta compression ratio : "
              << ((1 - (static_cast<double>(totalDeltaSize) / totalSize)) *
                  100.0)
              << "%\n";
    double throughput = (static_cast<double>(totalSize) / (1024 * 1024)) /
                        (duration / 1000000000.0);  // MB/s
    std::cout << "Throughput: " << throughput << " MB/s" << std::endl;

    std::cout << "Done. Results in " << outCsv << '\n';
    return 0;
} catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
