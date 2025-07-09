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
// static fs::path DATA_DIR =
//     "/mnt/data/fdedup/test_data/storage";  // default data directory
static fs::path DATA_DIR = ".";

static fs::path delta_map =
    "/mnt/data/delta/build/delta_map_test.csv";  // default delta map file

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

std::unordered_map<uint64_t, uint64_t> deltaChunks;  // map of delta chunks

void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);

    std::cout << "Input chunk size: " << sizeInputChunk
              << ", Base chunk size: " << sizeBaseChunk << '\n';
    auto start = std::chrono::high_resolution_clock::now();

    size_t offset = 0;
    size_t baseOffset = 0;
    size_t size = std::min(sizeInputChunk, sizeBaseChunk);
    while (offset < size) {
        uint64_t baseValue =
            *(reinterpret_cast<uint64_t*>(bufBaseChunk + baseOffset));
        uint64_t inputValue =
            *(reinterpret_cast<uint64_t*>(bufInputChunk + offset));
        if (inputValue == baseValue) {
            // std::cout << "Matched at offset: " << offset
            //           << ", baseOffset: " << baseOffset << '\n';
        } else {
            size_t currentOffset = offset;
            size_t currentBaseOffset = baseOffset;
            std::unordered_map<uint64_t, size_t> baseChunks;
            size_t numberOfchunks = 10;
            while (baseOffset < sizeBaseChunk) {
                size_t nextBaseChunkSize =
                    chunker->nextChunk(bufBaseChunk, baseOffset, sizeBaseChunk);
                // std::cout << "Base chunk at offset: " << baseOffset
                //           << ", size: " << nextBaseChunkSize << '\n';
                baseOffset += nextBaseChunkSize;
                baseChunks[*(reinterpret_cast<uint64_t*>(
                    bufBaseChunk + baseOffset - 8))] = baseOffset;
                // for (int i = 0; i < 8; i++) {
                //     char c = *(bufBaseChunk + baseOffset - 8 + i);
                //     if (isprint(c)) {
                //         std::cout << c;
                //     } else {
                //         std::cout << '.';
                //     }
                // }
                numberOfchunks--;
                if (numberOfchunks == 0) {
                    break;
                }
            }
            //TODO: if number of chunks > 0 base chunk has endded. insert add to the end of input chunk.
            numberOfchunks = 10;  // reset for input chunk
            size_t matchedBaseOffset = baseOffset;
            while (offset < sizeInputChunk) {
                size_t nextInputChunkSize =
                    chunker->nextChunk(bufInputChunk, offset, sizeInputChunk);
                // std::cout << "Input chunk at offset: " << offset
                //           << ", size: " << nextInputChunkSize << '\n';
                offset += nextInputChunkSize;
                // // Print the 8 bytes as characters
                // for (int i = 0; i < 8; i++) {
                //     char c = *(bufInputChunk + offset - 8 + i);
                //     if (isprint(c)) {
                //         std::cout << c;
                //     } else {
                //         std::cout << '.';
                //     }
                // }
                // std::cout << '\n';
                auto it = baseChunks.find(
                    *(reinterpret_cast<uint64_t*>(bufInputChunk + offset - 8)));
                if (it != baseChunks.end()) {
                    // std::cout
                    //     << "Found matching chunk at offset: " << offset
                    //     << ", baseOffset: " << it->second
                    //     << " baseOffset - offset : " << (it->second - offset)
                    //     << '\n';
                    matchedBaseOffset = it->second;
                    break;
                } else {
                    // std::cout << "No match found at offset: "
                    //           << offset + nextInputChunkSize << "\n";
                }
                numberOfchunks--;
                if (numberOfchunks == 0) {
                    // std::cout
                    //     << "Reached end of input chunk at offset: " << offset
                    //     << '\n';
                    break;
                }
            }
            uint64_t baseValue = *(reinterpret_cast<uint64_t*>(
                bufBaseChunk + matchedBaseOffset - 8));
            uint64_t inputValue =
                *(reinterpret_cast<uint64_t*>(bufInputChunk + offset - 8));
            if (baseValue == inputValue) {
                // std::cout << "Found a match. lets back trace...\n";
                size_t currOff = offset;
                while (inputValue == baseValue && offset > (currentOffset+8)) {
                    offset -= 8;  // backtrace by 8 bytes
                    matchedBaseOffset -= 8;
                    inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk +
                                                               offset - 8));
                    baseValue = *(reinterpret_cast<uint64_t*>(
                        bufBaseChunk + matchedBaseOffset - 8));
                    // std::cout << "Backtraced to offset: " << offset
                    //           << ", baseOffset: " << matchedBaseOffset
                    //           << ", inputValue: " << inputValue
                    //           << ", baseValue: " << baseValue << '\n';
                }
                // std::cout << "Final backtrace to offset: " << offset
                //           << ", baseOffset: " << matchedBaseOffset
                //           << ", inputValue: " << inputValue
                //           << ", baseValue: " << baseValue << '\n';
                size_t deltaOffset = currOff - offset;
                offset += deltaOffset;  // adjust offset to the backtrace point
                baseOffset = matchedBaseOffset + deltaOffset;  // adjust base offset
                // std::cout << "Adjusted offset: " << offset
                //           << ", baseOffset: " << baseOffset
                //           << ", deltaOffset: " << deltaOffset << '\n';

                // Emit the delta COPY operation
            }

            // while (*(reinterpret_cast<uint64_t*>(bufInputChunk + offset +
            //                                      nextInputChunkSize)) !=
            //            *(reinterpret_cast<uint64_t*>(bufBaseChunk +
            //            baseOffset +
            //                                          nextBaseChunkSize)) &&
            //        offset < size) {
            //     offset += nextInputChunkSize;
            //     baseOffset += nextBaseChunkSize;
            //     nextBaseChunkSize =
            //         chunker->nextChunk(bufBaseChunk, baseOffset,
            //         sizeBaseChunk);
            //     nextInputChunkSize =
            //         chunker->nextChunk(bufInputChunk, offset,
            //         sizeInputChunk);

            //     std::cout << "nextBaseChunkSize: "
            //               << nextBaseChunkSize + baseOffset
            //               << ", nextInputChunkSize: "
            //               << nextInputChunkSize + offset << '\n';
            // }
            // if (offset >= size) {
            //     std::cout << "Reached end of input chunk at offset: " <<
            //     offset
            //               << '\n';
            // } else {
            //     baseOffset += nextBaseChunkSize;
            //     offset += nextInputChunkSize;
            //     std::cout << "finally found a matching point. Base at "
            //               << baseOffset << ", Input at: " << offset << '\n';
            //     uint64_t inputValue =
            //         *(reinterpret_cast<uint64_t*>(bufInputChunk + offset));
            //     uint64_t baseValue =
            //         *(reinterpret_cast<uint64_t*>(bufBaseChunk +
            //         baseOffset));
            //     size_t backTrace = 8;
            //     while (inputValue == baseValue) {
            //         backTrace += 8;
            //         inputValue = *(reinterpret_cast<uint64_t*>(
            //             bufInputChunk + offset - backTrace));
            //         baseValue = *(reinterpret_cast<uint64_t*>(
            //             bufBaseChunk + baseOffset - backTrace));
            //         std::cout << "backtrace: " << backTrace
            //                   << ", inputValue: " << inputValue
            //                   << ", baseValue: " << baseValue << '\n';
            //     }
            //     std::cout << "Backtraced to offset: " << offset - backTrace
            //               << ", baseOffset: " << baseOffset - backTrace <<
            //               '\n';
            // }
        }
        offset += 8;
        baseOffset += 8;  // advance by 8 bytes (uint64_t)
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

        std::cout << "Processing delta ID: " << delta_id
                  << ", Original: " << origPath << ", Base: " << basePath
                  << '\n';

        deltaPtr = bufDeltaChunk;  // reset delta pointer

        // std::cout << "Generated delta of size of: " << dSize << '\n';
        // deltaCompressOriginal(origPath, basePath);
        deltaCompress(origPath, basePath);

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
