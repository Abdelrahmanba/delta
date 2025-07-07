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

#include "xxhash.h"

#define hash_length 32

namespace fs = std::filesystem;

// ─────────────────────────────────────── Config
// ──────────────────────────────────────
// static fs::path DATA_DIR =
//     "/mnt/data/fdedup/test_data/storage";  // default data directory
static fs::path DATA_DIR = ".";

    
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

void writeDeltaChunk(){
    size_t sizeDelta = static_cast<size_t>(deltaPtr - bufDeltaChunk);

    auto hash = XXH3_64bits(bufDeltaChunk, sizeDelta);

    std::ofstream deltaOut("./deltas/" + std::to_string(hash), std::ios::binary);
    if (deltaOut)
        deltaOut.write(bufDeltaChunk, sizeDelta);
    else
        std::cerr << "Cannot write delta file\n";
}

void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);

    std::unordered_map<uint64_t, chunk>
        baseChunks;                  // use map to keep chunks with their hashes
    if (sizeBaseChunk == 0) return;  // empty input
    size_t offset = 0;
    while (offset < sizeBaseChunk) {
        size_t chunkSize =
            chunker->nextChunk(bufBaseChunk, offset, sizeBaseChunk);
        auto hash = XXH3_64bits(bufBaseChunk + offset + chunkSize - hash_length,
                                hash_length);
        struct chunk c;
        c.offset = offset;
        c.size = chunkSize;
        baseChunks[hash] = c;

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
        if (baseChunk != baseChunks.end()) {
            struct chunk c;
            c.offset = offset;
            c.size = chunkSize;
            if (c.size == baseChunk->second.size &&
                std::memcmp(bufInputChunk + c.offset,
                            bufBaseChunk + baseChunk->second.offset,
                            c.size) == 0) {
                emitCOPY(baseChunk->second.offset,
                         c.size);  // <-- only two args now
                std::cout << "Matched chunk: " << hash << '\n';
            } else {
                emitADD(bufInputChunk + c.offset, c.size);
                std::cout << "Unique chunk: " << hash
                          << " at offset: " << c.offset
                          << " with size: " << c.size << '\n';
                std::cout << "Base chunk: " << baseChunk->first
                          << " at offset: " << baseChunk->second.offset
                          << " with size: " << baseChunk->second.size << '\n';
            }
        }

        offset += chunkSize;
    }
    /* ---- finish the window & dump it to disk ---------------------- */
    writeDeltaChunk();
    
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
        deltaCompress(origPath, basePath);

        // Stats s;
        // s.chunks_orig  = hOrig.size();
        // s.chunks_base  = hBase.size();
        // s.matched      = matched;
        // s.unique       = hOrig.size() - matched;
        // s.bytes_orig   = sizeInputChunk;
        // s.bytes_base   = sizeBaseChunk;

        // ---------- emit row ----------
        // out << s.chunks_orig << ','
        //     << s.chunks_base << ','
        //     << s.matched << ','
        //     << s.unique << ','
        //     << s.bytes_orig << ','
        //     << s.bytes_base << ','
        //     << (1- (newSize *1.0  / s.bytes_orig)) * 100.0<< '%' << ',' //
        //     size delta as percentage
        //     << newSize << ','
        //     << (1- (stoi(fields[5]) * 1.0  / stoi(fields[4]))) * 100.0<< '%'
        //     << ',' // size delta as percentage
        //     << fields[5] << '\n';

        // std::cout << "Processed: " << delta_id
        //           << " | Chunks (orig): " << s.chunks_orig
        //           << " | Chunks (base): " << s.chunks_base
        //           << " | Matched: " << s.matched
        //           << " | Unique: " << s.unique
        //           << " | Size (orig): " << s.bytes_orig
        //           << " | Size (base): " << s.bytes_base
        //           << '\n';
    }

    std::cout << "Done. Results in " << outCsv << '\n';
    return 0;
} catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
