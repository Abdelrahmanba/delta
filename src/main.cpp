#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "chunker.hpp"
#define XXH_INLINE_ALL
#include <chrono>
#include <unordered_map>

#include "xxhash.h"

#define hash_length 32

namespace fs = std::filesystem;

// ─────────────────────────────────────── Config
// ──────────────────────────────────────
static fs::path DATA_DIR =
    "/mnt/data/fdedup/test_data/storage";  // default data directory
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

size_t sizeBaseChunk = 0;
size_t sizeInputChunk = 0;

void readBaseChunk(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    uint8_t* data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(bufBaseChunk)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(reinterpret_cast<char*>(bufBaseChunk), size);
    sizeBaseChunk = static_cast<size_t>(size);
}

void readInputChunk(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    uint8_t* data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(bufInputChunk)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(reinterpret_cast<char*>(bufInputChunk), size);
    sizeInputChunk = static_cast<size_t>(size);
}

struct chunk {
    uint64_t offset;  // offset in the original data
    size_t size;      // length of the chunk
};

void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);

    // process input chunk
    std::unordered_map<uint64_t, chunk>
        inputChunks;  // use map to keep chunks with their hashes
    if (sizeInputChunk == 0) return;  // empty input
    size_t offset = 0;
    while (offset < sizeInputChunk) {
        size_t chunkSize =
            chunker->nextChunk(bufInputChunk, offset, sizeInputChunk);
        // std::cout << "Chunk at offset: " << offset
        //           << " with size: " << chunkSize << '\n';
        // for (int i = 0; i < hash_length; i++) {
        //     std::cout << bufInputChunk[offset + chunkSize - hash_length + i];  // debug output
        // }
        auto hash = XXH3_64bits(
            bufInputChunk + offset + chunkSize - hash_length, hash_length);
        struct chunk c;
        c.offset = offset;
        c.size = chunkSize;
        inputChunks[hash] = c;  // store chunk with its hash as key
        offset += chunkSize;
    }

    for (const auto& h : inputChunks) {
        std::cout << "Input chunk hash: " << h.first
                  << " at offset: " << h.second.offset
                  << " with size: " << h.second.size << '\n';
    }


    std::unordered_map<uint64_t, chunk>
        baseChunks;                  // use map to keep chunks with their hashes
    if (sizeBaseChunk == 0) return;  // empty input
    offset = 0;
    while (offset < sizeBaseChunk) {
        size_t chunkSize =
            chunker->nextChunk(bufBaseChunk, offset, sizeBaseChunk);
        auto hash = XXH3_64bits(bufBaseChunk + offset + chunkSize - hash_length,
                                hash_length);
        struct chunk c;
        c.offset = offset;
        c.size = chunkSize;
        // if (inputChunks.count(hash)) {
            baseChunks[hash] = c;
            // std::cout << "Chunk found in input: " << hash
            //           << " at offset: " << offset << " with size: " << chunkSize
            //           << '\n';
        // } else {
        //     std::cout << "Chunk not found in input: " << hash
        //               << " at offset: " << offset << " with size: " << chunkSize
        //               << '\n';
        // for (int i = 0; i < hash_length; i++) {
        //     std::cout << bufBaseChunk[offset + chunkSize - hash_length + i] ;  // debug output
        // }
        // std::cout << '\n';
        // }
        offset += chunkSize;
    }
    for (const auto& h : baseChunks) {
        std::cout << "Base chunk hash: " << h.first
                  << " at offset: " << h.second.offset
                  << " with size: " << h.second.size << '\n';
    }

    // // hash-set for fast lookup
    // std::unordered_set<Hash64> dict;
    // for (const auto& h : hBase) {
    //     dict.insert(h.hash); // insert base chunk hashes into the set
    // }

    // std::size_t matched = 0;
    // size_t newSize = 0;

    // for (auto h : hOrig){
    //     if (dict.count(h.hash)){
    //         ++matched;
    //     }
    //     else{
    //         newSize += h.size; // accumulate size of unique chunks
    //     }
    // }
}

int main(int argc, char* argv[]) try {
    // if (argc < 2) {
    // std::cerr << "Usage: " << argv[0] << " [input.csv] [output.csv]
    // [data_dir]\n"; return 1;
    // }
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
