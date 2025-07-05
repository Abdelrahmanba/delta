#include "chunker.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include "xxhash.h"
#include <chrono>

namespace fs = std::filesystem;

// ─────────────────────────────────────── Config ──────────────────────────────────────
static fs::path DATA_DIR = "/mnt/data/fdedup/test_data/storage"; // default data directory

Chunker* chunker = new Chunker(); // global chunker instance

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

uint8_t buf1[1024 * 1024]; // 1 MiB buffer for reading files
// Read whole file into memory (fatal on error)
uint8_t buf2[1024 * 1024]; // 1 MiB buffer for reading files
size_t size1 = 0;
size_t size2 = 0;

void readFile1(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    uint8_t *data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(buf1)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(reinterpret_cast<char*>(buf1), size);
    size1 = static_cast<size_t>(size);
}


void readFile2(const fs::path& p) {
    std::ifstream in(p, std::ios::binary | std::ios::ate);
    if (!in) throw std::runtime_error("Cannot open " + p.string());
    uint8_t *data = nullptr;
    std::streamsize size = in.tellg();
    if (size < 0) throw std::runtime_error("Cannot read " + p.string());
    if (size > sizeof(buf2)) {
        throw std::runtime_error("File too large: " + p.string());
    }
    in.seekg(0);
    in.read(reinterpret_cast<char*>(buf2), size);
    size2 = static_cast<size_t>(size);
}

struct chunk {
    size_t hash; // offset in the original data
    size_t size; // length of the chunk
};

std::vector<chunk> chunkAndHash(const unsigned char* data, size_t length) {
    std::vector<chunk> chunks;

    size_t offset = 0;
    if (length == 0) return chunks; // empty input
    
    while (offset < length) {
        size_t chunkSize = chunker->nextChunk(data, offset, length);
        auto* ptr = reinterpret_cast<const void*>(data + offset);
        auto hash = XXH3_64bits(ptr, chunkSize); // Compute the hash
        struct chunk c; 
        c.hash = hash; // Store the hash
        c.size = chunkSize; // Store the size of the chunk
        // std::cout << "Chunk hash: " << hash << " at offset: " << offset + chunkSize<< std::endl;
        chunks.push_back(c); // Hash the chunk


        // Move to the next chunk
        offset += 512;
    }

    return chunks;
}

// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) try
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input.csv> [output.csv] [data_dir]\n";
        return 1;
    }
    const fs::path inCsv  = argv[1];
    const fs::path outCsv = argc > 2 ? fs::path{argv[2]} : "stats.csv";
    if (argc > 3) DATA_DIR = fs::path{argv[3]}; // optional override

    std::ifstream in(inCsv);
    if (!in) { std::cerr << "Cannot open " << inCsv << '\n'; return 1; }
    std::ofstream out(outCsv);
    if (!out) { std::cerr << "Cannot write " << outCsv << '\n'; return 1; }

    out << 
           "chunks_original,"
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
    std::getline(in, header); // skip original header line

    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto fields = splitCSV(line);
        if (fields.size() < 6) { std::cerr << "Bad CSV line: " << line << '\n'; continue; }

        const std::string& delta_id = fields[0];
        const fs::path origPath = DATA_DIR / fields[1];
        const fs::path basePath = DATA_DIR / fields[2];

        // ---------- Load & chunk both files ----------
        readFile1(origPath);
        readFile2(basePath);

        auto hOrig = chunkAndHash(buf1, size1);
        auto hBase = chunkAndHash(buf2, size2);

        // hash-set for fast lookup
        std::unordered_set<Hash64> dict;
        for (const auto& h : hBase) {
            dict.insert(h.hash); // insert base chunk hashes into the set
        }

        std::size_t matched = 0;
        size_t newSize = 0;

        for (auto h : hOrig){
            if (dict.count(h.hash)){

                ++matched;
            }
            else{
                newSize += h.size; // accumulate size of unique chunks
            }

        } 

        Stats s;
        s.chunks_orig  = hOrig.size();
        s.chunks_base  = hBase.size();
        s.matched      = matched;
        s.unique       = hOrig.size() - matched;
        s.bytes_orig   = size1;
        s.bytes_base   = size2;

        // ---------- emit row ----------
        out << s.chunks_orig << ','
            << s.chunks_base << ','
            << s.matched << ','
            << s.unique << ','
            << s.bytes_orig << ','
            << s.bytes_base << ','
            << (1- (newSize *1.0  / s.bytes_orig)) * 100.0<< '%' << ',' // size delta as percentage
            << newSize << ','
            << (1- (stoi(fields[5]) * 1.0  / stoi(fields[4]))) * 100.0<< '%' << ',' // size delta as percentage
            << fields[5] << '\n';

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
}
catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
