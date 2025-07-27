#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#define XXH_INLINE_ALL
#include <string.h>

#include <chrono>
#include <cstring>
#include <unordered_map>

#include "xxhash.h"

namespace fs = std::filesystem;


// ---------------------------------------------------------------------------
//  VCDIFF *decoder*  (ADD + COPY-mode-0 only, var-int sizes)
// ---------------------------------------------------------------------------
namespace {

// --- read a little-endian base-128 varint ------------------------
inline uint32_t readVarint(const char*& p, const char* end)
{
    uint32_t val = 0;
    int      shift = 0;

    while (p < end) {
        uint8_t byte = static_cast<uint8_t>(*p++);
        val |= uint32_t(byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;             // high-bit 0 → last byte
        shift += 7;
        if (shift > 28) throw std::runtime_error("varint overflow");
    }
    if (p > end) throw std::runtime_error("truncated varint");
    return val;
}

} // anonymous namespace

/**
 * Decode a delta window produced by `deltaCompress()` and store the
 * reconstructed chunk beside the delta file.
 *
 * @param deltaPath   path to the *delta* file (“delta”)
 * @param basePath    path to the reference / dictionary chunk
 *
 * Result is written as  <deltaPath>.decoded   in the same directory.
 * Prints the reconstructed size in bytes.
 */
void decode(const fs::path& deltaPath, const fs::path& basePath)
{
    /* ---- load dictionary ------------------------------------------------- */
    std::ifstream baseIn(basePath, std::ios::binary | std::ios::ate);
    if (!baseIn) throw std::runtime_error("Cannot open base " + basePath.string());

    std::vector<char> baseBuf(static_cast<size_t>(baseIn.tellg()));
    baseIn.seekg(0);
    baseIn.read(baseBuf.data(), static_cast<std::streamsize>(baseBuf.size()));

    /* ---- load delta window ---------------------------------------------- */
    std::ifstream deltaIn(deltaPath, std::ios::binary | std::ios::ate);
    if (!deltaIn) throw std::runtime_error("Cannot open delta " + deltaPath.string());

    std::vector<char> deltaBuf(static_cast<size_t>(deltaIn.tellg()));
    deltaIn.seekg(0);
    deltaIn.read(deltaBuf.data(), static_cast<std::streamsize>(deltaBuf.size()));

    /* ---- decode --------------------------------------------------------- */
    std::vector<char> out;                  // grows as needed
    out.reserve(baseBuf.size());            // heuristic – often similar

    const char* p   = deltaBuf.data();
    const char* end = p + deltaBuf.size();

    while (p < end) {
        uint8_t opcode = static_cast<uint8_t>(*p++);
        switch (opcode) {
            case 0x00: {                    // ADD  (size varint, then raw bytes)
                uint32_t len = readVarint(p, end);
                if (static_cast<size_t>(end - p) < len)
                    throw std::runtime_error("truncated ADD data");
                out.insert(out.end(), p, p + len);
                p += len;
                break;
            }
            case 0x25: {                    // COPY (size varint, addr varint)
                uint32_t len  = readVarint(p, end);
                uint32_t addr = readVarint(p, end);
                #ifdef DEBUG
                std::cout << "COPY instruction: addr = " << addr
                          << ", len = " << len << ", addr + len = " << (addr + len) << '\n';
                #endif
                if (addr + len > baseBuf.size())
                    throw std::runtime_error("COPY out of bounds");
                out.insert(out.end(),
                           baseBuf.data() + addr,
                           baseBuf.data() + addr + len);
                break;
            }
            default:
                throw std::runtime_error(
                    "Opcode " + std::to_string(opcode) + " not supported");
        }
    }

    /* ---- write reconstructed chunk ------------------------------------- */
    fs::path outPath = deltaPath;
    outPath += ".decoded";

    std::ofstream outFile(outPath, std::ios::binary);
    if (!outFile) throw std::runtime_error("Cannot create " + outPath.string());
    outFile.write(out.data(), static_cast<std::streamsize>(out.size()));

    std::cout << "Decoded file written to " << outPath
              << " (size: " << out.size() << " bytes)\n";
}



/**
 * Decode a delta-encoded chunk that was produced against a *base* buffer and
 * verify that the reconstructed output matches an *expected* buffer.
 *
 * @param deltaBuf   pointer to the delta window
 * @param deltaSize  size of the delta window in bytes
 * @param baseBuf    pointer to the base (dictionary) buffer
 * @param baseSize   size of the base buffer in bytes
 * @param expected   pointer to the buffer that holds the expected result
 * @param expectedSz size of the expected result in bytes
 * @return true if the decoded output is byte-for-byte identical to *expected*
 *
 * Any decoding error throws std::runtime_error; a *false* return value means
 * decoding succeeded but the contents/sizes differ.
 */
bool decode(const char*  deltaBuf,  std::size_t deltaSize,
            const char*  baseBuf,   std::size_t baseSize,
            const char*  expected,  std::size_t expectedSz)
{
    if (!deltaBuf || !baseBuf || !expected)
        throw std::invalid_argument("null pointer argument");

    std::vector<char> out;
    out.reserve(baseSize);                 // heuristic – often similar

    const char* p   = deltaBuf;
    const char* end = deltaBuf + deltaSize;

    while (p < end) {
        uint8_t opcode = static_cast<uint8_t>(*p++);
        switch (opcode) {
            case 0x00: {                  // ADD  (size varint, then raw bytes)
                uint32_t len = readVarint(p, end);
                if (static_cast<std::size_t>(end - p) < len)
                    throw std::runtime_error("truncated ADD data");
                out.insert(out.end(), p, p + len);
                p += len;
                break;
            }
            case 0x25: {                  // COPY (size varint, addr varint)
                uint32_t len  = readVarint(p, end);
                uint32_t addr = readVarint(p, end);
                #ifdef DEBUG
                std::cout << "COPY instruction: addr = " << addr
                          << ", len = " << len << ", addr + len = " << (addr + len) << '\n';
                #endif
                if (addr + len > baseSize)
                    throw std::runtime_error("COPY out of bounds");
                out.insert(out.end(),
                           baseBuf + addr,
                           baseBuf + addr + len);
                break;
            }
            default:
                throw std::runtime_error("Opcode " + std::to_string(opcode) +
                                         " not supported");
        }
    }

    /* ---- compare with expected result ----------------------------------- */
    return out.size() == expectedSz &&
           std::memcmp(out.data(), expected, expectedSz) == 0;
}