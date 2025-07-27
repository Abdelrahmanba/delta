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

#include "decode.hpp"
#include "gdelta.h"
#include "edelta.h"

#include <immintrin.h>


#include "xxhash.h"

#define hash_length 48
// #define DEBUG
// #define VORBSE

#if defined(__GNUC__) || defined(__clang__)
#define LIKELY(x)   (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif
#include "chunker.hpp"
namespace fs = std::filesystem;

double duration = 0.0;  // global duration for performance measurement
size_t totalSize = 0;
size_t totalDeltaSize = 0;  // total size of all input chunks
size_t gDeltaSize = 0;


// ─────────────────────────────────────── Config
// ──────────────────────────────────────
static fs::path DATA_DIR =
// "/mydata/storage";  // default data directory
// "/mnt/data/test_data/storage";  // default data directory
// "/mnt/data/test_data/vm_fin_Storage";  // default data directory
// "/mnt/data/test_data/odess_docker_storage";  // default data directory
// "/mnt/data/test_data/odess_debian_storage";  // default data directory
// "/mnt/data/test_data/odess_vm_storage";  // default data directory
// "/mnt/data/test_data/odess_gnu_storage";  // default data directory
"/mnt/data/test_data/linux_storage";  // default data directory

// static fs::path DATA_DIR = "/mnt/data/delta/build/";

static fs::path delta_map =
"./delta_map_failing.csv";  // default delta map file
// "/mydata/fdedup/test_data/delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/vm_vn_delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_docker_delta_map.csv";  // default delta map file
// "./odess_docker_delta_map.csv";  // default delta map file
// "/mnt/data/test_data/odess_gnu_delta_map.csv";  // default delta map file
// "./delta_map_full.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_debian_delta_map.csv";  // default delta map file
// "/mydata/fdedup/test_data/odess_vm_delta_map.csv";  // default delta map file

auto chunker = new Chunker();  // global chunker instance

using Hash64 = std::uint64_t;

// Split a CSV line on commas, preserving empty fields
std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> out;
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, ',')) out.push_back(std::move(cell));
    return out;
}

uint64_t fingerprint = 0;  // Fingerprint for the current chunk


constexpr uint64_t GEARTABLE[256] = {
    0xb088d3a9e840f559, 0x5652c7f739ed20d6, 0x45b28969898972ab,
    0x6b0a89d5b68ec777, 0x368f573e8b7a31b7, 0x1dc636dce936d94b,
    0x207a4c4e5554d5b6, 0xa474b34628239acb, 0x3b06a83e1ca3b912,
    0x90e78d6c2f02baf7, 0xe1c92df7150d9a8a, 0x8e95053a1086d3ad,
    0x5a2ef4f1b83a0722, 0xa50fac949f807fae, 0x0e7303eb80d8d681,
    0x99b07edc1570ad0f, 0x689d2fb555fd3076, 0x00005082119ea468,
    0xc4b08306a88fcc28, 0x3eb0678af6374afd, 0xf19f87ab86ad7436,
    0xf2129fbfbe6bc736, 0x481149575c98a4ed, 0x0000010695477bc5,
    0x1fba37801a9ceacc, 0x3bf06fd663a49b6d, 0x99687e9782e3874b,
    0x79a10673aa50d8e3, 0xe4accf9e6211f420, 0x2520e71f87579071,
    0x2bd5d3fd781a8a9b, 0x00de4dcddd11c873, 0xeaa9311c5a87392f,
    0xdb748eb617bc40ff, 0xaf579a8df620bf6f, 0x86a6e5da1b09c2b1,
    0xcc2fc30ac322a12e, 0x355e2afec1f74267, 0x2d99c8f4c021a47b,
    0xbade4b4a9404cfc3, 0xf7b518721d707d69, 0x3286b6587bf32c20,
    0x0000b68886af270c, 0xa115d6e4db8a9079, 0x484f7e9c97b2e199,
    0xccca7bb75713e301, 0xbf2584a62bb0f160, 0xade7e813625dbcc8,
    0x000070940d87955a, 0x8ae69108139e626f, 0xbd776ad72fde38a2,
    0xfb6b001fc2fcc0cf, 0xc7a474b8e67bc427, 0xbaf6f11610eb5d58,
    0x09cb1f5b6de770d1, 0xb0b219e6977d4c47, 0x00ccbc386ea7ad4a,
    0xcc849d0adf973f01, 0x73a3ef7d016af770, 0xc807d2d386bdbdfe,
    0x7f2ac9966c791730, 0xd037a86bc6c504da, 0xf3f17c661eaa609d,
    0xaca626b04daae687, 0x755a99374f4a5b07, 0x90837ee65b2caede,
    0x6ee8ad93fd560785, 0x0000d9e11053edd8, 0x9e063bb2d21cdbd7,
    0x07ab77f12a01d2b2, 0xec550255e6641b44, 0x78fb94a8449c14c6,
    0xc7510e1bc6c0f5f5, 0x0000320b36e4cae3, 0x827c33262c8b1a2d,
    0x14675f0b48ea4144, 0x267bd3a6498deceb, 0xf1916ff982f5035e,
    0x86221b7ff434fb88, 0x9dbecee7386f49d8, 0xea58f8cac80f8f4a,
    0x008d198692fc64d8, 0x6d38704fbabf9a36, 0xe032cb07d1e7be4c,
    0x228d21f6ad450890, 0x635cb1bfc02589a5, 0x4620a1739ca2ce71,
    0xa7e7dfe3aae5fb58, 0x0c10ca932b3c0deb, 0x2727fee884afed7b,
    0xa2df1c6df9e2ab1f, 0x4dcdd1ac0774f523, 0x000070ffad33e24e,
    0xa2ace87bc5977816, 0x9892275ab4286049, 0xc2861181ddf18959,
    0xbb9972a042483e19, 0xef70cd3766513078, 0x00000513abfc9864,
    0xc058b61858c94083, 0x09e850859725e0de, 0x9197fb3bf83e7d94,
    0x7e1e626d12b64bce, 0x520c54507f7b57d1, 0xbee1797174e22416,
    0x6fd9ac3222e95587, 0x0023957c9adfbf3e, 0xa01c7d7e234bbe15,
    0xaba2c758b8a38cbb, 0x0d1fa0ceec3e2b30, 0x0bb6a58b7e60b991,
    0x4333dd5b9fa26635, 0xc2fd3b7d4001c1a3, 0xfb41802454731127,
    0x65a56185a50d18cb, 0xf67a02bd8784b54f, 0x696f11dd67e65063,
    0x00002022fca814ab, 0x8cd6be912db9d852, 0x695189b6e9ae8a57,
    0xee9453b50ada0c28, 0xd8fc5ea91a78845e, 0xab86bf191a4aa767,
    0x0000c6b5c86415e5, 0x267310178e08a22e, 0xed2d101b078bca25,
    0x3b41ed84b226a8fb, 0x13e622120f28dc06, 0xa315f5ebfb706d26,
    0x8816c34e3301bace, 0xe9395b9cbb71fdae, 0x002ce9202e721648,
    0x4283db1d2bb3c91c, 0xd77d461ad2b1a6a5, 0xe2ec17e46eeb866b,
    0xb8e0be4039fbc47c, 0xdea160c4d5299d04, 0x7eec86c8d28c3634,
    0x2119ad129f98a399, 0xa6ccf46b61a283ef, 0x2c52cedef658c617,
    0x2db4871169acdd83, 0x0000f0d6f39ecbe9, 0x3dd5d8c98d2f9489,
    0x8a1872a22b01f584, 0xf282a4c40e7b3cf2, 0x8020ec2ccb1ba196,
    0x6693b6e09e59e313, 0x0000ce19cc7c83eb, 0x20cb5735f6479c3b,
    0x762ebf3759d75a5b, 0x207bfe823d693975, 0xd77dc112339cd9d5,
    0x9ba7834284627d03, 0x217dc513e95f51e9, 0xb27b1a29fc5e7816,
    0x00d5cd9831bb662d, 0x71e39b806d75734c, 0x7e572af006fb1a23,
    0xa2734f2f6ae91f85, 0xbf82c6b5022cddf2, 0x5c3beac60761a0de,
    0xcdc893bb47416998, 0x6d1085615c187e01, 0x77f8ae30ac277c5d,
    0x917c6b81122a2c91, 0x5b75b699add16967, 0x0000cf6ae79a069b,
    0xf3c40afa60de1104, 0x2063127aa59167c3, 0x621de62269d1894d,
    0xd188ac1de62b4726, 0x107036e2154b673c, 0x0000b85f28553a1d,
    0xf2ef4e4c18236f3d, 0xd9d6de6611b9f602, 0xa1fc7955fb47911c,
    0xeb85fd032f298dbd, 0xbe27502fb3befae1, 0xe3034251c4cd661e,
    0x441364d354071836, 0x0082b36c75f2983e, 0xb145910316fa66f0,
    0x021c069c9847caf7, 0x2910dfc75a4b5221, 0x735b353e1c57a8b5,
    0xce44312ce98ed96c, 0xbc942e4506bdfa65, 0xf05086a71257941b,
    0xfec3b215d351cead, 0x00ae1055e0144202, 0xf54b40846f42e454,
    0x00007fd9c8bcbcc8, 0xbfbd9ef317de9bfe, 0xa804302ff2854e12,
    0x39ce4957a5e5d8d4, 0xffb9e2a45637ba84, 0x55b9ad1d9ea0818b,
    0x00008acbf319178a, 0x48e2bfc8d0fbfb38, 0x8be39841e848b5e8,
    0x0e2712160696a08b, 0xd51096e84b44242a, 0x1101ba176792e13a,
    0xc22e770f4531689d, 0x1689eff272bbc56c, 0x00a92a197f5650ec,
    0xbc765990bda1784e, 0xc61441e392fcb8ae, 0x07e13a2ced31e4a0,
    0x92cbe984234e9d4d, 0x8f4ff572bb7d8ac5, 0x0b9670c00b963bd0,
    0x62955a581a03eb01, 0x645f83e5ea000254, 0x41fce516cd88f299,
    0xbbda9748da7a98cf, 0x0000aab2fe4845fa, 0x19761b069bf56555,
    0x8b8f5e8343b6ad56, 0x3e5d1cfd144821d9, 0xec5c1e2ca2b0cd8f,
    0xfaf7e0fea7fbb57f, 0x000000d3ba12961b, 0xda3f90178401b18e,
    0x70ff906de33a5feb, 0x0527d5a7c06970e7, 0x22d8e773607c13e9,
    0xc9ab70df643c3bac, 0xeda4c6dc8abe12e3, 0xecef1f410033e78a,
    0x0024c2b274ac72cb, 0x06740d954fa900b4, 0x1d7a299b323d6304,
    0xb3c37cb298cbead5, 0xc986e3c76178739b, 0x9fabea364b46f58a,
    0x6da214c5af85cc56, 0x17a43ed8b7a38f84, 0x6eccec511d9adbeb,
    0xf9cab30913335afb, 0x4a5e60c5f415eed2, 0x00006967503672b4,
    0x9da51d121454bb87, 0x84321e13b9bbc816, 0xfb3d6fb6ab2fdd8d,
    0x60305eed8e160a8d, 0xcbbf4b14e9946ce8, 0x00004f63381b10c3,
    0x07d5b7816fcc4e10, 0xe5a536726a6a8155, 0x57afb23447a07fdd,
    0x18f346f7abc9d394, 0x636dc655d61ad33d, 0xcc8bab4939f7f3f6,
    0x63c7a906c1dd187b,
};


#define NUMBER_OF_CHUNKS 4
#define CHUNKS_MULTIPLIER 4
constexpr size_t MaxChunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;

inline size_t nextChunk(char* readBuffer, size_t buffBegin, size_t buffEnd)
{
    uint64_t i = 9;
    size_t size = buffEnd - buffBegin;
    fingerprint = 0;
    if (size > 1024)
        size = 1024;


    while (i + 1 < size) {
        fingerprint = (fingerprint << 1) + GEARTABLE[readBuffer[buffBegin + i]];  // simple hash
        if (!(fingerprint & 0x000018035100)) {
            return i;
        }
        i += 4;
    }
    return size;
}

inline size_t nextChunkBig(char* readBuffer, size_t buffBegin, size_t buffEnd)
{
    uint64_t i = 9;
    size_t size = buffEnd - buffBegin;
    fingerprint = 0;
    if (size > 4096)
        size = 4096;

    while (i + 1 < size) {
        fingerprint = (fingerprint << 1) + GEARTABLE[readBuffer[buffBegin + i]];  // simple hash
        if (!(fingerprint & 0x00001800035300)) {
            return i;
        }
        i += 4;
    }
    return size;
}

alignas(64) char bufBaseChunk[1024 * 65];
alignas(64) char bufInputChunk[1024 * 65];
alignas(64) char bufDeltaChunk[1024 * 65];
char* deltaPtr = bufDeltaChunk;  // pointer to the current position in the delta buffer

size_t sizeBaseChunk = 0;
size_t sizeInputChunk = 0;


// struct TinyMapSIMD {
//     static constexpr size_t kCap = 32;           // >= 12
//     alignas(32) uint64_t keys[kCap];
//     size_t               vals[kCap];
//     size_t               count = 0;

//     inline void clear() {
//         // Mark unused keys with a value that never occurs
//         for (size_t i = 0; i < kCap; ++i) keys[i] = UINT64_C(0xFFFFFFFFFFFFFFFF);
//         count = 0;
//     }

//     TinyMapSIMD() { clear(); }

//     inline bool find(uint64_t key, uint32_t& out) const {
// #if defined(__AVX2__)
//         const __m256i k = _mm256_set1_epi64x((long long)key);
//         // block 0: indices 0..3
//         __m256i v0 = _mm256_load_si256(reinterpret_cast<const __m256i*>(&keys[0]));
//         __m256i v1 = _mm256_load_si256(reinterpret_cast<const __m256i*>(&keys[4]));
//         __m256i v2 = _mm256_load_si256(reinterpret_cast<const __m256i*>(&keys[8]));
//         __m256i v3 = _mm256_load_si256(reinterpret_cast<const __m256i*>(&keys[12]));

//         __m256i m0 = _mm256_cmpeq_epi64(k, v0);
//         __m256i m1 = _mm256_cmpeq_epi64(k, v1);
//         __m256i m2 = _mm256_cmpeq_epi64(k, v2);
//         __m256i m3 = _mm256_cmpeq_epi64(k, v3);

//         unsigned mask0 = (unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(m0));
//         unsigned mask1 = (unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(m1));
//         unsigned mask2 = (unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(m2));
//         unsigned mask3 = (unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(m3));

//         if (mask0) { int idx = __builtin_ctz(mask0); out = vals[idx]; return true; }
//         if (mask1) { int idx = __builtin_ctz(mask1) + 4; out = vals[idx]; return true; }
//         if (mask2) { int idx = __builtin_ctz(mask2) + 8; out = vals[idx]; return true; }
//         if (mask3) { int idx = __builtin_ctz(mask3) + 12; out = vals[idx]; return true; }
//         return false;
// #else
//         // scalar fallback (still fast for 12)
//         for (size_t i = 0; i < count; ++i) {
//             if (keys[i] == key) { out = vals[i]; return true; }
//         }
//         return false;
// #endif
//     }

//     // insert or update
//     inline void upsert(uint64_t key, uint32_t value) {
//         uint32_t v;
//         if (find(key, v)) {
//             // update existing
//             // re-find index cheaply (small count; linear)
//             for (size_t i = 0; i < count; ++i) if (keys[i] == key) { vals[i] = value; return; }
//             return;
//         }
//         // append
//         // assume count < kCap (assert in debug)
// #ifndef NDEBUG
//         if (count >= kCap) __builtin_trap();
// #endif
//         keys[count] = key;
//         vals[count] = value;
//         ++count;
//     }
// };

void inline readBaseChunk(const fs::path& p) {
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
#ifdef DEBUG
    std::cout << "Base chunk read: " << p.string() << " with size: " << sizeBaseChunk << '\n';
#endif
}

void inline readInputChunk(const fs::path& p) {
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
#ifdef DEBUG
    std::cout << "Input chunk read: " << p.string() << " with size: " << sizeInputChunk << '\n';
#endif
}
// ---------- minimal vcdiff helpers -------------------------------
inline __attribute__((always_inline, hot)) void writeVarint(uint32_t v)  // <-- no return value
{
    while (v >= 0x80) {
        *deltaPtr++ = char((v & 0x7F) | 0x80);  // continuation-bit = 1
        v >>= 7;
    }
    *deltaPtr++ = char(v);  // final byte, cont-bit = 0
}

inline __attribute__((always_inline, hot)) void emitADD(const char* data, size_t len) {
    *deltaPtr++ = 0x00;                       // ADD, size follows
    writeVarint(static_cast<uint32_t>(len));  // updates deltaPtr inside
    std::memcpy(deltaPtr, data, len);
    deltaPtr += len;  // advance cursor

    // Print emitted bytes as chars
#ifdef DEBUG
    std::cout << "Emitted ADD of size: " << len << " with data: ";
    std::cout << "ADD instruction bytes: ";
    for (size_t i = 0; i < len; i++) {
        char c = data[i];
        if (isprint(c)) {
            std::cout << c;
        }
        else {
            std::cout << '.';
        }
    }
    std::cout << std::endl;
#endif
}

inline __attribute__((always_inline, hot)) void emitCOPY(size_t addr, size_t len) {
#ifdef DEBUG
    std::cout << "Emitting COPY of size: " << len
        << " from address: " << addr << '\n';
#endif
    * deltaPtr++ = 0x25;  // COPY (var-size, mode 0)
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
// #ifdef VORBSE
        std::cout << "Delta file written: "
            << "./deltas/" + std::to_string(hash)
            << " with size: " << sizeDelta << '\n';
// #endif
    }
    else
        std::cerr << "Cannot write delta file\n";
}

std::unordered_map<uint64_t, uint64_t> deltaChunks;  // map of delta chunks

// std::unordered_map<uint64_t, size_t> baseChunks;
// static bool reserved = (baseChunks.reserve(NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER), true);


// void deltaCompress(const fs::path& origPath, const fs::path& basePath) {
//     readBaseChunk(basePath);
//     readInputChunk(origPath);
//     __builtin_prefetch(deltaPtr, 1, 3);  // prefetch the delta buffer


//     auto start = std::chrono::high_resolution_clock::now();

//     size_t offset = 0;
//     size_t baseOffset = 0;
//     size_t size = sizeInputChunk > sizeBaseChunk ? sizeBaseChunk : sizeInputChunk;

//     while (offset < size) {
//         __builtin_prefetch(bufBaseChunk + baseOffset, 0, 3);
//         __builtin_prefetch(bufBaseChunk + baseOffset + 64, 0, 3);

//         __builtin_prefetch(bufInputChunk + offset, 0, 3);
//         __builtin_prefetch(bufInputChunk + offset + 64, 0, 3);
// #ifdef DEBUG
//         std::cout << "Processing offset: " << offset
//             << ", baseOffset: " << baseOffset << '\n';
// #endif
//         uint64_t baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + baseOffset));
//         uint64_t inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + offset));
//         size_t loopOffset = offset;  // save the original offset for later use
//         size_t loopBaseOffset = baseOffset;  // save the original base offset
//         while (inputValue == baseValue && loopOffset + 8 <= sizeInputChunk
//             && loopBaseOffset + 8 <= sizeBaseChunk) {
// #ifdef DEBUG
//             std::cout << "Matching 8 bytes at offset: " << loopOffset << ", base offset: " << loopBaseOffset << ", base value: " << baseValue << ", input value: " << inputValue << '\n';
// #endif
//             loopBaseOffset += 8;
//             baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + loopBaseOffset));
//             loopOffset += 8;
//             inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
//         }
//         if (offset != loopOffset) {
//             emitCOPY(baseOffset, loopOffset - offset);
// #ifdef DEBUG
//             std::cout << "Emitted COPY at of size: " << loopOffset - offset
//                 << " from offset: " << offset
//                 << " to offset: " << loopOffset << '\n';
// #endif
//         }
//         offset = loopOffset;          // update offset to the new position
//         baseOffset = loopBaseOffset;  // update baseOffset to the new position

//         // Region of change. We try to find a data anchor

//         // before that let's make sure the base buffer is not exhausted
//         if (baseOffset + 8 >= sizeBaseChunk) {
//             // std::cout << "Base chunk stream ended.\n";
//             break;  // no more base chunks to match against
//         }
//         size_t numberOfchunks = NUMBER_OF_CHUNKS;
//         baseChunks.clear();  // clear the base chunks map
//         while (loopBaseOffset < sizeBaseChunk && numberOfchunks > 0) {
//             size_t nextBaseChunkSize = nextChunk(bufBaseChunk, loopBaseOffset, sizeBaseChunk);
//             loopBaseOffset += nextBaseChunkSize;
//             baseChunks.upsert(fingerprint, loopBaseOffset - 8);
//             numberOfchunks--;
// #ifdef DEBUG
//             std::cout << "[first] Next base chunk size: " << nextBaseChunkSize
//                 << ", loop base offset: " << loopBaseOffset << '\n';
// #endif
//         }
//         // this means the base chunk stream has ended. TODO: Don't bother with
//         // the rest. emit Add and return.
//         if (numberOfchunks != 0) {
// #ifdef DEBUG
//             std::cout << "Base chunk stream ended before reaching all chunks.\n";
// #endif
//         }
//         numberOfchunks = NUMBER_OF_CHUNKS;

//         // init the matchedOffset
//         size_t matchedBaseOffset = baseOffset;
//         while (loopOffset < sizeInputChunk && numberOfchunks > 0) {
//             size_t nextInputChunkSize = nextChunk(bufInputChunk, loopOffset, sizeInputChunk);
//             loopOffset += nextInputChunkSize;
// #ifdef DEBUG
//             std::cout << "[first] Next input chunk size: " << nextInputChunkSize
//                 << ", loop offset: " << loopOffset << '\n';
// #endif
//             // query base index for a match
//             if (baseChunks.find(fingerprint, matchedBaseOffset)) {
//                 loopOffset -= 8;  // to the start of the mattch
//                 // stick with the first match TODO: verify that its a good matching point
// #ifdef DEBUG
//                 std::cout << "[FIRST] Matched base offset: " << matchedBaseOffset
//                     << ", loop offset: " << loopOffset << '\n';
// #endif
//                 break;
//             }
//             numberOfchunks--;
//         }
//         // a match is found. back trace then update offsets.
//         size_t currOff = loopOffset;
//         size_t currBaseOffset = matchedBaseOffset;
//         if (matchedBaseOffset != baseOffset) {
// #ifdef DEBUG
//             std::cout << "Matched base offset: " << matchedBaseOffset
//                 << ", curr offset: " << currOff << '\n';
// #endif
//             // pointers to the matched offsets
//             baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
//             inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
//             while (inputValue == baseValue && loopOffset >= (offset + 8) &&
//                 matchedBaseOffset >= 8) {
// #ifdef DEBUG
//                 std::cout << "Backtracing to offset: " << loopOffset
//                     << ", matched base offset: " << matchedBaseOffset
//                     << ", base value: " << baseValue
//                     << ", input value: " << inputValue << '\n';
// #endif
//                 loopOffset -= 8;  // backtrace by 8 bytes
//                 matchedBaseOffset -= 8;
//                 inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
//                 baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
//             }
// #ifdef DEBUG
//             std::cout << "Backtraced to offset: " << loopOffset
//                 << ", matched base offset: " << matchedBaseOffset
//                 << ", current offset: " << loopOffset
//                 << ", current base offset: " << currBaseOffset << '\n';
// #endif
//             if (matchedBaseOffset != currBaseOffset) {
//                 loopOffset += 8;             // advance offset by 8 bytes
//                 matchedBaseOffset += 8;  // advance base offset by 8 bytes
//             }
//             // finer grain backtrace
//             uint8_t inputValue_8 =
//                 *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
//             uint8_t baseValue_8 =
//                 *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
//             size_t beforeLoopOffset = loopOffset;
//             while (inputValue_8 == baseValue_8 && matchedBaseOffset > 0) {
// #ifdef DEBUG
//                 std::cout << "Backtracing to offset: " << loopOffset
//                     << ", matched base offset: " << matchedBaseOffset
//                     << ", base offset: " << baseOffset
//                     << ", current Base offset: " << currBaseOffset
//                     << ", matched base offset: " << matchedBaseOffset << "\n";
// #endif
//                 if (loopOffset == offset) {
//                     break;  // we are back at the current offset
//                 }
//                 loopOffset -= 1;  // backtrace by 1 byte
//                 matchedBaseOffset -= 1;
//                 inputValue_8 = *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
//                 baseValue_8 = *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
//             }
//             if (loopOffset != beforeLoopOffset) {
//                 loopOffset += 1;             // advance offset by 1 byte
//                 matchedBaseOffset += 1;  // advance base offset by 1 byte
//             }
// #ifdef DEBUG
//             std::cout << "Backtraced to offset: " << loopOffset
//                 << ", matched base offset: " << matchedBaseOffset
//                 << ", base value: " << baseValue
//                 << ", input value: " << inputValue << '\n';
// #endif

// // either we are back at offset (if no inserations happend in input chunk) or we found a mismatch:
// // 1. emit add from offset to loopOffset
// // 2. emit copy from to loopOffset to offset

// // insertation happened
//             if (loopOffset > offset) {
// #ifdef DEBUG
//                 std::cout << "inseration happend offset (EMIT ADD): " << offset
//                     << ", currentOffset: " << loopOffset << " of size "
//                     << (loopOffset - offset) << '\n';
// #endif
//                 emitADD(bufInputChunk + offset, loopOffset - offset);
//             }

//             if (matchedBaseOffset != currBaseOffset) {
// #ifdef DEBUG
//                 std::cout << "Emitting COPY from base offset: " << matchedBaseOffset
//                     << ", to current base offset: " << currBaseOffset
//                     << ", size: " << (currBaseOffset - matchedBaseOffset)
//                     << '\n';
// #endif     
//                 emitCOPY(matchedBaseOffset, currBaseOffset - matchedBaseOffset);
//             }
//             if (currOff != loopOffset) {
//                 offset = currOff;
//             }
//             // prepare offset and baseOffset for next iteration
//             baseOffset = currBaseOffset;
//         }
//         // if no match was found, we just emit ADD. This is what i will optimize now
//         else {
//             // try with more chunks
// #ifdef DEBUG
//             std::cout << "[FIRST] No match was found at offset: " << offset << '\n';
// #endif

//             numberOfchunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER - NUMBER_OF_CHUNKS;
//             while (loopBaseOffset < sizeBaseChunk && numberOfchunks > 0) {
//                 size_t nextBaseChunkSize =
//                     nextChunkBig(bufBaseChunk, loopBaseOffset, sizeBaseChunk);
//                 loopBaseOffset += nextBaseChunkSize;
// #ifdef DEBUG
//                 std::cout << "[SECOND] Next base chunk size: " << nextBaseChunkSize
//                     << ", loop base offset: " << loopBaseOffset << " Fingerprint: " << fingerprint << '\n';
// #endif
//                 baseChunks.upsert(fingerprint, loopBaseOffset - 8);
//                 numberOfchunks--;
//             }
//             // this means the base chunk stream has ended. TODO: Don't bother with
//             // the rest. emit Add and return.
//             if (numberOfchunks != 0) {
// #ifdef DEBUG
//                 std::cout << "Base chunk stream ended before reaching 3 chunks.\n";
// #endif
//             }
//             numberOfchunks = NUMBER_OF_CHUNKS * CHUNKS_MULTIPLIER;  // reset for input chunk
//             loopOffset = offset;  // reset loopOffset
//             // init the matchedOffset
//             size_t matchedBaseOffset = baseOffset;
//             while (loopOffset < sizeInputChunk && numberOfchunks > 0) {
//                 size_t nextInputChunkSize = nextChunkBig(bufInputChunk, loopOffset, sizeInputChunk);
//                 loopOffset += nextInputChunkSize;
// #ifdef DEBUG
//                 std::cout << "[SECOND] Next input chunk size: " << nextInputChunkSize <<
//                     ", loop offset: " << loopOffset << " Fingerprint: " << fingerprint << '\n';
// #endif
// // query base index for a match
//                 if (baseChunks.find(fingerprint, matchedBaseOffset)) {
//                     loopOffset -= 8;  // to the start of the mattch
// #ifdef DEBUG
//                     std::cout << "[SECOND] Matched base offset: " << matchedBaseOffset
//                         << ", loop offset: " << loopOffset << '\n';
// #endif
//                     // stick with the first match TODO: verify that its a good matching point
//                     break;
//                 }
//                 numberOfchunks--;
//             }

//             size_t currOff = loopOffset;
//             size_t currBaseOffset = matchedBaseOffset;

//             // [SECOND] if we found a match, we need to backtrace to the
//             if (matchedBaseOffset != baseOffset) {
//             // pointers to the matched offsets
//                 baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
//                 inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
//                 while (inputValue == baseValue && loopOffset >= (offset + 8) &&
//                 matchedBaseOffset >= 8) {
// #ifdef DEBUG
//                     std::cout << "[SECOND] Backtracing to offset: " << loopOffset
//                         << ", matched base offset: " << matchedBaseOffset
//                         << ", base value: " << baseValue
//                         << ", input value: " << inputValue << '\n';
// #endif
//                     loopOffset -= 8;  // backtrace by 8 bytes
//                     matchedBaseOffset -= 8;
//                     inputValue = *(reinterpret_cast<uint64_t*>(bufInputChunk + loopOffset));
//                     baseValue = *(reinterpret_cast<uint64_t*>(bufBaseChunk + matchedBaseOffset));
//                 }
// #ifdef DEBUG
//                 std::cout << "[SECOND] Backtraced to offset: " << offset
//                     << ", matched base offset: " << matchedBaseOffset
//                     << ", current offset: " << loopOffset
//                     << ", current base offset: " << currBaseOffset << '\n';
// #endif


//                 if (matchedBaseOffset != currBaseOffset) {
//                     loopOffset += 8;             // advance offset by 8 bytes
//                     matchedBaseOffset += 8;  // advance base offset by 8 bytes
//                 }
//                 // finer grain backtrace
//                 uint8_t inputValue_8 =
//                     *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
//                 uint8_t baseValue_8 =
//                     *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
//                 size_t beforeLoopOffset = loopOffset;
//                 while (inputValue_8 == baseValue_8 && matchedBaseOffset > 0) {
// #ifdef DEBUG
//                     std::cout << "[SECOND] Backtracing to offset: " << offset
//                         << ", matched base offset: " << matchedBaseOffset
//                         << ", base value: " << baseValue_8
//                         << ", input value: " << inputValue_8 << '\n';
// #endif
//                     if (loopOffset == offset) {
//                         break;  // we are back at the current offset
//                     }
//                     loopOffset -= 1;  // backtrace by 1 byte
//                     matchedBaseOffset -= 1;
//                     inputValue_8 = *(reinterpret_cast<uint8_t*>(bufInputChunk + loopOffset));
//                     baseValue_8 = *(reinterpret_cast<uint8_t*>(bufBaseChunk + matchedBaseOffset));
//                 }
//                 if (loopOffset != beforeLoopOffset) {
//                     loopOffset += 1;             // advance offset by 1 byte
//                     matchedBaseOffset += 1;  // advance base offset by 1 byte
//                 }
// #ifdef DEBUG
//                 std::cout << "[SECOND] Backtraced to offset: " << offset
//                     << ", matched base offset: " << matchedBaseOffset
//                     << ", base value: " << baseValue
//                     << ", input value: " << inputValue << '\n';
// #endif

//                 // either we are back at offset (if no inserations happend in input chunk) or we found a mismatch:
//                 // 1. emit add from offset to loopOffset
//                 // 2. emit copy from to loopOffset to offset

//                 // insertation happened
//                 if (loopOffset > offset) {
// #ifdef DEBUG
//                     std::cout << "[SECOND] inseration happend offset (EMIT ADD): " << offset
//                         << ", currentOffset: " << loopOffset << " of size "
//                         << (loopOffset - offset) << '\n';
// #endif
//                     emitADD(bufInputChunk + offset, loopOffset - offset);
//                 }
//                 if (matchedBaseOffset != currBaseOffset) {
// #ifdef DEBUG
//                     std::cout << "Emitting COPY from base offset: " << matchedBaseOffset
//                         << ", to current base offset: " << currBaseOffset
//                         << ", size: " << (currBaseOffset - matchedBaseOffset)
//                         << '\n';
// #endif

//                     emitCOPY(matchedBaseOffset, currBaseOffset - matchedBaseOffset);
//                 }
//                     // emit COPY instruction

//                 // prepare offset and baseOffset for next iteration
//                 // std:: cout << "offset : " << offset << ", baseOffset: " << baseOffset
//                 //     << ", currOff: " << currOff << ", currBaseOffset: " << currBaseOffset << " loopOffset: " << loopOffset
//                 //     << ", loopBaseOffset: " << loopBaseOffset << '\n';
//                 if (currOff != loopOffset) {
//                     offset = currOff;
//                 }
//                 baseOffset = currBaseOffset;
//             }
//             else {
//                // [SECOND] no match was found
// #ifdef DEBUG
//                 std::cout << "[SECOND] No match found at offset: " << offset << '\n';
// #endif
//                 if (loopOffset > offset) {
// #ifdef DEBUG
//                     std::cout << "inseration happend offset (insert ADD): " << offset
//                         << ", currentOffset: " << loopOffset << " of size "
//                         << (loopOffset - offset) << '\n';
// #endif
//                     emitADD(bufInputChunk + offset, loopOffset - offset);
//                     offset = loopOffset;  // advance offset to the new position
//                     baseOffset = loopBaseOffset;  // advance base offset to the new position
//                 }
//             }
//         } //ended else 
//     } // end while loop

//     // emit ADD at the end of the input chunk if any
//     if (offset < sizeInputChunk) {
// #ifdef DEBUG
//         std::cout << "Emitting ADD for remaining input chunk from offset: "
//             << offset << ", size: " << (sizeInputChunk - offset) << "\n";
// #endif
//         emitADD(bufInputChunk + offset, sizeInputChunk - offset);
//     }
//     auto end = std::chrono::high_resolution_clock::now();
//     duration +=
//         std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
//         .count();
// #ifdef VORBSE
//     std::cout << "generated delta of size: " << (deltaPtr - bufDeltaChunk)
//         << " bytes\n";
// #endif
// // writeDeltaChunk();
// }



void deltaCompressGdelta(const fs::path& origPath, const fs::path& basePath) {
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



inline bool memeq_8(const void* a, const void* b) {
    // memcpy is the safest unaligned load
    uint64_t x, y;
    std::memcpy(&x, a, 8);
    std::memcpy(&y, b, 8);
    return (x ^ y) == 0;
}

inline bool memeq_32(const void* a, const void* b) {
#if defined(__AVX2__)
    __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
    __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
    __m256i x = _mm256_xor_si256(va, vb);
    return _mm256_testz_si256(x, x);
#else
    const uint64_t* pa = reinterpret_cast<const uint64_t*>(a);
    const uint64_t* pb = reinterpret_cast<const uint64_t*>(b);
    // Unaligned 64-bit loads are okay on x86; if you target other arches, keep memcpy
    return ((pa[0] ^ pb[0]) | (pa[1] ^ pb[1]) | (pa[2] ^ pb[2]) | (pa[3] ^ pb[3])) == 0;
#endif
}

inline bool memeq_64(const void* a, const void* b) {
#if defined(__AVX2__)
    const uint8_t* pa = static_cast<const uint8_t*>(a);
    const uint8_t* pb = static_cast<const uint8_t*>(b);
    return memeq_32(pa, pb) && memeq_32(pa + 32, pb + 32);
#else
    const uint64_t* pa = reinterpret_cast<const uint64_t*>(a);
    const uint64_t* pb = reinterpret_cast<const uint64_t*>(b);
    return ((pa[0] ^ pb[0]) | (pa[1] ^ pb[1]) | (pa[2] ^ pb[2]) | (pa[3] ^ pb[3]) |
            (pa[4] ^ pb[4]) | (pa[5] ^ pb[5]) | (pa[6] ^ pb[6]) | (pa[7] ^ pb[7])) == 0;
#endif
}

inline bool memeq_128(const void* a, const void* b) {
#if defined(__AVX512F__)
    __m512i va0 = _mm512_loadu_si512(a);
    __m512i vb0 = _mm512_loadu_si512(b);
    __mmask64 k = _mm512_cmpeq_epi8_mask(va0, vb0);
    return k == ~__mmask64(0); // all bytes equal
#elif defined(__AVX2__)
    const uint8_t* pa = static_cast<const uint8_t*>(a);
    const uint8_t* pb = static_cast<const uint8_t*>(b);
    __m256i a0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 0));
    __m256i b0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 0));
    __m256i a1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 32));
    __m256i b1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 32));
    __m256i a2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 64));
    __m256i b2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 64));
    __m256i a3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pa + 96));
    __m256i b3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(pb + 96));

    __m256i x0 = _mm256_xor_si256(a0, b0);
    __m256i x1 = _mm256_xor_si256(a1, b1);
    __m256i x2 = _mm256_xor_si256(a2, b2);
    __m256i x3 = _mm256_xor_si256(a3, b3);

    __m256i o01 = _mm256_or_si256(x0, x1);
    __m256i o23 = _mm256_or_si256(x2, x3);
    __m256i o = _mm256_or_si256(o01, o23);
    return _mm256_testz_si256(o, o);
#else
    // Scalar fallback
    return memeq_64(a, b) && memeq_64(static_cast<const uint8_t*>(a) + 64,
                                      static_cast<const uint8_t*>(b) + 64);
#endif
}


struct alignas(64) TinyMapSIMD {
    static constexpr uint32_t kCap = 32;

    uint64_t fp[kCap];   // fingerprints
    uint32_t off[kCap];  // offsets
    uint32_t count;      // 0..16

    inline TinyMapSIMD() { clear(); }

    inline void clear() {
        // We only need to reset count; fp/off content can remain.
        // To be extra safe against vector path matching garbage beyond count,
        // we keep the scalar find as default. If you want a vector path,
        // either guard by count or prefill sentinels here.
        count = 0;
    }

    // Insert or overwrite the existing fingerprint's offset.
    inline void upsert(uint64_t fingerprint, uint32_t offset) {
        fp[count] = fingerprint;
        off[count] = offset;
        ++count;
    }
    // Optional AVX2 finder (disabled by default). Enable if you want.
    inline bool find(uint64_t fingerprint, uint32_t& outOffset) const {
#if defined(__AVX2__)

        // Load up to 16 lanes (4 loads of 4x64). Safe because arrays are full-size.
        __m256i key = _mm256_set1_epi64x((long long)fingerprint);

        const __m256i a0 = _mm256_loadu_si256((const __m256i*) & fp[0]);   // lanes 0..3
        const __m256i a1 = _mm256_loadu_si256((const __m256i*) & fp[4]);   // 4..7
        const __m256i a2 = _mm256_loadu_si256((const __m256i*) & fp[8]);   // 8..11
        const __m256i a3 = _mm256_loadu_si256((const __m256i*) & fp[12]);  // 12..15


        const __m256i m0 = _mm256_cmpeq_epi64(a0, key);
        const __m256i m1 = _mm256_cmpeq_epi64(a1, key);
        const __m256i m2 = _mm256_cmpeq_epi64(a2, key);
        const __m256i m3 = _mm256_cmpeq_epi64(a3, key);

        // Convert masks to per-lane hits:
        // movemask gives 32 bits; every equal 64-bit lane sets 8 bits to 1.
        uint32_t mm0 = (uint32_t)_mm256_movemask_epi8(m0);
        uint32_t mm1 = (uint32_t)_mm256_movemask_epi8(m1);
        uint32_t mm2 = (uint32_t)_mm256_movemask_epi8(m2);
        uint32_t mm3 = (uint32_t)_mm256_movemask_epi8(m3);

        // Helper to scan first hit within a 4-lane group
        auto first_lane = [](uint32_t mm)->int {
            // Each 64-bit lane corresponds to 8 mask bits.
            for (int lane = 0; lane < 4; ++lane) {
                if (mm & (0xFFu << (lane * 8))) return lane;
            }
            return -1;
            };

        int grp = -1, lane = -1;

        if (mm0) { grp = 0; lane = first_lane(mm0); }
        else if (mm1) { grp = 1; lane = first_lane(mm1); }
        else if (mm2) { grp = 2; lane = first_lane(mm2); }
        else if (mm3) { grp = 3; lane = first_lane(mm3); }

        if (lane >= 0) {
            uint32_t idx = (uint32_t)(grp * 4 + lane);
            if (idx < count && fp[idx] == fingerprint) {
                outOffset = off[idx];
                return true;
            }
        }
        return false;
#endif
    }
};

alignas(64) TinyMapSIMD baseChunks;

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
                uint32_t nextBaseChunkSize = chunker->nextChunk(bufBaseChunk, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(bufBaseChunk + loopBaseOffset - 32, 32);
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
                uint32_t nextInputChunkSize = chunker->nextChunk(bufInputChunk, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(bufInputChunk + loopOffset - 32, 32);


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
                uint32_t nextBaseChunkSize = chunker->nextChunkBig(bufBaseChunk, loopBaseOffset, baseSize);
                loopBaseOffset += nextBaseChunkSize;
                Hash64 fp = XXH3_64bits(bufBaseChunk + loopBaseOffset - 32, 32);
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
                uint32_t nextInputChunkSize = chunker->nextChunkBig(bufInputChunk, loopOffset, inSize);
                loopOffset += nextInputChunkSize;
                uint64_t fp = XXH3_64bits(bufInputChunk + loopOffset - 32, 32);

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

    auto end = std::chrono::high_resolution_clock::now();
    duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        // writeDeltaChunk();
    // writeDeltaChunk();

}


void deltaCompressEDelta(const fs::path& origPath, const fs::path& basePath) {
    readBaseChunk(basePath);
    readInputChunk(origPath);
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t dSize = 0;
    EDeltaEncode(reinterpret_cast<uint8_t*>(bufInputChunk), sizeInputChunk,
                  reinterpret_cast<uint8_t*>(bufBaseChunk), sizeBaseChunk,
                  reinterpret_cast<uint8_t*>(bufDeltaChunk), &dSize);
    deltaPtr += dSize;  // advance delta pointer
    // std::cout << "Generated delta of size: " << dSize << '\n';
    auto end = std::chrono::high_resolution_clock::now();

    duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
        .count();
    writeDeltaChunk();
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
// #ifdef VORBSE
//         std::cout << "Processing delta ID: " << delta_id
//             << ", Original: " << origPath << ", Base: " << basePath
//             << '\n';
// #endif
        // deltaPtr = bufDeltaChunk;  // reset delta pointer
        // deltaCompressGdelta(origPath, basePath);
        // size_t gsize = (deltaPtr - bufDeltaChunk);  // size of the generated delta
        // gDeltaSize += gsize;  // accumulate gdelta size
        // deltaPtr = bufDeltaChunk;  // reset delta pointer
        // deltaCompressEDelta(origPath, basePath);
        // uint64_t eDeltaSize = deltaPtr - bufDeltaChunk;

        deltaPtr = bufDeltaChunk;  // reset delta pointer
        deltaCompress(origPath, basePath);
        totalDeltaSize += (deltaPtr - bufDeltaChunk);

        // std::cout << "sizeBaseChunk: " << sizeBaseChunk
        //     << ", sizeInputChunk: " << sizeInputChunk << " sizeDeltaChunk: " << (deltaPtr - bufDeltaChunk) << '\n';
        // std::cout << "gdelta size: " << gsize << ", new delta size: " << (deltaPtr - bufDeltaChunk)
        //     << " bytes for chunk: " << fields[1] << '\n';
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

// std::cout << "delta size: " << (deltaPtr - bufDeltaChunk)
//           << " for chunk: " << fields[1] << '\n';
    }
    std::cout << "new delta compression ratio : " << ((1 - (static_cast<double>(totalDeltaSize) / (totalSize))) * 100.0) << "%\n";
    std::cout << " gdelta dcr : " << ((1 - (static_cast<double>(gDeltaSize) / (totalSize))) * 100.0) << "%\n";
    double throughput = (static_cast<double>(totalSize) / (1024 * 1024)) / (duration / 1000000000.0);  // MB/s
    std::cout << " Throughput: " << throughput << " MB/s" << std::endl;

    std::cout << "Done. Results in " << outCsv << '\n';
    delete chunker;
    return 0;
}
catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 2;
}
