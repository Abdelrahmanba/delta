
#if defined(__GNUC__) || defined(__clang__)
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif



unsigned char* deltaPtr = nullptr;


// ---------- minimal vcdiff helpers -------------------------------
enum : uint8_t {
    T_ADD = 0u << 6,       // 00
    T_COPY_V = 1u << 6,    // 01
    T_COPY_A8 = 2u << 6,   // 10
    T_COPY_A16 = 3u << 6,  // 11
    INLINE_LEN_MAX = 63
};



static inline __attribute__((always_inline, hot)) void writeVarint(uint32_t v) {
    while (v >= 0x80) {
        *deltaPtr++ = uint8_t((v & 0x7Fu) | 0x80u);
        v >>= 7;
    }
    *deltaPtr++ = uint8_t(v);
}

static inline __attribute__((always_inline, hot)) void writeLenHeader(
    uint8_t type, uint32_t len) {
    if (len < INLINE_LEN_MAX) {
        *deltaPtr++ = uint8_t(type | len);
    } else {
        *deltaPtr++ = uint8_t(type | INLINE_LEN_MAX);
        writeVarint(len - INLINE_LEN_MAX);
    }
}

inline __attribute__((always_inline, hot)) void emitADD(
    const unsigned char* data, size_t len) {
    const uint32_t n = static_cast<uint32_t>(len);
    writeLenHeader(T_ADD, n);
    std::memcpy(deltaPtr, data, len);
    deltaPtr += len;
#ifdef DEBUG
    std::cout << "ADD len=" << len << '\n';
#endif
}

inline __attribute__((always_inline, hot)) void emitCOPY(size_t addr,
                                                         size_t len) {
    const uint32_t nlen = static_cast<uint32_t>(len);
    const uint32_t naddr = static_cast<uint32_t>(addr);

    uint8_t type;
    if (naddr <= 0xFFu)
        type = T_COPY_A8;
    else if (naddr <= 0xFFFF)
        type = T_COPY_A16;
    else
        type = T_COPY_V;

    writeLenHeader(type, nlen);

    if (type == T_COPY_A8) {
        *deltaPtr++ = uint8_t(naddr);
    } else if (type == T_COPY_A16) {
        *deltaPtr++ = uint8_t(naddr & 0xFFu);
        *deltaPtr++ = uint8_t((naddr >> 8) & 0xFFu);
    } else {  // T_COPY_V
        writeVarint(naddr);
    }

#ifdef DEBUG
    std::cout << "COPY len=" << len << " addr=" << addr << " mode="
              << ((type == T_COPY_A8)    ? "A8"
                  : (type == T_COPY_A16) ? "A16"
                                         : "V")
              << '\n';
#endif
}


// --- read a little-endian base-128 varint ------------------------
inline uint32_t readVarint(const unsigned char*& p, const unsigned char* end) {
    uint32_t val = 0;
    int shift = 0;

    while (p < end) {
        uint8_t byte = static_cast<uint8_t>(*p++);
        val |= uint32_t(byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;  // high-bit 0 → last byte
        shift += 7;
        if (shift > 28) throw std::runtime_error("varint overflow");
    }
    if (p > end) throw std::runtime_error("truncated varint");
    return val;
}

