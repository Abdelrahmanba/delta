

#include "ddelta.h"

// ------------------------ Delta format (simple, fixed 32-bit fields) ------------------------
//
// Header:
//   4 bytes  magic "DDLT"
//   u32      version (1)
//   u32      target_size
//
// Records (repeated until end of buffer):
//   u8   type: 0 = COPY, 1 = INSERT
//   u32  len
//   if COPY:
//       u32 offset_in_src
//   else INSERT:
//       len bytes literal data
//
// Notes:
// - We merge adjacent COPYs and INSERTs for compactness.
// - Bounds are checked in decode.

namespace ddelta32 {

static inline void write_u32_le(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back(uint8_t(v & 0xFF));
  out.push_back(uint8_t((v >> 8) & 0xFF));
  out.push_back(uint8_t((v >> 16) & 0xFF));
  out.push_back(uint8_t((v >> 24) & 0xFF));
}

static inline uint32_t read_u32_le(const uint8_t*& p, const uint8_t* end) {
  if (end - p < 4) throw std::runtime_error("delta truncated (u32)");
  uint32_t v = uint32_t(p[0]) |
               (uint32_t(p[1]) << 8) |
               (uint32_t(p[2]) << 16) |
               (uint32_t(p[3]) << 24);
  p += 4;
  return v;
}

static inline uint8_t read_u8(const uint8_t*& p, const uint8_t* end) {
  if (p >= end) throw std::runtime_error("delta truncated (u8)");
  return *p++;
}

struct Op {
  enum Type : uint8_t { COPY = 0, INSERT = 1 } type;
  uint32_t off = 0;                 // for COPY
  std::vector<uint8_t> data;        // for INSERT
  uint32_t len = 0;                 // for COPY or INSERT length
};

static inline void merge_or_push(std::vector<Op>& ops, Op&& op) {
  if (op.len == 0) return;

  if (!ops.empty()) {
    Op& last = ops.back();
    if (last.type == op.type) {
      if (op.type == Op::INSERT) {
        // Merge INSERTs (append data).
        last.data.insert(last.data.end(), op.data.begin(), op.data.end());
        last.len += op.len;
        return;
      } else {
        // Merge COPYs only if they are contiguous in src.
        if (last.off + last.len == op.off) {
          last.len += op.len;
          return;
        }
      }
    }
  }
  ops.push_back(std::move(op));
}

// ------------------------ Gear chunking using MSB 5-bit mask ------------------------
//
// We use a 32-bit rolling "fp":
//   fp = (fp << 1) + GearTable[byte]
//
// Boundary rule (your request):
//   cut when the most significant 5 bits match a pattern.
//   mask = 0xF8000000 (MSB 5 bits), pattern = 0.
// Probability 1/32 => expected ~32 bytes per string.

static constexpr uint32_t kMaskMSB5 = 0xF8000000u;
static constexpr uint32_t kPattern  = 0x00000000u;

// Safety cap to avoid pathological no-cut inputs (not from the paper, just robustness).
static constexpr uint32_t kMaxStringLen = 1u << 15; // 32768 bytes

static size_t next_cut_gear_msb5(const std::vector<uint8_t>& buf,
                                size_t start, size_t end) {
  if (start >= end) return end;

  uint32_t fp = 0;
  size_t pos = start;
  for (; pos < end; ++pos) {
    fp = (fp << 1) + GEAR[buf[pos]];
    size_t cur_len = (pos + 1) - start;

    if (cur_len >= 1) {
      if ((fp & kMaskMSB5) == kPattern) {
        return pos + 1; // cut AFTER pos
      }
      if (cur_len >= kMaxStringLen) {
        return pos + 1; // forced cut
      }
    }
  }
  return end;
}

// ------------------------ Common prefix/suffix scan (chunk-level locality trick) ------------------------

static size_t common_prefix(const std::vector<uint8_t>& a,
                            const std::vector<uint8_t>& b) {
  size_t n = std::min(a.size(), b.size());
  size_t i = 0;
  while (i < n && a[i] == b[i]) ++i;
  return i;
}

static size_t common_suffix(const std::vector<uint8_t>& a,
                            const std::vector<uint8_t>& b,
                            size_t avoid_prefix) {
  // avoid overlapping the prefix region
  size_t maxlen = std::min(a.size(), b.size());
  size_t i = 0;
  while (i < maxlen - avoid_prefix) {
    size_t ai = a.size() - 1 - i;
    size_t bi = b.size() - 1 - i;
    if (a[ai] != b[bi]) break;
    ++i;
  }
  return i;
}

// ------------------------ Ddelta encode/decode ------------------------

std::vector<uint8_t> DDeltaEncode(const std::vector<uint8_t>& src,
                             const std::vector<uint8_t>& tgt) {
  // 1) Build base index: fingerprint -> list of offsets in src
  //    Strings are defined by GearChunking with MSB5 rule.
  std::unordered_map<uint64_t, std::vector<uint32_t>> index;
  index.reserve(src.size() / 16 + 1);

  size_t s_last = 0;
  while (s_last < src.size()) {
    size_t s_cut = next_cut_gear_msb5(src, s_last, src.size());
    size_t len = s_cut - s_last;
    uint64_t fp = SpookyHash::Hash64(src.data() + s_last, len, 0);
    index[fp].push_back(static_cast<uint32_t>(s_last));
    s_last = s_cut;
  }

  std::vector<Op> ops;

  // 2) Chunk-level prefix/suffix matches (fast capture of big equal ends)
  size_t pre = common_prefix(src, tgt);
  size_t suf = common_suffix(src, tgt, pre);

  // Emit prefix copy
  if (pre > 0) {
    Op op;
    op.type = Op::COPY;
    op.off = 0;
    op.len = static_cast<uint32_t>(pre);
    merge_or_push(ops, std::move(op));
  }

  // Define middle region to process with Ddelta string matching
  size_t t_mid_start = pre;
  size_t t_mid_end = tgt.size() - suf;

  // 3) Process target middle by GearChunking + Spooky lookup + memcmp verify
  size_t i = t_mid_start;
  while (i < t_mid_end) {
    size_t cut = next_cut_gear_msb5(tgt, i, t_mid_end);
    size_t str_len = cut - i;

    uint64_t fp = SpookyHash::Hash64(tgt.data() + i, str_len, 0);

    bool matched = false;
    uint32_t best_off = 0;
    size_t best_len = 0;

    auto it = index.find(fp);
    if (it != index.end()) {
      for (uint32_t off : it->second) {
        if (static_cast<size_t>(off) + str_len <= src.size() &&
            std::memcmp(src.data() + off, tgt.data() + i, str_len) == 0) {
          matched = true;
          best_off = off;
          best_len = str_len;
          break;
        }
      }
    }

    if (!matched) {
      // INSERT this string
      Op op;
      op.type = Op::INSERT;
      op.len = static_cast<uint32_t>(str_len);
      op.data.insert(op.data.end(), tgt.begin() + i, tgt.begin() + cut);
      merge_or_push(ops, std::move(op));
      i = cut;
      continue;
    }

    // 4) String-level adjacent scanning:
    //    Extend forward across boundaries to capture nearby equal bytes.
    while (i + best_len < t_mid_end &&
           static_cast<size_t>(best_off) + best_len < src.size() &&
           src[static_cast<size_t>(best_off) + best_len] == tgt[i + best_len]) {
      ++best_len;
    }

    // Extend backward by stealing from the tail of a previous INSERT (if any).
    // This fixes "boundary drift" where a duplicated region got split by GearChunking.
    size_t back = 0;
    if (!ops.empty() && ops.back().type == Op::INSERT) {
      Op& last = ops.back();
      size_t ins_len = last.len;

      // How far can we go back without leaving the middle region or src bounds?
      size_t max_back = std::min({ins_len, static_cast<size_t>(best_off), i - t_mid_start});
      while (back < max_back) {
        uint8_t sb = src[static_cast<size_t>(best_off) - 1 - back];
        uint8_t tb = tgt[i - 1 - back];
        if (sb != tb) break;
        ++back;
      }

      if (back > 0) {
        // Remove bytes from the end of last INSERT
        last.data.resize(last.data.size() - back);
        last.len -= static_cast<uint32_t>(back);
        if (last.len == 0) ops.pop_back();

        best_off -= static_cast<uint32_t>(back);
        i -= back;
        best_len += back;
      }
    }

    // Emit COPY
    Op op;
    op.type = Op::COPY;
    op.off = best_off;
    op.len = static_cast<uint32_t>(best_len);
    merge_or_push(ops, std::move(op));

    i += best_len;
  }

  // Emit suffix copy
  if (suf > 0) {
    Op op;
    op.type = Op::COPY;
    op.off = static_cast<uint32_t>(src.size() - suf);
    op.len = static_cast<uint32_t>(suf);
    merge_or_push(ops, std::move(op));
  }

  // 5) Serialize delta
  std::vector<uint8_t> out;
  out.reserve(tgt.size() / 2 + 32);

  // Header
  out.push_back('D'); out.push_back('D'); out.push_back('L'); out.push_back('T');
  write_u32_le(out, 1u); // version
  write_u32_le(out, static_cast<uint32_t>(tgt.size()));

  // Records
  for (const Op& op : ops) {
    out.push_back(static_cast<uint8_t>(op.type));
    write_u32_le(out, op.len);
    if (op.type == Op::COPY) {
      write_u32_le(out, op.off);
    } else {
      out.insert(out.end(), op.data.begin(), op.data.end());
    }
  }

  return out;
}

std::vector<uint8_t> DDeltaDecode(const std::vector<uint8_t>& src,
                             const std::vector<uint8_t>& delta) {
  const uint8_t* p = delta.data();
  const uint8_t* end = delta.data() + delta.size();

  if (end - p < 12) throw std::runtime_error("delta too small");
  if (!(p[0] == 'D' && p[1] == 'D' && p[2] == 'L' && p[3] == 'T')) {
    throw std::runtime_error("bad magic");
  }
  p += 4;

  uint32_t ver = read_u32_le(p, end);
  if (ver != 1u) throw std::runtime_error("unsupported version");

  uint32_t tgt_size = read_u32_le(p, end);
  std::vector<uint8_t> out;
  out.reserve(tgt_size);

  while (p < end) {
    uint8_t type = read_u8(p, end);
    uint32_t len = read_u32_le(p, end);

    if (type == Op::COPY) {
      uint32_t off = read_u32_le(p, end);
      if (static_cast<uint64_t>(off) + static_cast<uint64_t>(len) > src.size()) {
        throw std::runtime_error("COPY out of bounds");
      }
      out.insert(out.end(), src.begin() + off, src.begin() + off + len);
    } else if (type == Op::INSERT) {
      if (static_cast<uint64_t>(end - p) < len) {
        throw std::runtime_error("INSERT truncated");
      }
      out.insert(out.end(), p, p + len);
      p += len;
    } else {
      throw std::runtime_error("unknown record type");
    }
  }

  if (out.size() != tgt_size) {
    throw std::runtime_error("decoded size mismatch");
  }
  return out;
}

} // namespace ddelta32

int DDeltaEncode(uint8_t* input, uint64_t input_size,
                 uint8_t* base, uint64_t base_size,
                 uint8_t* delta, uint64_t* delta_size) {
  if (delta_size == nullptr || delta == nullptr) {
    return -1;
  }

  try {
    std::vector<uint8_t> src(base, base + base_size);
    std::vector<uint8_t> tgt(input, input + input_size);
    std::vector<uint8_t> out = ddelta32::DDeltaEncode(src, tgt);

    std::memcpy(delta, out.data(), out.size());
    *delta_size = out.size();
    return static_cast<int>(out.size());
  } catch (const std::exception&) {
    return -1;
  }
}

int DDeltaDecode(uint8_t* delta, uint64_t delta_size,
                 uint8_t* base, uint64_t base_size,
                 uint8_t* output, uint64_t* output_size) {
  if (output_size == nullptr || output == nullptr) {
    return -1;
  }

  try {
    std::vector<uint8_t> src(base, base + base_size);
    std::vector<uint8_t> deltabuf(delta, delta + delta_size);
    std::vector<uint8_t> out = ddelta32::DDeltaDecode(src, deltabuf);

    std::memcpy(output, out.data(), out.size());
    *output_size = out.size();
    return static_cast<int>(out.size());
  } catch (const std::exception&) {
    return -1;
  }
}
