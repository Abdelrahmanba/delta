#include <cstdint>
#include <cstddef>

class Chunker{
    public:
        Chunker();
        ~Chunker();
        
        size_t nextChunk(const unsigned char *readBuffer, size_t buffBegin,
        size_t buffEnd);

    private:
        uint64_t mask;
        uint64_t large_mask;
        size_t minChunkSize;
        size_t maxChunkSize;
        size_t avgChunkSize;
};
