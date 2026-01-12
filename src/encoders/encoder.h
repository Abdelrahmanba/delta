#pragma once

#include <filesystem>
#include <string>
#include <iostream>

#include <fstream>
#include <cstring>

#define MAX_CHUNK_SIZE (64 * 1024)  // 64MB 

class DeltaEncoder {
public:
    virtual ~DeltaEncoder() = default;

    alignas(64) uint8_t* inputBuf;
    uint64_t inputSize;

    alignas(64) uint8_t* outputBuf;
    uint64_t outputSize;

    alignas(64) uint8_t* baseBuf;
    uint64_t baseSize;
    
    DeltaEncoder() : inputBuf(nullptr), inputSize(0), outputBuf(nullptr), outputSize(0), baseBuf(nullptr), baseSize(0) {
        inputBuf = new uint8_t[MAX_CHUNK_SIZE];
        outputBuf = new uint8_t[MAX_CHUNK_SIZE];
        baseBuf = new uint8_t[MAX_CHUNK_SIZE];
        std::cout << "DeltaEncoder initialized.\n";
    }
    virtual uint64_t encode() = 0;
    virtual uint64_t decode(uint8_t* delta_buf, uint64_t delta_size) = 0;
    bool loadInput(const std::filesystem::path& filePath) {
        std::ifstream inFile(filePath, std::ios::binary | std::ios::ate);
        if (!inFile) {
            std::cerr << "Failed to open input file: " << filePath << "\n";
            return false;
        }
        inputSize = inFile.tellg();
        inFile.seekg(0, std::ios::beg);
        inFile.read(reinterpret_cast<char*>(inputBuf), inputSize);
        inFile.close();
        return true;
    }

    bool loadBase(const std::filesystem::path& filePath) {
        std::ifstream inFile(filePath, std::ios::binary | std::ios::ate);
        if (!inFile) {
            std::cerr << "Failed to open base file: " << filePath << "\n";
            return false;
        }               
        baseSize = inFile.tellg();
        inFile.seekg(0, std::ios::beg); 
        inFile.read(reinterpret_cast<char*>(baseBuf), baseSize);
        inFile.close();
        return true;
    }

    bool verifyDecode(uint8_t* delta_buf, uint64_t delta_size) {
        int status =  memcmp(outputBuf, inputBuf, inputSize);
       return  (status == 0);
    }

};
