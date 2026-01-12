#include "encoders/fdelta_encoder.h"
#include "encoders/gdelta_encoder.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;


uint64_t total_encoded_size = 0;
uint64_t total_original_size = 0;
double total_encoding_time = 0.0;

struct Options {
    std::string dataset = "linux";
    std::string encoder_type = "gdelta";
    uint64_t total_chunks = 1000;
    fs::path path_prefix = "/data/";
};

static void printUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n\n"
        << "Options:\n"
        << "  -d, --dataset <name>        Dataset name (default: linux)\n"
        << "  -e, --encoder <type>        Encoder type: gdelta|fdelta (default: gdelta)\n"
        << "  -c, --chunks <count>        Max chunks to process (default: 1000)\n"
        << "  -p, --path-prefix <path>    Dataset root path (default: /data/)\n"
        << "  -h, --help                  Show this help\n";
}

static bool parseArgs(int argc, char* argv[], Options* options, bool* show_help) {
    *show_help = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            printUsage(argv[0]);
            *show_help = true;
            return true;
        } else if (arg == "-d" || arg == "--dataset") {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                return false;
            }
            options->dataset = argv[++i];
        } else if (arg == "-e" || arg == "--encoder") {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                return false;
            }
            options->encoder_type = argv[++i];
        } else if (arg == "-c" || arg == "--chunks") {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                return false;
            }
            options->total_chunks = std::stoull(argv[++i]);
        } else if (arg == "-p" || arg == "--path-prefix") {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                return false;
            }
            options->path_prefix = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            printUsage(argv[0]);
            return false;
        }
    }
    return true;
}

int main(int argc, char* argv[]){
    Options options;
    bool show_help = false;
    if (!parseArgs(argc, argv, &options, &show_help)) {
        return 1;
    }
    if (show_help) {
        return 0;
    }

    auto data_path = options.path_prefix / options.dataset / "chunks";
    auto map_path = options.path_prefix / options.dataset / "meta/delta_map.csv";

    DeltaEncoder* encoder = nullptr;

    if (options.encoder_type == "fdelta") {
        encoder = new FDeltaEncoder();
    } else if (options.encoder_type == "gdelta") {
        encoder = new GDeltaEncoder();
    } else {
        std::cerr << "Unknown encoder type: " << options.encoder_type << "\n";
        return 1;
    }

    // delta_id,original_hash,base_hash,base_size,original_size,delta_size,base_level,estimated_similarity
    std::ifstream map_file(map_path);
    if (!map_file) {
        std::cerr << "Failed to open delta map file: " << map_path << "\n";
        return 1;
    }
    std::string line;
    // Skip header
    std::getline(map_file, line);
    while (std::getline(map_file, line)) {
        if (options.total_chunks-- == 0) break;
        std::istringstream ss(line);
        std::string delta_id, original_hash, base_hash;
        uint64_t base_size, original_size, delta_size, base_level;
        double estimated_similarity;
        std::cout << "Processing line: " << line << "\n";


        std::getline(ss, delta_id, ',');
        std::getline(ss, original_hash, ',');
        std::getline(ss, base_hash, ',');
        ss >> base_size; ss.ignore(1);
        ss >> original_size; ss.ignore(1);
        ss >> delta_size; ss.ignore(1);
        ss >> base_level; ss.ignore(1);
        ss >> estimated_similarity;
        fs::path base_path = data_path / (base_hash);
        fs::path original_path = data_path / (original_hash);
        std::cout << "Processing Delta ID: " << delta_id << " Base: " << base_path << " Original: " << original_path << "\n";
        if (!encoder->loadBase(base_path)) {
            std::cerr << "Failed to load base chunk: " << base_path << "\n";
            continue;
        }
        if (!encoder->loadInput(original_path)) {
            std::cerr << "Failed to load input chunk: " << original_path << "\n";
            continue;
        }

        auto start = std::chrono::steady_clock::now();
        uint64_t encoded_size = encoder->encode();
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed = end - start;

        total_encoding_time += elapsed.count();
        total_original_size += encoder->inputSize;
        total_encoded_size += encoded_size;
        std::cout << "Delta ID: " << delta_id << ", Encoded Size: " << encoded_size << "\n";
    }

    if (total_original_size > 0 && total_encoded_size > 0) {
        double compression_ratio = static_cast<double>(total_original_size) /
            static_cast<double>(total_encoded_size);
        double efficiency = (1.0 - (static_cast<double>(total_encoded_size) /
            static_cast<double>(total_original_size))) * 100.0;
        double throughput = 0.0;
        if (total_encoding_time > 0.0) {
            throughput = (static_cast<double>(total_original_size) / (1024.0 * 1024.0)) /
                total_encoding_time;
        }
        std::cout << std::fixed << std::setprecision(3);
        std::cout << "\nStats\n";
        std::cout << "Total original size: " << total_original_size << " bytes (" << total_original_size/1024.0/1024.0 << " MB)\n";
        std::cout << "Total encoded size: " << total_encoded_size << " bytes (" << total_encoded_size/1024.0/1024.0 << " MB)\n";
        std::cout << "Total encode time: " << total_encoding_time << " s\n";
        std::cout << "Throughput: " << throughput << " MB/s\n";
        std::cout << "Delta compression ratio (input/output): " << compression_ratio << "\n";
        std::cout << "Delta compression efficiency: " << efficiency << "%\n";
    } else {
        std::cout << "No data processed; skipping stats.\n";
    }
                               
}
