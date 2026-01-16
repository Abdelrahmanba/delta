#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "decode.hpp"
#include "encoders/fdelta_encoder.h"
#include "encoders/gdelta_encoder.h"
#include "encoders/xdelta_encoder.h"
#include "encoders/edelta_encoder.h"
#include "encoders/zdelta_encoder.h"
#include "encoders/ddelta_encoder.h"


namespace fs = std::filesystem;

uint64_t total_encoded_size = 0;
uint64_t total_original_size = 0;
double total_encoding_time = 0.0;
uint64_t total_decoded_size = 0;
double total_decoding_time = 0.0;

struct Options {
    std::string dataset = "linux";
    std::string encoder_type = "fdelta";
    uint64_t total_chunks = 1000;
    fs::path path_prefix = "/data/";
    fs::path delta_dir;
    bool write_delta = false;
    bool verify_decode = false;
    bool write_decoded = false;
};

static void printUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n\n"
        << "Options:\n"
        << "  -d, --dataset <name>        Dataset name (default: linux)\n"
        << "  -e, --encoder <type>        Encoder type: gdelta|fdelta|xdelta|edelta|zdelta "
           "(default: fdelta)\n"
        << "  -c, --chunks <count>        Max chunks to process (default: "
           "1000)\n"
        << "  -p, --path-prefix <path>    Dataset root path (default: /data/)\n"
        << "  -D, --delta-dir <path>      Delta chunk directory (default: "
           "<dataset>/chunks)\n"
        << "  -w, --write-delta           Write delta chunks as "
           "<input_hash>.delta\n"
        << "  -W, --write-decoded         Write decoded chunks as "
           "<input_hash>.decoded\n"
        << "  -v, --verify-decode         Decode-only: assert delta+base == "
           "input\n"
        << "  -h, --help                  Show this help\n";
}

static bool parseArgs(int argc, char* argv[], Options* options,
                      bool* show_help) {
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
        } else if (arg == "-D" || arg == "--delta-dir") {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                return false;
            }
            options->delta_dir = argv[++i];
        } else if (arg == "-w" || arg == "--write-delta") {
            options->write_delta = true;
        } else if (arg == "-W" || arg == "--write-decoded") {
            options->write_decoded = true;
        } else if (arg == "-v" || arg == "--verify-decode") {
            options->verify_decode = true;
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            printUsage(argv[0]);
            return false;
        }
    }
    return true;
}

int main(int argc, char* argv[]) {
    Options options;
    bool show_help = false;
    if (!parseArgs(argc, argv, &options, &show_help)) {
        return 1;
    }
    if (show_help) {
        return 0;
    }

    auto data_path = options.path_prefix / options.dataset / "chunks";
    auto map_path =
        options.path_prefix / options.dataset / "meta/delta_map.csv";
    fs::path delta_dir =
        options.delta_dir.empty() ? data_path : options.delta_dir;

    DeltaEncoder* encoder = nullptr;

    if (options.encoder_type == "fdelta") {
        encoder = new FDeltaEncoder();
    } else if (options.encoder_type == "gdelta") {
        encoder = new GDeltaEncoder();
    } else if (options.encoder_type == "xdelta") {
        encoder = new XDeltaEncoder();
    } else if (options.encoder_type == "edelta") {
        encoder = new EDeltaEncoder();
    } else if (options.encoder_type == "zdelta") {
        encoder = new ZDeltaEncoder();
    } else if (options.encoder_type == "ddelta") {
        encoder = new DDeltaEncoder();
    } 
    
    else {
        std::cerr << "Unknown encoder type: " << options.encoder_type << "\n";
        return 1;
    }

    if (options.verify_decode && options.write_delta) {
        std::cerr
            << "Choose either --verify-decode or --write-delta, not both\n";
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
        ss >> base_size;
        ss.ignore(1);
        ss >> original_size;
        ss.ignore(1);
        ss >> delta_size;
        ss.ignore(1);
        ss >> base_level;
        ss.ignore(1);
        ss >> estimated_similarity;
        fs::path base_path = data_path / (base_hash);
        fs::path original_path = data_path / (original_hash);
        std::cout << "Processing Delta ID: " << delta_id
                  << " Base: " << base_path << " Original: " << original_path
                  << "\n";
        if (!encoder->loadBase(base_path)) {
            std::cerr << "Failed to load base chunk: " << base_path << "\n";
            continue;
        }
        if (!encoder->loadInput(original_path)) {
            std::cerr << "Failed to load input chunk: " << original_path
                      << "\n";
            continue;
        }

        fs::path delta_path = delta_dir / (original_hash + ".delta");
        if (!options.verify_decode) {
            auto start = std::chrono::steady_clock::now();
            uint64_t encoded_size = encoder->encode();
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = end - start;

            total_encoding_time += elapsed.count();
            total_original_size += encoder->inputSize;
            total_encoded_size += encoded_size;
            std::cout << "Delta ID: " << delta_id
                      << ", Encoded Size: " << encoded_size << "\n";

            if (options.write_delta) {
                std::ofstream delta_out(delta_path, std::ios::binary);
                if (!delta_out) {
                    std::cerr << "Failed to write delta chunk: " << delta_path
                              << "\n";
                    return 1;
                }
                delta_out.write(
                    reinterpret_cast<const char*>(encoder->outputBuf),
                    static_cast<std::streamsize>(encoded_size));
            }
        } else {
            if (!fs::exists(delta_path)) {
                std::cerr << "Delta chunk not found: " << delta_path << "\n";
                return 1;
            }
            std::ifstream delta_in(delta_path,
                                   std::ios::binary | std::ios::ate);
            if (!delta_in) {
                std::cerr << "Failed to open delta chunk: " << delta_path
                          << "\n";
                return 1;
            }
            size_t deltaSize = static_cast<size_t>(delta_in.tellg());

            unsigned char* delta_buf =
                new unsigned char[deltaSize];
            delta_in.seekg(0);
            delta_in.read(reinterpret_cast<char*>(delta_buf),
                            static_cast<std::streamsize>(deltaSize));

            auto decode_start = std::chrono::steady_clock::now();
            bool ok = false;
            uint64_t decoded_size = 0;
            try {
                decoded_size = encoder->decode(delta_buf, deltaSize);
                if (decoded_size != encoder->inputSize) {
                    std::cerr << "Decoded size mismatch: expected "
                              << encoder->inputSize << ", got " << decoded_size
                              << "\n";
                    ok = false;
                } else {
                    ok = encoder->verifyDecode(delta_buf, static_cast<uint64_t>(delta_in.tellg()));
                    if (!ok) {
                        std::cerr << "Decoded content mismatch for delta: "
                                  << delta_id << "\n";
                        throw std::runtime_error("verification failed");
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "Decode error: " << e.what() << "\n";
                return 1;
            }
            auto decode_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> decode_elapsed =
                decode_end - decode_start;

            if (!ok) {
                // std::cerr << "Decode verification failed for delta: "
                        //   << delta_id << "\n";
                // return 1;
            }else {
                std::cout << "Decode verification succeeded for delta: "
                          << delta_id << "\n";
            }

            if (options.write_decoded) {
                fs::path decoded_path = delta_dir / (original_hash + ".decoded");
                std::ofstream decoded_out(decoded_path, std::ios::binary);
                if (!decoded_out) {
                    std::cerr << "Failed to write decoded chunk: "
                              << decoded_path << "\n";
                    return 1;
                }
                decoded_out.write(
                    reinterpret_cast<const char*>(encoder->outputBuf),
                    static_cast<std::streamsize>(decoded_size));
            }

            total_decoding_time += decode_elapsed.count();
            total_decoded_size += encoder->inputSize;
        }
    }

    if (!options.verify_decode && total_original_size > 0 &&
        total_encoded_size > 0) {
        double compression_ratio = static_cast<double>(total_original_size) /
                                   static_cast<double>(total_encoded_size);
        double efficiency = (1.0 - (static_cast<double>(total_encoded_size) /
                                    static_cast<double>(total_original_size))) *
                            100.0;
        double throughput = 0.0;
        if (total_encoding_time > 0.0) {
            throughput =
                (static_cast<double>(total_original_size) / (1024.0 * 1024.0)) /
                total_encoding_time;
        }
        std::cout << std::fixed << std::setprecision(3);
        std::cout << "\nStats\n";
        std::cout << "Total original size: " << total_original_size
                  << " bytes (" << total_original_size / 1024.0 / 1024.0
                  << " MB)\n";
        std::cout << "Total encoded size: " << total_encoded_size << " bytes ("
                  << total_encoded_size / 1024.0 / 1024.0 << " MB)\n";
        std::cout << "Total encode time: " << total_encoding_time << " s\n";
        std::cout << "Throughput: " << throughput << " MB/s\n";
        std::cout << "Delta compression ratio (input/output): "
                  << compression_ratio << "\n";
        std::cout << "Delta compression efficiency: " << efficiency << "%\n";
    } 

    if (options.verify_decode) {
        double decode_throughput = 0.0;
        if (total_decoding_time > 0.0) {
            decode_throughput =
                (static_cast<double>(total_decoded_size) / (1024.0 * 1024.0)) /
                total_decoding_time;
        }
        std::cout << "Total decode size: " << total_decoded_size << " bytes\n";
        std::cout << "Total decode time: " << total_decoding_time << " s\n";
        std::cout << "Decode throughput: " << decode_throughput << " MB/s\n";
    }
}
