#include <iostream>
#include <fstream>

#include "decode.hpp"


int main(int argc, char* argv[])
{    
    std::string deltaPath = argc >= 2 ? argv[1] : "./deltas/3918728420697869253";
    std::string basePath = argc >= 3 ? argv[2] : "./base";
    try {
        decode(deltaPath, basePath);   // prints size & saves file
    } catch (const std::exception& e) {
        std::cerr << "decode error: " << e.what() << '\n';
    }
}