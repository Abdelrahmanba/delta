#include <iostream>
#include <fstream>

#include "decode.hpp"


int main(int argc, char* argv[])
{

    try {
        decode("./deltas/3918728420697869253", "base");   // prints size & saves file
    } catch (const std::exception& e) {
        std::cerr << "decode error: " << e.what() << '\n';
    }
}