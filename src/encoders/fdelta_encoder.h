#pragma once

#include "encoder.h"
#include "fdelta_interface.h"
class FDeltaEncoder final : public DeltaEncoder {
public:
    uint64_t encode() override;

    FDeltaEncoder(){
        std::cout << "FDeltaEncoder initialized.\n";
    }
};
