#pragma once

#include "encoder.h"

#include "ddelta.h"


class DDeltaEncoder final : public DeltaEncoder {
public:
    uint64_t encode() override;
    uint64_t decode(uint8_t* delta_buf, uint64_t delta_size) override;

};
