#pragma once

#include "encoder.h"

#include "gdelta.h"


class GDeltaEncoder final : public DeltaEncoder {
public:
    uint64_t encode() override;
};
