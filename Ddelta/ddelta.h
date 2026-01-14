#ifndef _DDELTA_H
#define _DDELTA_H

#include <cstdint>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <stdexcept>
#include <string>

#include "htable.h"
#include "util.h"
#include "spooky.hpp"

int DDeltaEncode( uint8_t* input, uint64_t input_size,
		  				uint8_t* base, uint64_t base_size,
		  				uint8_t* delta, uint64_t *delta_size );	
    
int DDeltaDecode(uint8_t *delta, uint64_t delta_size,
                  uint8_t *base, uint64_t base_size,
                  uint8_t *output, uint64_t *output_size);
            
#endif // _EDELTA_H