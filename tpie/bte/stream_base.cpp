//
// File: bte/stream_base.cpp
// Author: Octavian Procopiuc <tavi@cs.duke.edu>
//         (using some code by Darren Erik Vengroff)
// Created: 01/08/02
//

#include "../lib_config.h"

#include <tpie/bte/stream_base.h>

using namespace tpie;

static unsigned long get_remaining_streams() {
    TPIE_OS_SET_LIMITS_BODY;
}

stats_stream bte::stream_base_generic::gstats_;

int bte::stream_base_generic::remaining_streams = get_remaining_streams();
