// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2008,2012, The TPIE development team
// 
// This file is part of TPIE.
// 
// TPIE is free software: you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
// 
// TPIE is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
// License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with TPIE.  If not, see <http://www.gnu.org/licenses/>

// The tpie_stats class for recording statistics. The parameter C is
// the number of statistics to be recorded.

#include <tpie/stats.h>
#include <tpie/atomic.h>

namespace {
	tpie::atomic_stream_size_type temp_file_usage;
	tpie::atomic_stream_size_type bytes_read;
	tpie::atomic_stream_size_type bytes_written;
} // unnamed namespace

namespace tpie {

	stream_size_type get_temp_file_usage() {
		return temp_file_usage.fetch();
	}

	void increment_temp_file_usage(stream_offset_type delta) {
		stream_size_type x = temp_file_usage.add_and_fetch(delta);
		if (static_cast<stream_offset_type>(x) < 0) {
			// Somebody has a net negative temp_file_usage!
			// This is a race, but this branch is only taken when
			// the application has a negative temp_file_usage,
			// which is a bug in the stats reporting of the application.
			temp_file_usage.sub(x);
		}
	}

	stream_size_type get_bytes_read() {
		return bytes_read.fetch();
	}

	stream_size_type get_bytes_written() {
		return bytes_written.fetch();
	}

	void increment_bytes_read(stream_size_type delta) {
		bytes_read.add(delta);
	}
	
	void increment_bytes_written(stream_size_type delta) {
		bytes_written.add(delta);
	}
}  //  tpie namespace

