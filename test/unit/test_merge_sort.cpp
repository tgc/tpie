// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet cino+=(0 :
// Copyright 2012, 2013, The TPIE development team
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

#include "common.h"
#include <tpie/pipelining/merge_sorter.h>
#include <tpie/parallel_sort.h>
#include <tpie/sysinfo.h>
#include <boost/random.hpp>

using namespace tpie;

#include "merge_sort.h"

class use_merge_sort {
public:
	typedef uint64_t test_t;
	typedef merge_sorter<test_t, false> sorter;

	static void merge_runs(sorter & s) {
		dummy_progress_indicator pi;
		s.calc(pi);
	}
};

bool sort_upper_bound_test() {
	typedef use_merge_sort Traits;
	typedef Traits::sorter sorter;
	typedef Traits::test_t test_t;

	memory_size_type m1 = 100 *1024*1024;
	memory_size_type m2 = 20  *1024*1024;
	memory_size_type m3 = 20  *1024*1024;
	memory_size_type dataSize = 15*1024*1024;
	memory_size_type dataUpperBound = 80*1024*1024;

	memory_size_type items = dataSize / sizeof(test_t);

	stream_size_type io = get_bytes_written();

	relative_memory_usage m(0);
	sorter s;
	s.set_available_memory(m1, m2, m3);
	s.set_items(dataUpperBound / sizeof(test_t));

	m.set_threshold(m1);
	s.begin();
	for (stream_size_type i = 0; i < items; ++i) {
		s.push(i);
	}
	s.end();
	Traits::merge_runs(s);
	while (s.can_pull()) s.pull();

	return io == get_bytes_written();
}

int main(int argc, char ** argv) {
	tests t(argc, argv);
	return
		sort_tester<use_merge_sort>::add_all(t)
		.test(sort_upper_bound_test, "sort_upper_bound")
		;
}
