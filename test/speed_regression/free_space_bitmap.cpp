// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet cino+=(0 :
// Copyright 2013, The TPIE development team
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

#include <iostream>
#include <boost/progress.hpp>
#include <boost/random.hpp>
#include <tpie/tpie.h>
#include <tpie/tempname.h>
#include <tpie/blocks/block_collection.h>
#include "testinfo.h"

struct test_params {
	size_t operations;
	tpie::stream_size_type size;
};

void test(const test_params & p) {
	using tpie::temp_file;
	using tpie::blocks::block_collection;
	using tpie::blocks::block_handle;

	temp_file tf;
	boost::rand48 rnd;
	boost::uniform_01<double> urnd;

	boost::progress_timer _;
	block_collection bc;

	bc.open(tf.path(), true);
	std::vector<block_handle> handles;
	handles.reserve(p.operations);
	size_t first = 0;
	size_t last = 0;
	for (size_t i = 0; i < p.operations; ++i) {
		if (first == last ||
			(first + p.size > last &&
			 urnd(rnd) * 2.0 - 1.0 <= cos(i * 60.0 / p.size)))
		{
			block_handle h = bc.get_free_block();
			handles.push_back(h);
			++last;
			//std::cout << "Push " << h << '\n';
		} else {
			//std::cout << "Pop " << handles[first] << '\n';
			bc.free_block(handles[first++]);
		}
	}
	while (first < last) {
		//std::cout << "Pop " << handles[first] << '\n';
		bc.free_block(handles[first++]);
	}
	//std::cout << std::flush;
}

int main(int argc, char ** argv) {
	test_params p;
	p.operations = 1000000;
	p.size = 1000;
	size_t repeats = 1;

	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-h" || arg == "--help") {
			std::cout << "Usage: " << argv[0] << " [--ops ops] [--size size] [--repeat n]"
				<< std::endl;
			return 0;
		} else if (arg == "--ops") {
			std::stringstream(argv[++i]) >> p.operations;
		} else if (arg == "--size") {
			std::stringstream(argv[++i]) >> p.size;
		} else if (arg == "--repeat") {
			std::stringstream(argv[++i]) >> repeats;
		}
	}

	tpie::test::testinfo ti("Free space bitmap speed test", 0, 0, repeats);
	tpie::sysinfo si;
	si.printinfo("Operations", p.operations);
	si.printinfo("Max size", p.size);
	for (size_t i = 0; i < repeats; ++i)
		test(p);
	return 0;
}
