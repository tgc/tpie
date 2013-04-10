// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet cino+=(0 :
// Copyright 2013 The TPIE development team
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
#include <tpie/blocks/b_tree.h>
#include <tpie/prime.h>

bool b_tree_test() {
	bool result = true;
	tpie::blocks::b_tree<int> t;
	for (int i = 0; i < 100; ++i) {
		t.insert((3*i) % 100);
		if (!t.count((i/2*3) % 100)) {
			tpie::log_error() << "Missing element " << (i/2*3)%100 << " from B tree" << std::endl;
			result = false;
		}
	}
	return result;
}

bool b_tree_test_2(size_t items) {
	size_t p = tpie::next_prime(items);
	tpie::log_debug() << "Generating items " << p << "*i%" << items << " for i in [0," << items << ")" << std::endl;
	tpie::blocks::b_tree<size_t> t;
	for (size_t i = 0; i < items; ++i) {
		t.insert(p*i%items);
	}
	std::vector<size_t> output;
	output.reserve(items);
	t.in_order_dump(std::back_inserter(output));

	if (output.size() != items) {
		tpie::log_error() << "B tree dump output incorrect no. of items" << std::endl;
		return false;
	}

	for (size_t i = 0; i < items; ++i) {
		if (output[i] != i) {
			tpie::log_error() << "B tree dump incorrect @ " << i << std::endl;
			return false;
		}
	}
	return true;
}

int main(int argc, char ** argv) {
	return tpie::tests(argc, argv)
	.test(b_tree_test, "b_tree")
	.test(b_tree_test_2, "b_tree_2", "n", static_cast<size_t>(1000))
	;
}
