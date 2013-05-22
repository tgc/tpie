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

typedef size_t key_type;
typedef tpie::blocks::b_tree_traits<key_type> traits_type;
typedef tpie::blocks::b_tree<traits_type> tree_type;
typedef tpie::blocks::b_tree_builder<traits_type> builder_type;

bool b_tree_test() {
	bool result = true;
	tree_type t;
	t.open();
	for (key_type i = 0; i < 100; ++i) {
		t.insert((3*i) % 100);
		if (!t.count((i/2*3) % 100)) {
			tpie::log_error() << "Missing element " << (i/2*3)%100 << " from B tree" << std::endl;
			result = false;
		}
	}
	return result;
}

bool b_tree_test_2(key_type items) {
	key_type p = static_cast<key_type>(tpie::next_prime(static_cast<size_t>(items+1)));
	tpie::log_debug() << "Generating items " << p << "*i%" << items << " for i in [0," << items << ")" << std::endl;
	tree_type t;
	t.open();
	for (key_type i = 0; i < items; ++i) {
		try {
			t.insert(p*i%items);
		} catch (...) {
			tpie::log_error() << "Exception after " << i << " insertions" << std::endl;
			throw;
		}
	}
	std::vector<key_type> output;
	output.reserve(items);
	t.in_order_dump(std::back_inserter(output));

	if (output.size() != items) {
		tpie::log_error() << "B tree dump output incorrect no. of items" << std::endl;
		return false;
	}

	for (key_type i = 0; i < items; ++i) {
		if (output[i] != i) {
			tpie::log_error() << "B tree dump incorrect @ " << i << std::endl;
			return false;
		}
	}
	return true;
}

class verify_iterator {
public:
	verify_iterator(size_t * outputSize, bool * error, key_type * i, key_type a, key_type step, key_type b)
		: outputSize(outputSize)
		, error(error)
		, i(i)
		, a(a)
		, step(step)
		, b(b)
	{
	}

	void operator++() const {}

	const verify_iterator & operator*() const {
		return *this;
	}

	const verify_iterator & operator=(key_type v) const {
		++*outputSize;
		if (*error) return *this;
		if (v != *i) {
			tpie::log_error() << "B tree dump incorrect: got " << v << ", expected " << *i << std::endl;
			*error = true;
		} else {
			*i += step;
		}
		return *this;
	}

private:
	size_t * outputSize;
	bool * error;
	key_type * i;
	key_type a;
	key_type step;
	key_type b;
};

bool verify_tree(tree_type & t, key_type a, key_type step, key_type b) {
	size_t items = (a == b) ? 0 : ((b-a+step-1)/step);
	size_t outputSize = 0;
	key_type i = a;
	bool error = false;
	t.in_order_dump(verify_iterator(&outputSize, &error, &i, a, step, b));
	if (outputSize != items) {
		tpie::log_error()
			<< "B tree dump output incorrect no. of items\n"
			<< "Expected " << items << "; got " << outputSize << '\n'
			<< std::flush;
		return false;
	}
	return !error;
}

bool b_tree_erase_test(key_type items, size_t fanout) {
	tree_type t;
	t.open();
	if (fanout != 0) {
		tpie::blocks::b_tree_parameters params = t.get_parameters();
		params.nodeMax = params.leafMax = fanout;
		params.nodeMin = params.leafMin = (fanout + 3) / 4;
		t.set_parameters(params);
	}
	{
		builder_type b(t);
		for (key_type i = 0; i < items; ++i) {
			b.push(i);
		}
		b.end();
	}
	if (!verify_tree(t, 0, 1, items)) return false;
	for (key_type i = 0; i < items; i += 2) {
		t.erase(i);
	}
	if (!verify_tree(t, 1, 2, items)) return false;
	for (key_type i = 0; i < items; i += 2) {
		t.insert(i);
	}
	if (!verify_tree(t, 0, 1, items)) return false;
	for (key_type i = 0; i < items; ++i) {
		t.erase(i);
	}
	if (!verify_tree(t, 0, 0, 0)) return false;
	return true;
}

bool b_tree_builder_test(key_type n) {
	tree_type t;
	t.open();
	{
		builder_type builder(t);
		for (key_type i = 0; i < n; ++i) builder.push(i);
		builder.end();
	}
	if (!verify_tree(t, 0, 1, n)) return false;
	return true;
}

int main(int argc, char ** argv) {
	return tpie::tests(argc, argv)
	.test(b_tree_test, "b_tree")
	.test(b_tree_test_2, "b_tree_2", "n", static_cast<key_type>(1000))
	.test(b_tree_erase_test, "b_tree_erase",
		  "n", static_cast<key_type>(1000),
		  "fanout", static_cast<size_t>(0))
	.test(b_tree_builder_test, "b_tree_builder", "n", static_cast<key_type>(1000))
	;
}
