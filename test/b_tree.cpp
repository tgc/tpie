// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet :
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
#include <tpie/tpie.h>
#include <tpie/blocks/b_tree.h>

class traits {
public:
	typedef size_t Key;
	typedef size_t Value;
	typedef std::less<size_t> Compare;
	static Key key_of_value(const Value & v) { return static_cast<Key>(v); }
};

class number_output {
	class dereferenced {
	public:
		dereferenced & operator=(size_t n) {
			std::cout << n << ' ';
			return *this;
		}
	};
public:
	number_output & operator++() { return *this; }
	dereferenced operator*() { return dereferenced(); }
};

int main() {
	tpie::tpie_init();
	{
		tpie::stderr_log_target tgt(tpie::LOG_DEBUG);
		tpie::get_log().add_target(&tgt);
		tpie::blocks::b_tree<traits> t;
		std::string cmd;
		size_t key;
		while (std::cin >> cmd) {
			std::string line;
			std::getline(std::cin, line);
			if (cmd == "insert") {
				std::stringstream ss(line);
				while (ss >> key)
					t.insert(key);
			} else if (cmd == "erase") {
				std::stringstream ss(line);
				while (ss >> key)
					t.erase(key);
			} else if (cmd == "dump") {
				t.in_order_dump(number_output());
				std::cout << std::endl;
			}
		}
	}
	tpie::tpie_finish();
	return 0;
}
