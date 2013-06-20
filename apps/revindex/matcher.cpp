// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2012, The TPIE development team
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
#include <vector>
#include <tpie/file_stream.h>

namespace TP = tpie;

int main() {
	TP::tpie_init();
	TP::get_memory_manager().set_limit(50*1024*1024);
	{
		tpie::file_stream<size_t> posStream;
		posStream.open("positions");
		tpie::file_stream<size_t> lists;
		lists.open("lists");
		std::string line;
		while (std::getline(std::cin, line)) {
			std::vector<size_t> positions;
			for (size_t i = 0; i < line.size(); ++i) {
				unsigned char c = static_cast<unsigned char>(line[i]);
				size_t i1, i2;
				std::cout << "Seek to " << (int) c << std::endl;
				lists.seek(c);
				i1 = lists.read();
				i2 = lists.read();
				std::cout << "Seek to " << i1 << std::endl;
				posStream.seek(i1);
				std::vector<size_t> p(i2-i1);
				posStream.read(p.begin(), p.end());
				if (i == 0) positions = p;
				else {
					for (size_t j = 0; j < p.size(); ++j) p[j] -= i;
					std::vector<size_t> res(std::min(p.size(),
													 positions.size()));
					size_t k =
						std::set_intersection
						(p.begin(), p.end(),
						 positions.begin(), positions.end(),
						 res.begin()) - res.begin();
					res.resize(k);
					positions = res;
				}
			}
			std::cout << positions.size() << std::endl;
			for (size_t i = 0; i < positions.size(); ++i)
				std::cout << positions[i] << std::endl;
		}
	}
	TP::tpie_finish();
	return 0;
}
