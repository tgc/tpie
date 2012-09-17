// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet cino+=(0 :
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

///////////////////////////////////////////////////////////////////////////////
/// \file data_structure.h  Data structures for pipelining
///////////////////////////////////////////////////////////////////////////////

#ifndef __TPIE_PIPELINING_DATA_STRUCTURE_H__
#define __TPIE_PIPELINING_DATA_STRUCTURE_H__

#include <tpie/pipelining/tokens.h>

namespace tpie {

namespace pipelining {

class data_structure : public segment_base {
public:
	data_structure()
		: segment_base()
	{
		m_selfPipeSegment = 0;
		m_selfDataStructure = this;
	}

	data_structure(const data_structure & other)
		: segment_base(other)
	{
		m_selfPipeSegment = 0;
		m_selfDataStructure = this;
	}

	template <typename T>
	void set(T * ds) {
		get_segment_map()->find_authority()->set_data_structure(get_id(), ds);
	}

	template <typename T>
	T * get() {
		boost::any a = get_segment_map()->find_authority()->get_data_structure(get_id());
		return boost::any_cast<T *>(a);
	}
};

} // namespace pipelining

} // namespace tpie

#endif // __TPIE_PIPELINING_DATA_STRUCTURE_H__
