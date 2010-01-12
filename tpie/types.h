// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2009, The TPIE development team
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
#ifndef __TPIE_TYPES_H__
#define __TPIE_TYPES_H__

#include <boost/cstdint.hpp>

namespace tpie {

using boost::uint64_t;
using boost::int64_t;

using boost::uint32_t;
using boost::int32_t;

typedef boost::uint64_t stream_size_type;
typedef boost::int64_t stream_offset_type;

typedef ::size_t memory_size_type;
typedef ::ssize_t memory_offset_type;

}

#endif //__TPIE_TYPES_H__