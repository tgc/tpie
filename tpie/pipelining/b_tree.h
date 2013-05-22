// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
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

#ifndef TPIE_PIPELINING_B_TREE_H
#define TPIE_PIPELINING_B_TREE_H

#include <tpie/blocks/b_tree.h>

#include <tpie/pipelining/node.h>
#include <tpie/pipelining/pipe_base.h>
#include <tpie/pipelining/factory_helpers.h>

namespace tpie {

namespace pipelining {

namespace bits {

template <typename Traits>
class b_tree_builder_t : public node {
public:
	typedef typename Traits::Value item_type;

	b_tree_builder_t(blocks::b_tree_builder<Traits> & builder)
		: m_builder(builder)
	{
		set_name("B-tree builder", PRIORITY_INSIGNIFICANT);
	}

	void push(const item_type & v) {
		m_builder.push(v);
	}

	virtual void end() override {
		m_builder.end();
	}

private:
	blocks::b_tree_builder<Traits> & m_builder;
};

} // namespace bits

template <typename Traits>
pipe_end<termfactory_1<bits::b_tree_builder_t<Traits>, blocks::b_tree_builder<Traits> &> >
b_tree_builder(blocks::b_tree_builder<Traits> & builder) {
	return termfactory_1<bits::b_tree_builder_t<Traits>, blocks::b_tree_builder<Traits> &>(builder);
}

} // namespace pipelining

} // namespace tpie

#endif // TPIE_PIPELINING_B_TREE_H
