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

///////////////////////////////////////////////////////////////////////////////
/// \file blocks/b_tree_builder.h  Efficient B+ tree bottom-up building.
///////////////////////////////////////////////////////////////////////////////

#ifndef TPIE_BLOCKS_B_TREE_BUILDER_H
#define TPIE_BLOCKS_B_TREE_BUILDER_H

namespace tpie {

namespace blocks {

///////////////////////////////////////////////////////////////////////////////
/// \brief  b_tree_builder helper class.
///
/// Provides access to deques of (block_handle, key)-pairs, one deque for each
/// level in the B tree to be built.
/// A deque can be added with push_layer(), and the number of deques is
/// accessed through height().
///
/// A deque can be accessed through the methods
/// empty(), layer_size(), front_key(), front_handle(),
/// and can be modified through the methods
/// push_block() and pop_front().
///////////////////////////////////////////////////////////////////////////////
template <typename Traits>
class b_tree_block_overview {
	typedef typename Traits::Key Key;
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

	// We could use a std::pair, but `handle` and `key` are better than
	// `first` and `second`.
	struct handle_key_pair {
		block_handle handle;
		Key key;
	};

	// Similar to std::make_pair.
	handle_key_pair make_pair(block_handle handle, Key key) {
		handle_key_pair res;
		res.handle = handle;
		res.key = key;
		return res;
	}

public:
	b_tree_block_overview(const b_tree_parameters & params)
		: m_params(params)
		, m_layers(1)
	{
	}

	memory_size_type height() const {
		return m_layers.size();
	}

	bool empty(memory_size_type level) const {
		return m_layers[level].empty();
	}

	void push_layer() {
		m_layers.push_back(std::deque<handle_key_pair>());
	}

	memory_size_type layer_size(memory_size_type level) const {
		return m_layers[level].size();
	}

	Key front_key(memory_size_type level) const {
		if (empty(level)) throw exception("front_key: empty level");
		return m_layers[level].front().key;
	}

	block_handle front_handle(memory_size_type level) const {
		if (empty(level)) throw exception("front_handle: empty level");
		return m_layers[level].front().handle;
	}

	void push_block(memory_size_type level, Key firstKey, block_handle hdl) {
		if (hdl == block_handle(0)) throw exception("Zero handle in push_block");
		m_layers[level].push_back(make_pair(hdl, firstKey));
	}

	void pop_front(memory_size_type level) {
		if (empty(level)) throw exception("pop_front: empty level");
		m_layers[level].pop_front();
	}

private:
	const b_tree_parameters & m_params;
	std::vector<std::deque<handle_key_pair> > m_layers;
};

///////////////////////////////////////////////////////////////////////////////
/// \brief  Build a B tree efficiently from a sorted sequence.
///
/// To use, instantiate a b_tree and pass it to the constructor of a builder.
/// Call push() for each item in the sorted sequence,
/// and call end() when done.
///
/// The builder maintains lists of B trees of varying heights.
/// The push() method inserts the value into the next leaf,
/// and when a leaf is full it is inserted into the list of trees
/// as a B tree of height 0.
///
/// When more than (nodeMin+nodeMax) B trees of height n have been created,
/// they are joined into a single B tree of height n+1.
/// We cannot join the B trees as soon as we have just nodeMax trees,
/// since that could leave behind an underfull right spine in the final tree.
///
/// In the end, all the remaining B trees are merged into the resulting B tree.
///////////////////////////////////////////////////////////////////////////////
template <typename Traits>
class b_tree_builder {
	typedef typename Traits::Key Key;
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

public:
	b_tree_builder(b_tree<Traits> & res)
		: m_state(builder_state::EMPTY)
		, m_tree(res)
		, m_blocks(m_tree.m_blocks)
		, m_params(m_tree.m_params)
		, m_layers(m_params)
	{
		m_blocks.get_free_block(m_leafBuffer);
	}

	void push(const Value & v) {
		if (m_state == builder_state::BUILT) throw exception("b_tree_builder: push() after end()");
		m_state = builder_state::BUILDING;

		{
			b_tree_leaf<Traits> leaf(m_leafBuffer, m_params);
			if (leaf.full()) {
				push_leaf();
				new_leaf();
			}
		}

		{
			b_tree_leaf<Traits> leaf(m_leafBuffer, m_params);
			if (leaf.empty()) {
				m_leafKey = Traits::key_of_value(v);
			}
			leaf.insert(v);
		}
	}

	void end() {
		switch (m_state) {
			case builder_state::BUILT:
				throw exception("b_tree_builder: end() after end()");
			case builder_state::EMPTY:
				m_state = builder_state::BUILT;
				return;
			case builder_state::BUILDING:
				break;
		}

		{
			b_tree_leaf<Traits> leaf(m_leafBuffer, m_params);
			if (!leaf.empty())
				push_leaf();
		}

		if (m_layers.height() == 0)
			throw exception("height == 0 in end()");

		for (memory_size_type i = 0; i < m_layers.height(); ++i) {
			if (i == m_layers.height() - 1
					&& m_layers.layer_size(i) == 1)
				break;

			finish_layer(i);
		}

		block_handle hdl = m_layers.front_handle(m_layers.height() - 1);
		memory_size_type level = m_layers.height() - 1;

		m_tree.set_root(hdl, level);

		m_state = builder_state::BUILT;
	}

private:
	void push_leaf() {
		m_layers.push_block(0, m_leafKey, m_leafBuffer.get_handle());
		m_blocks.write_block(m_leafBuffer);
		reduce_layer(0);
	}

	void new_leaf() {
		m_blocks.get_free_block(m_leafBuffer);
		b_tree_leaf<Traits> leaf(m_leafBuffer, m_params);
		leaf.clear();
	}

	void reduce_layer(memory_size_type level) {
		memory_size_type min = m_params.nodeMin;
		memory_size_type max = m_params.nodeMax;
		if (m_layers.layer_size(level) > min + max) {
			while (m_layers.layer_size(level) > min + max) {
				push_block(max, level+1);
			}
			reduce_layer(level+1);
		}
	}

	void finish_layer(memory_size_type level) {
		memory_size_type min = m_params.nodeMin;
		memory_size_type max = m_params.nodeMax;
		while (m_layers.layer_size(level) > min + max) {
			push_block(max, level+1);
		}
		if (m_layers.layer_size(level) > max)
			push_block(m_layers.layer_size(level) - min, level+1);
		if (m_layers.layer_size(level) > 0)
			push_block(m_layers.layer_size(level), level+1);
	}

	void push_block(memory_size_type children, memory_size_type level) {
		if (level == 0)
			throw exception("push_block: level == 0");
		if (level > m_layers.height())
			throw exception("push_block: level > m_layers.height()");

		if (level == m_layers.height()) {
			m_layers.push_layer();
		}

		block_buffer & buf = m_blockBuffer;
		m_blocks.get_free_block(buf);
		b_tree_block<Traits> block(buf, m_params);
		block.clear();
		m_layers.push_block(level, m_layers.front_key(level-1), buf.get_handle());
		for (memory_size_type i = 0; i < children; ++i) {
			if (m_layers.empty(level-1))
				throw exception("push_block: source is empty");
			if (i == 0) {
				block.push_first_child(m_layers.front_handle(level-1));
			} else {
				block.push_child(m_layers.front_key(level-1), m_layers.front_handle(level-1));
			}
			m_layers.pop_front(level-1);
		}
		m_blocks.write_block(buf);
	}

	builder_state::type m_state;

	/** Destination tree to build. */
	b_tree<Traits> & m_tree;

	/** The block collection underlying the tree to build. */
	block_collection & m_blocks;

	/** Reference to m_tree.m_params. */
	const b_tree_parameters & m_params;

	/** Next leaf to push to the tree. */
	block_buffer m_leafBuffer;

	/** Key of first value inserted into the current leaf. */
	Key m_leafKey;

	/** Temporary buffer used in push_block. */
	block_buffer m_blockBuffer;

	/** B tree fragments that will make up the final B tree. */
	b_tree_block_overview<Traits> m_layers;
};

} // namespace blocks

} // namespace tpie

#endif // TPIE_BLOCKS_B_TREE_BUILDER_H
