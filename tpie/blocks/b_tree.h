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

#ifndef TPIE_BLOCKS_B_TREE_H
#define TPIE_BLOCKS_B_TREE_H

#include <boost/filesystem.hpp>

#include <tpie/blocks/block_collection.h>

namespace tpie {

namespace blocks {

struct b_tree_block_header {
	uint64_t keys;
};

template <typename Key>
class b_tree_block {
public:
	b_tree_block(block_buffer & buffer, memory_size_type fanout) {
		char * children = buffer.get() + sizeof(b_tree_block_header);
		char * keys = children + (1+fanout) * sizeof(block_handle);

		m_header = reinterpret_cast<b_tree_block_header *>(buffer.get());
		m_children = reinterpret_cast<block_handle *>(children);
		m_keys = reinterpret_cast<Key *>(keys);

		m_fanout = fanout;
	}

	memory_size_type keys() const {
		return static_cast<memory_size_type>(m_header->keys);
	}

	bool full() const {
		return keys() == m_fanout;
	}

	bool underfull() const {
		return keys()*2 < m_fanout;
	}

	Key key(memory_size_type idx) const {
		return m_keys[idx];
	}

	block_handle child(memory_size_type idx) const {
		return m_children[idx];
	}

	void insert(memory_size_type i, Key k) {
		block_handle c = block_handle(0);
		while (i < keys()) {
			std::swap(m_children[i], c);
			std::swap(m_keys[i], k);
			++i;
		}
		m_children[i] = c;
		m_keys[i] = k;
		++m_header->keys;
	}

	Key split_insert(memory_size_type insertIndex,
					 Key insertKey,
					 block_buffer & leftBuf,
					 block_buffer & rightBuf)
	{
		for (memory_size_type i = 0; i != keys(); ++i)
			if (child(i) != 0) throw exception("split_insert on non-leaf");

		b_tree_block<Key> left(leftBuf, m_fanout);
		b_tree_block<Key> right(rightBuf, m_fanout);

		if (left.keys() != 0) throw exception("split_insert to non-empty left");
		if (right.keys() != 0) throw exception("split_insert to non-empty right");

		Key midKey;
		bool nextIsMid = false;

		for (memory_size_type i = 0; i < keys() || i == insertIndex; ++i) {
			b_tree_block<Key> & dest = left.underfull() ? left : right;

			Key k;

			if (i == insertIndex) {
				k = insertKey;
				--i, --insertIndex;
			} else {
				k = key(i);
			}

			if (nextIsMid) {
				midKey = k;
				nextIsMid = false;
			} else {
				dest.insert(dest.keys(), k);

				// if we just filled up the destination,
				// next key is the middle key
				nextIsMid = !dest.underfull();
			}
		}

		m_header->keys = 0;
		return midKey;
	}

private:
	b_tree_block_header * m_header;
	block_handle * m_children;
	Key * m_keys;
	memory_size_type m_fanout;
};

template <typename Key>
class b_tree {
	typedef std::less<Key> Compare;

public:
	b_tree()
		: m_root(0)
	{
		open();
	}

	~b_tree() {
		close();
	}

	void insert(Key v) {
		block_buffer buf;
		read_root(buf);
		memory_size_type i = find_key(buf, v);
		b_tree_block<Key> block(buf, fanout());
		if (block.full()) {
			block_buffer left;
			block_buffer right;
			m_blocks.get_free_block(left);
			m_blocks.get_free_block(right);
			block.split_insert(i, v, left, right);
			m_blocks.write_block(buf);
			m_blocks.write_block(left);
			m_blocks.write_block(right);
		} else {
			block.insert(i, v);
			m_blocks.write_block(buf);
		}
	}

private:
	void open() {
		m_blocks.open(m_tempFile.path(), true);
	}

	void close() {
		m_blocks.close();
		boost::filesystem::remove(m_tempFile.path());
	}

	memory_size_type fanout() {
		return 4;
	}

	void read_root(block_buffer & b) {
		if (m_root == block_handle(0)) {
			m_root = m_blocks.get_free_block();
		}
		m_blocks.read_block(m_root, b);
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief  Search the tree for the given key.
	///
	/// Reuses the given block buffer when changing node and returns the index
	/// in the block where the key is found or should be inserted.
	///////////////////////////////////////////////////////////////////////////
	memory_size_type find_key(block_buffer & buf, Key v) {
		log_debug() << "Find_key " << v << std::endl;
		while (true) {
			b_tree_block<Key> b(buf, fanout());

			log_debug() << "Find_key iteration" << std::endl;
			for (size_t i = 0; i < b.keys(); ++i) log_debug() << b.key(i) << ' ';
			log_debug() << std::endl;
			for (size_t i = 0; i <= b.keys(); ++i) log_debug() << b.child(i) << ' ';
			log_debug() << std::endl;

			memory_size_type i;
			for (i = 0; i != b.keys(); ++i)
				if (!m_comp(b.key(i), v)) break;

			if (i != b.keys() && !m_comp(v, b.key(i)))
				return i; // found by anti-symmetry
			else if (b.child(i) == block_handle(0))
				return i; // cannot seek any further
			else {
				m_blocks.read_block(b.child(i), buf);
			}
		}
	}

	tpie::temp_file m_tempFile;
	block_collection m_blocks;
	block_handle m_root;
	Compare m_comp;
};

} // namespace blocks

} // namespace tpie

#endif // TPIE_BLOCKS_B_TREE_H
