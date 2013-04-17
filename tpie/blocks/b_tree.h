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

#include <tpie/tempname.h>
#include <tpie/blocks/block_collection.h>

namespace tpie {

namespace blocks {

template <typename T>
class b_tree_traits {
public:
	typedef T Key;

	typedef T Value;

	/* [Notation: If `R` is a Compare object, then `a R b` means `R(a, b)`.]
	 *
	 * Antisymmetry: If `<` is a Compare object, and `a` and `b` are Keys,
	 * then `a` and `b` are considered equal if not a < b and not b < a.
	 * We write (a == b) to mean (not a < b and not b < a).
	 *
	 * Transitivity: If `<` is a Compare object, and `a`, `b`, `c` are Keys,
	 * then if a < b and b < c, then a < c.
	 */
	typedef std::less<Key> Compare;

	/* It is assumed that v1 == v2 iff key_of_value(v1) == key_of_value(v2). */
	static Key key_of_value(const Value & v) { return static_cast<Key>(v); }
};

struct b_tree_block_header {
	uint64_t keys;
};

template <typename Key>
class key_ops {
public:
	typedef Key * ptr_type;
	static ptr_type get_ptr(Key & k)    { return &k; }
	static Key      get_val(ptr_type p) { return *p; }
};

enum fuse_result {
	/** `left` and `right` are still in use. */
	fuse_share,

	/** All of `right` was merged into `left`. */
	fuse_merge
};

template <typename Traits>
class b_tree_block {
	typedef typename Traits::Key Key;
public:
	static memory_size_type calculate_fanout(memory_size_type blockSize) {
		blockSize -= sizeof(b_tree_block_header);
		blockSize -= sizeof(block_handle); // one more child pointer than keys
		memory_size_type perKey = sizeof(block_handle) + sizeof(Key);
		return blockSize / perKey; // floored division
	}

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
		return keys() < m_fanout/2;
	}

	Key key(memory_size_type idx) const {
		return m_keys[idx];
	}

	block_handle child(memory_size_type idx) const {
		return m_children[idx];
	}

	void erase(memory_size_type idx) {
		for (memory_size_type i = idx+1; i != keys(); ++i) {
			m_keys[i-1] = m_keys[i];
		}
		--m_header->keys;
	}

	void insert(memory_size_type i, Key k, block_handle leftChild, block_handle rightChild) {
		if (full()) throw exception("Insert on full block");

		m_children[i] = leftChild;

		block_handle c = rightChild;
		while (i < keys()) {
			std::swap(m_children[i+1], c);
			std::swap(m_keys[i], k);
			++i;
		}
		m_children[i+1] = c;
		m_keys[i] = k;
		++m_header->keys;
	}

	Key split_insert(memory_size_type insertIndex,
					 Key insertKey,
					 block_handle leftChild,
					 block_handle rightChild,
					 block_buffer & leftBuf,
					 block_buffer & rightBuf)
	{
		if (keys() != m_fanout) throw exception("split_insert on non-full block");

		typedef key_ops<Key> O;
		typedef typename O::ptr_type KeyPtr;

		std::vector<block_handle> children(m_fanout+2);
		std::vector<KeyPtr> keys(m_fanout+1);

		{
			for (memory_size_type i = 0; i < m_fanout; ++i) {
				memory_size_type dest = i;
				if (insertIndex <= i) ++dest;
				children[dest] = m_children[i];
				keys[dest] = O::get_ptr(m_keys[i]);
			}
			children[m_fanout+1] = m_children[m_fanout];

			keys[insertIndex] = O::get_ptr(insertKey);
			children[insertIndex] = leftChild;
			children[insertIndex+1] = rightChild;
		}

		Key midKey;

		{
			b_tree_block<Traits> left(leftBuf, m_fanout);
			b_tree_block<Traits> right(rightBuf, m_fanout);

			if (left.keys() != 0) throw exception("split_insert to non-empty left");
			if (right.keys() != 0) throw exception("split_insert to non-empty right");

			memory_size_type in = 0;
			memory_size_type out;
			for (out = 0; in*2 < m_fanout; ++out) {
				left.m_children[out] = children[in];
				left.m_keys[out] = O::get_val(keys[in]);
				++in;
			}
			left.m_children[out] = children[in];
			left.m_header->keys = static_cast<uint64_t>(out);

			midKey = O::get_val(keys[in]);
			++in;

			for (out = 0; in < m_fanout+1; ++out) {
				right.m_children[out] = children[in];
				right.m_keys[out] = O::get_val(keys[in]);
				++in;
			}
			right.m_children[out] = children[in];
			right.m_header->keys = static_cast<uint64_t>(out);
		}

		m_header->keys = 0;
		return midKey;
	}

	fuse_result fuse(memory_size_type rightIndex,
					 block_buffer & leftBuf,
					 block_buffer & rightBuf)
	{
		b_tree_block<Traits> left(leftBuf, m_fanout);
		b_tree_block<Traits> right(rightBuf, m_fanout);

		std::vector<Key> keys(left.keys() + 1 + right.keys());
		std::vector<block_handle> children(left.keys() + right.keys() + 2);

		{
			memory_size_type output = 0;
			for (memory_size_type i = 0; i < left.keys(); ++i) {
				keys[output] = left.key(i);
				children[output] = left.child(i);
				++output;
			}
			keys[output] = key(rightIndex-1);
			children[output] = left.child(output);
			++output;
			for (memory_size_type i = 0; i < right.keys(); ++i) {
				keys[output] = right.key(i);
				children[output] = right.child(i);
				++output;
			}
			children[output] = right.child(right.keys());
		}

		if (keys.size() <= m_fanout) {
			std::copy(keys.begin(), keys.end(), &left.m_keys[0]);
			std::copy(children.begin(), children.end(), &left.m_children[0]);
			left.m_header->keys = static_cast<uint64_t>(keys.size());

			for (memory_size_type i = rightIndex; i < this->keys(); ++i) {
				m_keys[i-1] = m_keys[i];
				m_children[i] = m_children[i+1];
			}
			--m_header->keys;

			return fuse_merge;

		} else {

			memory_size_type half = keys.size()/2;
			std::copy(keys.begin(), keys.begin() + half, &left.m_keys[0]);
			std::copy(children.begin(), children.begin() + (half + 1), &left.m_children[0]);
			left.m_header->keys = static_cast<uint64_t>(half);

			m_keys[rightIndex-1] = keys[half];

			std::copy(keys.begin() + 1, keys.end(), &right.m_keys[0]);
			std::copy(children.begin() + 1, children.end(), &right.m_children[0]);
			left.m_header->keys = static_cast<uint64_t>(keys.size() - half - 1);

			return fuse_share;
		}
	}

private:
	b_tree_block_header * m_header;
	block_handle * m_children;
	Key * m_keys;
	memory_size_type m_fanout;
};

class b_tree_path {
public:
	void follow(block_handle b, memory_size_type index) {
		m_components.push_back(std::make_pair(b, index));
	}

	void parent() {
		m_components.pop_back();
	}

	block_handle current_block() const {
		return m_components[m_components.size()-1].first;
	}

	memory_size_type current_index() const {
		return m_components[m_components.size()-1].second;
	}

	bool empty() const {
		return m_components.empty();
	}

private:
	std::vector<std::pair<block_handle, memory_size_type> > m_components;
};

template <typename Traits>
class b_tree {
	typedef typename Traits::Key Key;
	typedef typename Traits::Compare Compare;

public:
	b_tree()
		: m_root(0)
	{
		open();
	}

	~b_tree() {
		close();
	}

	void insert(Key k) {
		block_buffer buf;
		read_root(buf);
		b_tree_path p = key_path(buf, k);
		b_tree_block<Traits> block(buf, fanout());

		memory_size_type i = p.current_index();

		block_handle leftChild(0);
		block_handle rightChild(0);

		while (block.full()) {
			block_buffer left;
			block_buffer right;
			m_blocks.get_free_block(left);
			m_blocks.get_free_block(right);
			k = block.split_insert(i, k, leftChild, rightChild, left, right);
			m_blocks.write_block(left);
			m_blocks.write_block(right);
			leftChild = left.get_handle();
			rightChild = right.get_handle();

			p.parent();
			if (p.empty()) break;

			m_blocks.free_block(buf);

			i = p.current_index();
			m_blocks.read_block(p.current_block(), buf);
		}

		if (p.empty()) {
			if (buf.get_handle() != m_root) throw exception("Didn't end up at the root");
			if (block.keys() != 0) throw exception("Unexpected nonempty root");
			i = 0;
		}

		block.insert(i, k, leftChild, rightChild);
		m_blocks.write_block(buf);
	}

	void erase(Key k) {
		block_buffer buf;
		read_root(buf);
		b_tree_path p = key_path(buf, k);
		b_tree_block<Traits> block(buf, fanout());

		memory_size_type idx = p.current_index();
		log_debug() << "erase key " << k << " in block " << p.current_block() << "/" << buf.get_handle() << " idx " << idx << std::endl;

		if (idx == block.keys() || m_comp(block.key(idx), k)) {
			log_debug() << "erase non-existing key " << k << " " << idx << "; noop." << std::endl;
			return;
		}

		block.erase(idx);
		m_blocks.write_block(buf);

		while (block.underfull()) {
			memory_size_type i = p.current_index();
			p.parent();
			if (p.empty()) {
				throw exception("Empty path?");
			}
			log_debug() << "Read block " << p.current_block() << std::endl;
			m_blocks.read_block(p.current_block(), buf);
			memory_size_type rightIndex = (i == 0) ? 1 : i;
			block_buffer left;
			block_buffer right;
			m_blocks.read_block(block.child(rightIndex-1), left);
			m_blocks.read_block(block.child(rightIndex), right);
			bool updatedRoot = false;
			switch (block.fuse(rightIndex, left, right)) {
				case fuse_share:
					log_debug() << "Erase fuse_share of " << left.get_handle() << " and " << right.get_handle() << std::endl;
					m_blocks.write_block(left);
					m_blocks.write_block(right);
					break;
				case fuse_merge:
					log_debug() << "Erase fuse_merge of " << left.get_handle() << " and " << right.get_handle() << std::endl;
					m_blocks.write_block(left);
					m_blocks.free_block(right);
					if (p.current_block() == m_root) {
						m_root = left.get_handle();
						log_debug() << "New root " << m_root << std::endl;
						updatedRoot = true;
					}
					break;
			}
			m_blocks.write_block(buf);
			if (updatedRoot)
				break;
		}
	}

	memory_size_type count(Key k) {
		block_buffer buf;
		read_root(buf);
		b_tree_path p = key_path(buf, k);
		b_tree_block<Traits> block(buf, fanout());
		memory_size_type i = p.current_index();
		if (i == block.keys()) return 0;
		if (m_comp(k, block.key(i))) return 0;
		if (m_comp(block.key(i), k)) return 0;
		return 1;
	}

private:
	void open() {
		m_blocks.open(m_tempFile.path(), true);
	}

	void close() {
		m_blocks.close();
		boost::filesystem::remove(m_tempFile.path());
	}

	memory_size_type block_size() {
		return block_collection::default_block_size();
	}

	memory_size_type fanout() {
		return b_tree_block<Traits>::calculate_fanout(block_size());
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
	b_tree_path key_path(block_buffer & buf, Key k) {
		b_tree_path res;

		while (true) {
			b_tree_block<Traits> b(buf, fanout());

			memory_size_type i;
			for (i = 0; i != b.keys(); ++i)
				if (!m_comp(b.key(i), k)) break;

			res.follow(buf.get_handle(), i);

			if (i != b.keys() && !m_comp(k, b.key(i)))
				break; // found by anti-symmetry
			else if (b.child(i) == block_handle(0))
				break; // cannot seek any further
			else
				m_blocks.read_block(b.child(i), buf);
		}

		return res;
	}

public:
	template <typename It>
	void in_order_dump(It it) {
		if (m_root == block_handle(0)) {
			log_debug() << "in_order_dump: Empty tree" << std::endl;
			return;
		}
		in_order_dump_visit(it, m_root);
	}

private:
	template <typename It>
	void in_order_dump_visit(It it, block_handle id) {
		if (id == block_handle(0)) return;
		block_buffer buf;
		m_blocks.read_block(id, buf);
		b_tree_block<Traits> block(buf, fanout());
		if (block.underfull() && id != m_root) {
			log_error() << "in_order_dump: Underfull non-root block " << id << std::endl;
		}
		for (memory_size_type i = 0; i < block.keys(); ++i) {
			in_order_dump_visit(it, block.child(i));
			*it = block.key(i);
			++it;
		}
		in_order_dump_visit(it, block.child(block.keys()));
	}

public:
	template <typename It>
	void bfs_order_block_dump(It it) {
		if (m_root == block_handle(0)) {
			log_debug() << "in_order_dump: Empty tree" << std::endl;
			return;
		}
		block_buffer buf;
		std::deque<block_handle> to_visit;
		to_visit.push_back(m_root);
		while (!to_visit.empty()) {
			const block_handle id = to_visit.front();
			to_visit.pop_front();
			m_blocks.read_block(id, buf);
			const b_tree_block<Traits> block(buf, fanout());
			if (block.underfull() && id != m_root) {
				log_error() << "in_order_dump: Underfull non-root block " << id << std::endl;
			}
			for (memory_size_type i = 0; i <= block.keys(); ++i) {
				if (block.child(i) == block_handle(0)) continue;
				to_visit.push_back(block.child(i));
			}
			it(id, block);
		}
	}

private:
	tpie::temp_file m_tempFile;
	block_collection m_blocks;
	block_handle m_root;
	Compare m_comp;
};

} // namespace blocks

} // namespace tpie

#endif // TPIE_BLOCKS_B_TREE_H
