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
/// \file blocks/b_tree.h  B+ tree in external memory
///
/// The implementation is based off the B tree described in
/// Lars Arge, External Memory Geometric Data Structures (2005).
///
/// The main class `b_tree` handles reading from and writing to the underlying
/// block collection.
///
/// The helper classes `b_tree_block` and `b_tree_leaf` operate on individual
/// blocks that have been read and support primitive operations such as
/// inserting, erasing, splitting and fusing.
///
/// The `b_tree` class uses these to insert and erase elements on a tree-wide
/// scale.
///
/// There is a clear division of responsibility: The block and leaf classes
/// have no access to reading from and writing to the block collection,
/// and the b_tree class cannot modify the key/child/value arrays directly,
/// but must do so through the helpers.
///////////////////////////////////////////////////////////////////////////////

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

struct b_tree_header {
	/** Number of child blocks or leaf values. */
	uint64_t degree;
};

// Contains the "branching parameter" nodeMax and the "leaf parameter" leafMax,
// which are called b and k respectively in the paper.
struct b_tree_parameters {
	uint64_t nodeMin;
	uint64_t nodeMax;
	uint64_t leafMin;
	uint64_t leafMax;
};

template <typename Key,
		 bool PtrSized = sizeof(Key) <= sizeof(Key *)>
class key_ops {
public:
	typedef Key * ptr_type;
	static ptr_type get_ptr(Key & k)    { return &k; }
	static Key      get_val(ptr_type p) { return *p; }
};

template <typename Key>
class key_ops<Key, true> {
public:
	typedef Key ptr_type;
	static ptr_type get_ptr(Key & k)    { return k; }
	static Key      get_val(ptr_type p) { return p; }
};

// Result of a fuse operation.
enum fuse_result {
	/** `left` and `right` are still in use. */
	fuse_share,

	/** All of `right` was merged into `left`. */
	fuse_merge
};

// Functor for partitioning an array of Values according to a given Key.
template <typename Traits>
class key_less_than {
	typedef typename Traits::Key Key;
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

	Compare comp;
	Key key;

public:
	key_less_than(const Compare & comp, const Value & v)
		: comp(comp)
		, key(Traits::key_of_value(v))
	{
	}

	bool operator()(const Value & v) const {
		return comp(Traits::key_of_value(v), key);
	}
};

// Comparator for sorting an array of Values by their Keys.
template <typename Traits>
class key_less {
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

	Compare comp;

public:
	key_less(const Compare & comp)
		: comp(comp)
	{
	}

	bool operator()(const Value & v1, const Value & v2) const {
		return comp(Traits::key_of_value(v1), Traits::key_of_value(v2));
	}
};

template <typename Traits>
class b_tree_leaf {
	typedef typename Traits::Key Key;
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

public:
	static memory_size_type calculate_fanout(memory_size_type blockSize) {
		blockSize -= sizeof(b_tree_header);
		return blockSize / sizeof(Value); // floored division
	}

	b_tree_leaf(block_buffer & buffer, const b_tree_parameters & params)
		: m_params(params)
	{
		m_header = reinterpret_cast<b_tree_header *>(buffer.get());
		m_values = reinterpret_cast<Value *>(buffer.get() + sizeof(b_tree_header));
	}

	void clear() {
		m_header->degree = 0;
	}

	memory_size_type degree() const {
		return static_cast<memory_size_type>(m_header->degree);
	}

	const Value & operator[](memory_size_type i) const {
		return m_values[i];
	}

	memory_size_type index_of(const Key & key, Compare comp) const {
		memory_size_type i = 0;
		while (i != degree()) {
			Key k = Traits::key_of_value(m_values[i]);
			if (!comp(k, key) && !comp(key, k)) break;
			++i;
		}
		return i;
	}

	// Called by b_tree::count.
	memory_size_type count(const Key & key, const Compare & comp) const {
		if (index_of(key, comp) == degree())
			return 0;
		else
			return 1;
	}

	// Definition 1, first bullet:
	// All leaves of T are on the same level and contain
	// between leafMin and leafMax elements.
	bool full() const {
		return m_header->degree == m_params.leafMax;
	}

	bool underfull() const {
		return m_header->degree < m_params.leafMin;
	}

	bool empty() const {
		return m_header->degree == 0;
	}

	// Called by b_tree::insert
	// Pre-condition: !full()
	void insert(Value v) {
		if (full()) throw exception("Insert in full leaf");
		m_values[m_header->degree] = v;
		++m_header->degree;
	}

	// Called by b_tree::insert
	// Returns the minimum key in the right buffer.
	// Pre-condition: full()
	Key split_insert(Value v, block_buffer & rightBuf, const Compare & comp) {
		if (m_header->degree != m_params.leafMax) throw exception("Split insert in non-full leaf");

		b_tree_leaf<Traits> rightLeaf(rightBuf, m_params);

		Value * endPoint = m_values + m_params.leafMax;
		Value * insertionPoint = std::partition(m_values, endPoint, key_less_than<Traits>(comp, v));
		Value * splitPoint = m_values + m_params.leafMax/2;

		// All values in [m_values, insertionPoint) are less than v,
		// which is less than all values in [insertionPoint, endPoint).

		if (insertionPoint < splitPoint) {
			// We must insert v into left leaf.
			std::nth_element(insertionPoint, splitPoint, endPoint, key_less<Traits>(comp));
			// All values in [m_values, insertionPoint) are less than
			// all values in [insertionPoint, splitPoint) which are less than
			// all values in [splitPoint, endPoint).
			// Let [m_values, splitPoint) + v be the new left leaf,
			// and [splitPoint, endPoint) be the new right leaf.
			std::copy(splitPoint, endPoint, rightLeaf.m_values);
			m_header->degree = (splitPoint - m_values) + 1;
			rightLeaf.m_header->degree = endPoint - splitPoint;
			*splitPoint = v;
		} else if (insertionPoint > splitPoint) {
			// We must insert v into right leaf.
			std::nth_element(m_values, splitPoint, insertionPoint, key_less<Traits>(comp));
			// All values in [m_values, splitPoint) are less than
			// all values in [splitPoint, insertionPoint) which are less than
			// all values in [insertionPoint, endPoint).
			// Let [m_values, splitPoint) be the new left leaf,
			// and [splitPoint, endPoint) + v
			// be our new right leaf.
			std::swap(*splitPoint, v);
			Value * d = std::copy(splitPoint, endPoint, rightLeaf.m_values);
			*d = v;

			m_header->degree = splitPoint - m_values;
			rightLeaf.m_header->degree = (endPoint - splitPoint) + 1;
		} else { // insertionPoint == splitPoint
			// Let [m_values, splitPoint) + v be our new left leaf,
			// and [splitPoint, endPoint) be our new right leaf.
			std::copy(splitPoint, endPoint, rightLeaf.m_values);
			m_header->degree = (splitPoint - m_values) + 1;
			rightLeaf.m_header->degree = endPoint - splitPoint;
			*splitPoint = v;
		}

		// At this point, verify that all values in the left leaf
		// are less than all values in the right leaf.
		Value * rightMin = std::min_element(rightLeaf.m_values, rightLeaf.m_values + rightLeaf.degree(), key_less<Traits>(comp));
		Key rightMinKey = Traits::key_of_value(*rightMin);
#ifndef TPIE_NDEBUG
		Value * leftMax = std::max_element(m_values, m_values + degree(), key_less<Traits>(comp));
		Key leftMaxKey = Traits::key_of_value(*leftMax);
		if (comp(rightMinKey, leftMaxKey)) {
			throw exception("split_insert failed to maintain order invariant");
		}
#endif // TPIE_NDEBUG
		return rightMinKey;
	}

	// Called by b_tree::erase
	// Pre-condition: degree() > 0.
	void erase(const Key & key, const Compare & comp) {
		memory_size_type i = index_of(key, comp);
		if (i == m_header->degree) throw exception("Key not found");
		m_values[i] = m_values[m_header->degree-1];
		--m_header->degree;
	}

	// Called by b_tree_block::fuse_leaves.
	// Returns fuse_merge if the `right` leaf was merged into `this`.
	// Returns fuse_share if the two leaves shared keys with each other.
	// In the `fuse_share` case, the new lowest key in `right` is put into `midKey`.
	fuse_result fuse_with(b_tree_leaf & right, Key & midKey, const Compare & comp) {
		array<Value> values(degree() + right.degree());
		if (degree() + right.degree() <= m_params.leafMax) {
			std::copy(right.m_values,
					  right.m_values + right.degree(),
					  m_values + degree());
			m_header->degree += static_cast<uint64_t>(right.degree());
			return fuse_merge;
		} else {
			std::copy(m_values,
					  m_values + degree(),
					  values.get());
			std::copy(right.m_values,
					  right.m_values + right.degree(),
					  values.get() + degree());

			Value * midPoint = values.get() + values.size() / 2;
			std::nth_element(values.get(),
							 midPoint,
							 values.get() + values.size(),
							 key_less<Traits>(comp));

			std::copy(values.get(),
					  midPoint,
					  m_values);
			m_header->degree = midPoint - values.get();

			std::copy(midPoint,
					  values.get() + values.size(),
					  right.m_values);
			right.m_header->degree = (values.get() + values.size()) - midPoint;

			midKey = Traits::key_of_value(*midPoint);
			return fuse_share;
		}
	}

private:
	b_tree_header * m_header;
	Value * m_values;
	b_tree_parameters m_params;
};

template <typename Traits>
class b_tree_block {
	typedef typename Traits::Key Key;
	typedef typename Traits::Compare Compare;
public:
	static memory_size_type calculate_fanout(memory_size_type blockSize) {
		blockSize -= sizeof(b_tree_header);
		blockSize -= sizeof(block_handle); // one more child pointer than keys
		memory_size_type perKey = sizeof(block_handle) + sizeof(Key);
		return blockSize / perKey; // floored division
	}

	b_tree_block(block_buffer & buffer, const b_tree_parameters & params)
		: m_params(params)
	{
		char * children = buffer.get() + sizeof(b_tree_header);
		char * keys = children + params.nodeMax * sizeof(block_handle);

		m_header = reinterpret_cast<b_tree_header *>(buffer.get());
		m_children = reinterpret_cast<block_handle *>(children);
		m_keys = reinterpret_cast<Key *>(keys);
	}

	// Called by b_tree::insert after splitting the root into `left` and `right`.
	void new_root(const Key & k, const block_handle & left, const block_handle & right) {
		m_header->degree = 2;
		m_keys[0] = k;
		m_children[0] = left;
		m_children[1] = right;
	}

	void clear() {
		m_header->degree = 0;
	}

	void push_first_child(block_handle block) {
		if (!empty())
			throw exception("push_first_child: !empty");

		m_children[0] = block;
		m_header->degree = 1;
	}

	void push_child(Key k, block_handle block) {
		if (full())
			throw exception("push_child: full");

		++m_header->degree;
		m_keys[m_header->degree - 2] = k;
		m_children[m_header->degree - 1] = block;
	}

	memory_size_type degree() const {
		return static_cast<memory_size_type>(m_header->degree);
	}

	memory_size_type keys() const {
		return degree() - 1;
	}

	// Definition 1, second bullet:
	// Except for the root, all nodes have degree
	// between nodeMin and nodeMax
	// (contain between nodeMin - 1 and nodeMax - 1 elements)
	bool full() const {
		return degree() == m_params.nodeMax;
	}

	bool underfull() const {
		return degree() < m_params.nodeMin;
	}

	bool empty() const {
		return degree() == 0;
	}

	Key key(memory_size_type idx) const {
		if (idx > keys()) throw exception("Block key: Index out of bounds");
		return m_keys[idx];
	}

	block_handle child(memory_size_type idx) const {
		if (idx > degree()) throw exception("Block child: Index out of bounds");
		return m_children[idx];
	}

	// Called by b_tree::insert
	// Pre-condition: !full()
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
		++m_header->degree;
	}

	// Called by b_tree::insert
	// Pre-condition: full()
	Key split_insert(memory_size_type insertIndex,
					 Key insertKey,
					 block_handle leftChild,
					 block_handle rightChild,
					 block_buffer & leftBuf,
					 block_buffer & rightBuf)
	{
		if (!full()) throw exception("split_insert on non-full block");

		typedef key_ops<Key> O;
		typedef typename O::ptr_type KeyPtr;

		std::vector<block_handle> children(degree()+1);
		std::vector<KeyPtr> keys(this->keys() + 1);

		{
			for (memory_size_type i = 0; i < this->keys(); ++i) {
				memory_size_type dest = i;
				if (insertIndex <= i) ++dest;
				children[dest] = m_children[i];
				keys[dest] = O::get_ptr(m_keys[i]);
			}
			children[degree()] = m_children[degree()-1];

			keys[insertIndex] = O::get_ptr(insertKey);
			children[insertIndex] = leftChild;
			children[insertIndex+1] = rightChild;
		}

		Key midKey;

		{
			b_tree_block<Traits> left(leftBuf, m_params);
			b_tree_block<Traits> right(rightBuf, m_params);

			memory_size_type in = 0;
			memory_size_type out;
			for (out = 0; in*2 < keys.size(); ++out) {
				left.m_children[out] = children[in];
				left.m_keys[out] = O::get_val(keys[in]);
				++in;
			}
			left.m_children[out] = children[in];
			left.m_header->degree = static_cast<uint64_t>(out + 1);

			midKey = O::get_val(keys[in]);
			++in;

			for (out = 0; in < keys.size(); ++out) {
				right.m_children[out] = children[in];
				right.m_keys[out] = O::get_val(keys[in]);
				++in;
			}
			right.m_children[out] = children[in];
			right.m_header->degree = static_cast<uint64_t>(out + 1);
		}

		m_header->degree = 0;
		return midKey;
	}

	// Called by b_tree::erase
	// Returns fuse_merge or fuse_share.
	fuse_result fuse_leaves(memory_size_type rightIndex,
							block_buffer & leftBuf,
							block_buffer & rightBuf,
							const Compare & comp)
	{
		b_tree_leaf<Traits> left(leftBuf, m_params);
		b_tree_leaf<Traits> right(rightBuf, m_params);
		Key k;
		switch (left.fuse_with(right, k, comp)) {
			case fuse_merge:
				std::copy(m_keys + rightIndex,
						  m_keys + keys(),
						  m_keys + (rightIndex - 1));
				std::copy(m_children + (rightIndex + 1),
						  m_children + degree(),
						  m_children + rightIndex);
				--m_header->degree;
				return fuse_merge;
			case fuse_share:
				m_keys[rightIndex-1] = k;
				return fuse_share;
			default:
				throw exception("Unreachable statement");
		}
	}

	// Called by b_tree::erase
	// Returns fuse_merge or fuse_share.
	fuse_result fuse(memory_size_type rightIndex,
					 block_buffer & leftBuf,
					 block_buffer & rightBuf)
	{
		b_tree_block<Traits> left(leftBuf, m_params);
		b_tree_block<Traits> right(rightBuf, m_params);

		std::vector<Key> keys(left.keys() + 1 + right.keys());
		std::vector<block_handle> children(left.degree() + right.degree());

		{
			memory_size_type output = 0;
			for (memory_size_type i = 0; i < left.keys(); ++i) {
				keys[output] = left.key(i);
				children[output] = left.child(i);
				++output;
			}
			keys[output] = key(rightIndex-1);
			children[output] = left.child(left.keys());
			++output;
			for (memory_size_type i = 0; i < right.keys(); ++i) {
				keys[output] = right.key(i);
				children[output] = right.child(i);
				++output;
			}
			children[output] = right.child(right.keys());
			++output;
		}

		if (children.size() <= m_params.nodeMax) {
			std::copy(keys.begin(),
					  keys.end(),
					  &left.m_keys[0]);
			std::copy(children.begin(),
					  children.end(),
					  &left.m_children[0]);
			left.m_header->degree = static_cast<uint64_t>(children.size());

			std::copy(&m_keys[rightIndex],
					  &m_keys[this->keys()],
					  &m_keys[rightIndex-1]);
			std::copy(&m_children[rightIndex+1],
					  &m_children[degree()],
					  &m_children[rightIndex]);
			--m_header->degree;

			return fuse_merge;

		} else {

			memory_size_type half = children.size()/2;
			std::copy(keys.begin(),
					  keys.begin() + (half - 1),
					  &left.m_keys[0]);
			std::copy(children.begin(),
					  children.begin() + half,
					  &left.m_children[0]);
			left.m_header->degree =
				static_cast<uint64_t>(half);

			m_keys[rightIndex-1] = keys[half-1];

			std::copy(keys.begin() + half,
					  keys.end(),
					  &right.m_keys[0]);
			std::copy(children.begin() + half,
					  children.end(),
					  &right.m_children[0]);
			right.m_header->degree =
				static_cast<uint64_t>(children.size() - half);

			return fuse_share;
		}
	}

private:
	b_tree_header * m_header;
	block_handle * m_children;
	Key * m_keys;
	b_tree_parameters m_params;
};

// A path in the B tree of height h (having the root at level 0 and leaves at level h)
// is a sequence ((b_0, i_0), (b_1, i_1), ..., (b_(h-1), i_(h-1)))
// where b_0 is the block handle of the root block,
// and b_(j+1) is the i_j'th block referred to by block b_j.
// A b_tree_path supports the stack operation PUSH through the `follow` method,
// and the stack operation POP through the `parent` method.
class b_tree_path {
public:
	// Pre-condition:
	// If empty(), b refers to the root of the B tree.
	// If !empty(), b is the index'th child of block `current_block()`.
	void follow(block_handle b, memory_size_type index) {
		m_components.push_back(std::make_pair(b, index));
	}

	// Pre-condition: !empty()
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
class b_tree_builder;

template <typename Traits>
class b_tree {
	typedef typename Traits::Key Key;
	typedef typename Traits::Value Value;
	typedef typename Traits::Compare Compare;

public:
	b_tree()
		: m_root(0)
		, m_treeHeight(0)
	{
		set_default_parameters();
		open();
	}

	~b_tree() {
		close();
	}

	void set_default_parameters() {
		m_params.nodeMax = b_tree_block<Traits>::calculate_fanout(block_size());
		m_params.nodeMin = (m_params.nodeMax + 3)/4;
		m_params.leafMax = b_tree_leaf<Traits>::calculate_fanout(block_size());
		m_params.leafMin = (m_params.leafMax + 3)/4;
		verify_parameters();
		log_parameters();
	}

	b_tree_parameters get_parameters() const {
		return m_params;
	}

	void set_parameters(const b_tree_parameters & params) {
		b_tree_parameters prev = m_params;
		m_params = params;
		try {
			verify_parameters();
			log_parameters();
		} catch (const exception &) {
			m_params = prev;
			throw;
		}
	}

private:
	void verify_parameters() {
		if (m_params.nodeMin < 2)
			throw exception("Block size too small; nodeMin >= 2 violated");
		if (m_params.nodeMax < m_params.nodeMin*2-1)
			throw exception("Block size too small; nodeMax >= 2a-1 violated");
		if (m_params.leafMin < 2)
			throw exception("Block size too small; leafMin >= 2 violated");
		if (m_params.leafMax < m_params.leafMin*2-1)
			throw exception("Block size too small; leafMax >= 2a-1 violated");
	}

	void log_parameters() {
		log_debug()
			<< "B tree parameters\n"
			<< "Node degree in [" << m_params.nodeMin << ", " << m_params.nodeMax << "]\n"
			<< "Leaf degree in [" << m_params.leafMin << ", " << m_params.leafMax << "]\n"
			<< std::flush;
	}

public:
	///////////////////////////////////////////////////////////////////////////
	/// \brief  Insert value into the B tree.
	///////////////////////////////////////////////////////////////////////////
	void insert(Value v) {
		block_buffer buf;
		Key k = Traits::key_of_value(v);
		b_tree_path p = key_path(buf, k);
		block_handle leftChild;
		block_handle rightChild;

		{
			b_tree_leaf<Traits> leaf(buf, m_params);
			if (!leaf.full()) {
				leaf.insert(v);
				m_blocks.write_block(buf);
				return;
			} else {
				block_buffer & left = buf;
				block_buffer right;

				m_blocks.get_free_block(right);

				k = leaf.split_insert(v, right, m_comp);
				m_blocks.write_block(left);
				m_blocks.write_block(right);
				leftChild = left.get_handle();
				rightChild = right.get_handle();
			}
		}

		if (p.empty()) {
			// The root was previously a single leaf
			// which has now been split into two.
			m_blocks.get_free_block(buf);
			b_tree_block<Traits> block(buf, m_params);
			block.new_root(k, leftChild, rightChild);
			m_blocks.write_block(buf);
			++m_treeHeight;
			m_root = buf.get_handle();
			log_debug() << "Increase tree height to " << m_treeHeight << "; root is now " << m_root << std::endl;
			return;
		}

		b_tree_block<Traits> block(buf, m_params);

		while (!p.empty()) {
			m_blocks.read_block(p.current_block(), buf);
			if (!block.full()) break;
			// else, we split the block
			block_buffer left;
			block_buffer right;
			m_blocks.get_free_block(left);
			m_blocks.get_free_block(right);
			k = block.split_insert(p.current_index(), k, leftChild, rightChild, left, right);
			m_blocks.write_block(left);
			m_blocks.write_block(right);
			leftChild = left.get_handle();
			rightChild = right.get_handle();

			p.parent();
			m_blocks.free_block(buf);
		}

		if (p.empty()) {
			// We split the root.
			m_blocks.get_free_block(buf);
			block.new_root(k, leftChild, rightChild);
			m_blocks.write_block(buf);
			++m_treeHeight;
			m_root = buf.get_handle();
			log_debug() << "Increase tree height to " << m_treeHeight << "; root is now " << m_root << std::endl;
		} else {
			m_blocks.read_block(p.current_block(), buf);
			block.insert(p.current_index(), k, leftChild, rightChild);
			m_blocks.write_block(buf);
		}
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief  Erase value from B tree given its key.
	///////////////////////////////////////////////////////////////////////////
	void erase(Key k) {
		block_buffer buf;
		b_tree_path p = key_path(buf, k);
		{
			b_tree_leaf<Traits> leaf(buf, m_params);

			leaf.erase(k, m_comp);

			m_blocks.write_block(buf);
			if (p.empty() || !leaf.underfull()) {
				return;
			}
		}

		memory_size_type rightIndex = p.current_index() == 0 ? 1 : p.current_index();
		block_buffer left;
		block_buffer right;

		m_blocks.read_block(p.current_block(), buf);
		b_tree_block<Traits> block(buf, m_params);
		m_blocks.read_block(block.child(rightIndex-1), left);
		m_blocks.read_block(block.child(rightIndex), right);
		switch (block.fuse_leaves(rightIndex, left, right, m_comp)) {
			case fuse_share:
				//log_debug() << "fuse_leaves() == fuse_share" << std::endl;
				m_blocks.write_block(buf);
				m_blocks.write_block(left);
				m_blocks.write_block(right);
				return;
			case fuse_merge:
				//log_debug() << "fuse_leaves() == fuse_merge" << std::endl;
				m_blocks.write_block(buf);
				m_blocks.write_block(left);
				m_blocks.free_block(right);
				break;
		}

		p.parent();

		while (!p.empty() && block.underfull()) {
			memory_size_type i = p.current_index();
			m_blocks.read_block(p.current_block(), buf);
			memory_size_type rightIndex = (i == 0) ? 1 : i;
			m_blocks.read_block(block.child(rightIndex-1), left);
			m_blocks.read_block(block.child(rightIndex), right);
			switch (block.fuse(rightIndex, left, right)) {
				case fuse_share:
					//log_debug() << "Erase fuse_share of " << left.get_handle()
						//<< " and " << right.get_handle() << std::endl;
					m_blocks.write_block(buf);
					m_blocks.write_block(left);
					m_blocks.write_block(right);
					return;
				case fuse_merge:
					//log_debug() << "Erase fuse_merge of " << left.get_handle()
						//<< " and " << right.get_handle() << std::endl;
					m_blocks.write_block(buf);
					m_blocks.write_block(left);
					m_blocks.free_block(right);
					break;
			}
			p.parent();
		}
		if (p.empty() && block.degree() == 1) {
			m_root = block.child(0);
			m_blocks.free_block(buf);
			--m_treeHeight;
			log_debug() << "Decrease tree height to " << m_treeHeight << "; root is now " << m_root << std::endl;
		}
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief  Search B tree for item with given Key.
	///
	/// Returns 1 if found, and 0 if not found.
	///////////////////////////////////////////////////////////////////////////
	memory_size_type count(Key k) {
		block_buffer buf;
		b_tree_path p = key_path(buf, k);
		b_tree_leaf<Traits> leaf(buf, m_params);
		return leaf.count(k, m_comp);
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief  Search B tree for item with given Key.
	///
	/// If found, assigns the result to *out.
	///
	/// Returns true if found; false otherwise.
	///////////////////////////////////////////////////////////////////////////
	bool try_find(Key k, Value * out) {
		block_buffer buf;
		b_tree_path p = key_path(buf, k);
		b_tree_leaf<Traits> leaf(buf, m_params);
		memory_size_type i = leaf.index_of(k, m_comp);
		if (i == leaf.degree()) {
			return false;
		} else {
			*out = leaf[i];
			return true;
		}
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief  Find value associated to given key.
	///
	/// Precondition: count(k) == 1.
	///
	/// If you are not sure if the tree contains a value for the given key,
	/// use `try_find` instead.
	///////////////////////////////////////////////////////////////////////////
	Value find(Key k) {
		Value out;
		if (try_find(k, &out))
			return out;
		else
			throw exception("Value with given key not found");
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

	void read_root(block_buffer & b) {
		if (m_root == block_handle(0)) {
			m_root = m_blocks.get_free_block();
			m_treeHeight = 0;
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

		read_root(buf);

		for (memory_size_type i = 0; i < m_treeHeight; ++i) {
			b_tree_block<Traits> b(buf, m_params);

			memory_size_type i;
			for (i = 0; i != b.keys(); ++i)
				if (m_comp(k, b.key(i))) break;

			res.follow(buf.get_handle(), i);

			if (b.child(i) == block_handle(0))
				throw exception("Child pointer is 0 in non-leaf");
			else
				m_blocks.read_block(b.child(i), buf);
		}

		return res;
	}

public:
	///////////////////////////////////////////////////////////////////////////
	/// \brief  Iterate through values of the tree in-order.
	///
	/// \tparam It  An output iterator type that has Traits::Value among its
	///             value types.
	///////////////////////////////////////////////////////////////////////////
	template <typename It>
	void in_order_dump(It it) {
		if (m_root == block_handle(0)) {
			log_debug() << "in_order_dump: Empty tree" << std::endl;
			return;
		}
		in_order_dump_visit(it, m_root, m_treeHeight);
	}

private:
	template <typename It>
	void in_order_dump_visit(It it, block_handle id, memory_size_type leafDistance) {
		if (id == block_handle(0)) return;
		block_buffer buf;
		m_blocks.read_block(id, buf);
		if (leafDistance == 0) {
			b_tree_leaf<Traits> leaf(buf, m_params);
			std::vector<Value> vals;
			vals.reserve(leaf.degree());
			for (memory_size_type i = 0; i < leaf.degree(); ++i) {
				vals.push_back(leaf[i]);
			}
			std::sort(vals.begin(), vals.end(), key_less<Traits>(m_comp));
			for (memory_size_type i = 0; i < vals.size(); ++i) {
				*it = vals[i];
				++it;
			}
		} else {
			b_tree_block<Traits> block(buf, m_params);
			if (block.underfull() && id != m_root) {
				log_error() << "in_order_dump: Underfull non-root block " << id << std::endl;
			}
			for (memory_size_type i = 0; i < block.keys(); ++i) {
				in_order_dump_visit(it, block.child(i), leafDistance-1);
			}
			in_order_dump_visit(it, block.child(block.keys()), leafDistance-1);
		}
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Internal b_tree_builder helper.
	///////////////////////////////////////////////////////////////////////////
	void set_root(block_handle root, memory_size_type treeHeight) {
		m_root = root;
		m_treeHeight = treeHeight;
	}

	tpie::temp_file m_tempFile;
	block_collection m_blocks;
	block_handle m_root;
	Compare m_comp;
	memory_size_type m_treeHeight;
	b_tree_parameters m_params;

	friend class b_tree_builder<Traits>;
};

struct builder_state {
	enum type {
		/** No items have been pushed. */
		EMPTY,
		/** Items have been pushed, but end() has not been called. */
		BUILDING,
		/** end() has been called. */
		BUILT
	};
};

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

#endif // TPIE_BLOCKS_B_TREE_H