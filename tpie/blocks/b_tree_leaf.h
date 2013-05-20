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
/// \file blocks/b_tree_leaf.h  B+ tree leaf buffer view
///////////////////////////////////////////////////////////////////////////////

#ifndef TPIE_BLOCKS_B_TREE_LEAF_H
#define TPIE_BLOCKS_B_TREE_LEAF_H

namespace tpie {

namespace blocks {

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

} // namespace blocks

} // namespace tpie

#endif // TPIE_BLOCKS_B_TREE_LEAF_H
