// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
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

#ifndef __TPIE_PIPELINING_MERGER_H__
#define __TPIE_PIPELINING_MERGER_H__

#include <tpie/internal_priority_queue.h>
#include <tpie/file_stream.h>
#include <tpie/tpie_assert.h>

namespace tpie {

template <typename T, typename pred_t>
class merger {
public:
	inline merger(pred_t pred)
		: pq(0, predwrap(pred))
	{
	}

	inline bool can_pull() {
		return !pq.empty();
	}

	inline T pull() {
		tp_assert(can_pull(), "pull() while !can_pull()");
		T el = pq.top().first;
		size_t i = pq.top().second;
		if (in[i].can_read() && itemsRead[i] < runLength) {
			pq.pop_and_push(std::make_pair(in[i].read(), i));
			++itemsRead[i];
		} else {
			pq.pop();
		}
		if (!can_pull()) {
			reset();
		}
		return el;
	}

	inline void reset() {
		in.resize(0);
		pq.resize(0);
		itemsRead.resize(0);
	}

	// Initialize merger with given sorted input runs. Each file stream is
	// assumed to have a stream offset pointing to the first item in the run,
	// and runLength items are read from each stream (unless end of stream
	// occurs earlier).
	// Precondition: !can_pull()
	void reset(array<file_stream<T> > & inputs, stream_size_type runLength) {
		this->runLength = runLength;
		tp_assert(pq.empty(), "Reset before we are done");
		in.swap(inputs);
		pq.resize(in.size());
		for (size_t i = 0; i < in.size(); ++i) {
			pq.unsafe_push(std::make_pair(in[i].read(), i));
		}
		pq.make_safe();
		itemsRead.resize(in.size(), 1);
	}

	inline static memory_size_type memory_usage(memory_size_type fanout) {
		return sizeof(merger)
			- sizeof(internal_priority_queue<std::pair<T, size_t>, predwrap>) // pq
			+ static_cast<memory_size_type>(internal_priority_queue<std::pair<T, size_t>, predwrap>::memory_usage(fanout)) // pq
			- sizeof(array<file_stream<T> >) // in
			+ static_cast<memory_size_type>(array<file_stream<T> >::memory_usage(fanout)) // in
			- fanout*sizeof(file_stream<T>) // in file_streams
			+ fanout*file_stream<T>::memory_usage() // in file_streams
			- sizeof(array<size_t>) // itemsRead
			+ static_cast<memory_size_type>(array<size_t>::memory_usage(fanout)) // itemsRead
			;
	}

	class predwrap {
	public:
		typedef std::pair<T, size_t> item_type;
		typedef item_type first_argument_type;
		typedef item_type second_argument_type;
		typedef bool result_type;

		predwrap(pred_t pred)
			: pred(pred)
		{
		}

		inline bool operator()(const item_type & lhs, const item_type & rhs) {
			return pred(lhs.first, rhs.first);
		}

	private:
		pred_t pred;
	};

private:
	internal_priority_queue<std::pair<T, size_t>, predwrap> pq;
	array<file_stream<T> > in;
	array<stream_size_type> itemsRead;
	stream_size_type runLength;
};

} // namespace tpie

#endif // __TPIE_PIPELINING_MERGER_H__
