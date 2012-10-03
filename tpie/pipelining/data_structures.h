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
/// \file data_structures.h  Data structures for pipelining
///////////////////////////////////////////////////////////////////////////////

#ifndef __TPIE_PIPELINING_DATA_STRUCTURES_H__
#define __TPIE_PIPELINING_DATA_STRUCTURES_H__

#include <tpie/priority_queue.h>
#include <tpie/pipelining/data_structure.h>

namespace tpie {

namespace pipelining {

template <typename T>
class push_priority_queue_type : public pipe_segment {
	data_structure m_ds;
	priority_queue<T> * m_pq;
public:
	typedef T item_type;

	inline push_priority_queue_type(segment_token pushToken, segment_token dataStructureToken)
		: pipe_segment(pushToken)
		, m_ds(dataStructureToken)
	{
		add_data_structure(m_ds);
		set_name("Push PQ", PRIORITY_INSIGNIFICANT);
		m_ds.set_name("Priority queue");
	}

	virtual void begin() /*override*/ {
		log_debug() << "Memory for PQ: " << m_ds.get_available_memory() << " b" << std::endl;
		m_pq = tpie_new<priority_queue<T> >(m_ds.get_available_memory());
		m_ds.set<priority_queue<T> >(m_pq);
	}

	inline void push(const T & item) {
		m_pq->push(item);
	}
};

template <typename T>
class pop_priority_queue_type {
public:
	template <typename dest_t>
	class type : public pipe_segment {
		dest_t dest;
		data_structure * m_ds;
		priority_queue<T> * m_pq;
	public:
		typedef T item_type;

		inline type(dest_t dest, segment_token pushToken, segment_token dataStructureToken)
			: dest(dest)
		{
			add_push_destination(dest);
			add_dependency(pushToken);
			m_ds = get_data_structure(dataStructureToken);
			add_data_structure(*m_ds);
			set_name("Pop PQ", PRIORITY_INSIGNIFICANT);
		}

		virtual void begin() /*override*/ {
			m_pq = m_ds->get<priority_queue<T> >();
			set_steps(m_pq->size());
		}

		virtual void go() /*override*/ {
			while (!m_pq->empty()) {
				dest.push(m_pq->top());
				m_pq->pop();
				step();
			}
		}

		virtual void end() /*override*/ {
			tpie_delete(m_pq);
		}
	};
};

template <typename T>
class priority_queue_push_pull {
	segment_token m_pushToken;
	segment_token m_ds;

public:
	pipe_end<termfactory_2<push_priority_queue_type<T>, segment_token, segment_token> >
	inline pusher() {
		return termfactory_2<push_priority_queue_type<T>, segment_token, segment_token>
			(m_pushToken, m_ds);
	}

	pipe_begin<tempfactory_2<pop_priority_queue_type<T>, segment_token, segment_token> >
	inline puller() {
		return tempfactory_2<pop_priority_queue_type<T>, segment_token, segment_token>
			(m_pushToken, m_ds);
	}
};

} // namespace pipelining

} // namespace tpie

#endif // __TPIE_PIPELINING_DATA_STRUCTURES_H__
