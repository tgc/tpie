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

///////////////////////////////////////////////////////////////////////////////
/// \file graph.h  Traverse pipeline graphs
///////////////////////////////////////////////////////////////////////////////

#ifndef __TPIE_PIPELINING_GRAPH_H__
#define __TPIE_PIPELINING_GRAPH_H__

#include <tpie/disjoint_sets.h>
#include <tpie/dummy_progress.h>
#include <tpie/tpie_assert.h>
#include <vector>
#include <stack>
#include <tpie/pipelining/tokens.h>

namespace tpie {

namespace pipelining {

struct pipe_segment;

class phase {
public:
	typedef pipe_segment * val_t;

	struct segment_graph;

	phase();
	phase(const phase &);
	phase & operator=(const phase &);
	~phase();

	inline void set_initiator(val_t s) {
		tp_assert(m_initiator == 0, "Initiator set twice");
		m_initiator = s;
	}

	bool is_initiator(val_t s);

	void add(segment_base * s);

	void add_successor(val_t from, val_t to);

	void add_data_structure(data_structure * ds);

	inline size_t count(val_t s) {
		for (size_t i = 0; i < m_segments.size(); ++i) {
			if (m_segments[i] == s) return 1;
		}
		return 0;
	}

	void go(progress_indicator_base & pi);

	void evacuate_all() const;

	inline double memory_fraction() const {
		return m_memoryFraction;
	}

	inline memory_size_type minimum_memory() const {
		return m_minimumMemory;
	}

	void assign_memory(memory_size_type m) const;

	const std::string & get_name() const;

	std::string get_unique_id() const;

private:
	std::auto_ptr<segment_graph> g;

	/** a pointer is a weak reference to something that isn't reference counted. */
	std::vector<val_t> m_segments;

	std::vector<data_structure *> m_dataStructures;

	double m_memoryFraction;
	memory_size_type m_minimumMemory;
	val_t m_initiator;

	void assign_minimum_memory() const;
};

struct segment_map;

struct graph_traits {
	typedef std::vector<phase> phases_t;
	typedef phases_t::iterator phaseit;
	typedef progress_types<true> Progress;
	typedef segment_base * val_t;

	graph_traits(const segment_map & map);

	double sum_memory() {
		double sum = 0.0;
		for (size_t i = 0; i < m_phases.size(); ++i) {
			sum += m_phases[i].memory_fraction();
		}
		return sum;
	}

	const phases_t & phases() {
		return m_phases;
	}

	void go_all(stream_size_type n, Progress::base & pi);

private:
	const segment_map & map;
	phases_t m_phases;
	std::vector<bool> m_evacuatePrevious;

	void calc_phases();

};

} // namespace pipelining

} // namespace tpie

#endif // __TPIE_PIPELINING_GRAPH_H__
