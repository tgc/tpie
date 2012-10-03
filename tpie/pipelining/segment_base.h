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

#ifndef __TPIE_PIPELINING_SEGMENT_BASE_H__
#define __TPIE_PIPELINING_SEGMENT_BASE_H__

#include <tpie/pipelining/tokens.h>

namespace tpie {

namespace pipelining {

class segment_base {
public:
	///////////////////////////////////////////////////////////////////////////
	/// \brief Virtual dtor.
	///////////////////////////////////////////////////////////////////////////
	virtual ~segment_base() {}

	inline memory_size_type get_minimum_memory() const {
		return m_minimumMemory;
	}

	inline memory_size_type get_available_memory() const {
		return m_availableMemory;
	}

	inline void set_memory_fraction(double f) {
		m_memoryFraction = f;
	}

	inline double get_memory_fraction() const {
		return m_memoryFraction;
	}

	inline segment_map::ptr get_segment_map() const {
		return token.get_map();
	}

	inline segment_token::id_t get_id() const {
		return token.id();
	}

	inline priority_type get_name_priority() {
		return m_namePriority;
	}

	inline const std::string & get_name() {
		return m_name;
	}

	inline void set_name(const std::string & name, priority_type priority = PRIORITY_USER) {
		m_name = name;
		m_namePriority = priority;
	}

	inline void set_breadcrumb(const std::string & breadcrumb) {
		m_name = m_name.empty() ? breadcrumb : (breadcrumb + " | " + m_name);
	}

	inline pipe_segment * cast_pipe_segment() {
		return m_selfPipeSegment;
	}

	inline pipe_segment * assert_pipe_segment() {
		if (0 == m_selfPipeSegment) throw not_pipe_segment();
		return m_selfPipeSegment;
	}

	inline data_structure * cast_data_structure() {
		return m_selfDataStructure;
	}

	inline data_structure * assert_data_structure() {
		if (0 == m_selfDataStructure) throw not_data_structure();
		return m_selfDataStructure;
	}

	inline void set_available_memory_at_most(memory_size_type availableMemory) {
		if (m_availableMemory > 0 && availableMemory > m_availableMemory) return;
		this->set_available_memory(availableMemory);
	}

	virtual void set_available_memory(memory_size_type availableMemory) {
		m_availableMemory = availableMemory;
	}

	// Called on data structures by graph.cpp phase::finalize_data_structure_memory
	inline void finalize_memory() {
		m_minimumMemory = m_availableMemory;
		m_memoryFraction = 0.0;
	}

protected:
	inline segment_base()
		: token(this)
		, m_minimumMemory(0)
		, m_availableMemory(0)
		, m_memoryFraction(1.0)
		, m_name()
		, m_namePriority(PRIORITY_NO_NAME)
		, m_selfPipeSegment(0)
		, m_selfDataStructure(0)
	{
	}

	inline segment_base(const segment_base & other)
		: token(other.token, this)
		, m_minimumMemory(other.m_minimumMemory)
		, m_availableMemory(other.m_availableMemory)
		, m_memoryFraction(other.m_memoryFraction)
		, m_name(other.m_name)
		, m_namePriority(other.m_namePriority)
		, m_selfPipeSegment(0)
		, m_selfDataStructure(0)
	{
	}

	inline segment_base(const segment_token & token)
		: token(token, this, true)
		, m_minimumMemory(0)
		, m_availableMemory(0)
		, m_memoryFraction(1.0)
		, m_name()
		, m_namePriority(PRIORITY_NO_NAME)
		, m_selfPipeSegment(0)
		, m_selfDataStructure(0)
	{
	}

	inline void set_minimum_memory(memory_size_type minimumMemory) {
		m_minimumMemory = minimumMemory;
	}

private:
	friend struct pipe_segment;
	friend class data_structure;

	segment_token token;

	memory_size_type m_minimumMemory;
	memory_size_type m_availableMemory;
	double m_memoryFraction;

	std::string m_name;
	priority_type m_namePriority;

protected:
	pipe_segment * m_selfPipeSegment;
	data_structure * m_selfDataStructure;
};

} // namespace pipelining

} // namespace tpie

#endif // __TPIE_PIPELINING_SEGMENT_BASE_H__
