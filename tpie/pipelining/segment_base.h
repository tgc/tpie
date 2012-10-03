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

///////////////////////////////////////////////////////////////////////////////
/// \file segment_base.h Node in the pipeline graph
///////////////////////////////////////////////////////////////////////////////

#include <tpie/pipelining/tokens.h>

namespace tpie {

namespace pipelining {

class segment_base {
public:
	///////////////////////////////////////////////////////////////////////////
	/// \brief Virtual dtor.
	///////////////////////////////////////////////////////////////////////////
	virtual ~segment_base() {}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Get minimum memory needed by this object.
	///////////////////////////////////////////////////////////////////////////
	inline memory_size_type get_minimum_memory() const {
		return m_minimumMemory;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief After memory assignment, get the memory actually assigned.
	///////////////////////////////////////////////////////////////////////////
	inline memory_size_type get_available_memory() const {
		return m_availableMemory;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Before memory assignment, set the memory fraction of this
	/// object.
	///////////////////////////////////////////////////////////////////////////
	inline void set_memory_fraction(double f) {
		m_memoryFraction = f;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Get the memory fraction of this object.
	///////////////////////////////////////////////////////////////////////////
	inline double get_memory_fraction() const {
		return m_memoryFraction;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Get the map mapping token IDs to segment_base pointers.
	///////////////////////////////////////////////////////////////////////////
	inline segment_map::ptr get_segment_map() const {
		return token.get_map();
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Get the ID of the aggregated token.
	///////////////////////////////////////////////////////////////////////////
	inline segment_token::id_t get_id() const {
		return token.id();
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief In phase naming, get the priority of this node's name.
	///////////////////////////////////////////////////////////////////////////
	inline priority_type get_name_priority() {
		return m_namePriority;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Get the name of this node, or the empty string if not defined.
	///////////////////////////////////////////////////////////////////////////
	inline const std::string & get_name() {
		return m_name;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Set the name of this node.
	/// \param name      New name, or empty to unset.
	/// \param priority  The priority of this node's name.
	///////////////////////////////////////////////////////////////////////////
	inline void set_name(const std::string & name, priority_type priority = PRIORITY_USER) {
		m_name = name;
		m_namePriority = priority;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Set the breadcrumb of this node, that is a string to prefix this
	/// node's name in progress indication.
	///////////////////////////////////////////////////////////////////////////
	inline void set_breadcrumb(const std::string & breadcrumb) {
		m_name = m_name.empty() ? breadcrumb : (breadcrumb + " | " + m_name);
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Cast this object to a pipe_segment.
	/// \returns A pipe_segment pointer if this is a pipe_segment, otherwise
	/// null.
	///////////////////////////////////////////////////////////////////////////
	inline pipe_segment * cast_pipe_segment() {
		return m_selfPipeSegment;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Cast this object to a pipe_segment.
	/// \throws not_pipe_segment if this is not a pipe_segment.
	/// \returns A pipe_segment pointer.
	///////////////////////////////////////////////////////////////////////////
	inline pipe_segment * assert_pipe_segment() {
		if (0 == m_selfPipeSegment) throw not_pipe_segment();
		return m_selfPipeSegment;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Cast this object to a data_structure.
	/// \returns A data_structure pointer if this is a data_structure,
	/// otherwise null.
	///////////////////////////////////////////////////////////////////////////
	inline data_structure * cast_data_structure() {
		return m_selfDataStructure;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Cast this object to a data_structure.
	/// \throws not_data_structure if this is not a data_structure.
	/// \returns A data_structure pointer.
	///////////////////////////////////////////////////////////////////////////
	inline data_structure * assert_data_structure() {
		if (0 == m_selfDataStructure) throw not_data_structure();
		return m_selfDataStructure;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief During memory assignment, restrict this node's memory.
	/// If the node has previously had less memory assigned, does nothing.
	/// Otherwise, sets the available memory to the given amount.
	/// \param availableMemory Available memory (in bytes).
	///////////////////////////////////////////////////////////////////////////
	inline void set_available_memory_at_most(memory_size_type availableMemory) {
		if (m_availableMemory > 0 && availableMemory > m_availableMemory) return;
		this->set_available_memory(availableMemory);
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief During memory assignment, set this node's available memory.
	/// May be overridden as long as the base set_available_memory is called.
	/// May be called more than once by the framework.
	/// \param availableMemory Available memory (in bytes).
	///////////////////////////////////////////////////////////////////////////
	virtual void set_available_memory(memory_size_type availableMemory) {
		m_availableMemory = availableMemory;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief During memory assignment, make the current memory assignment
	/// final.
	/// Sets the node's minimum memory to the currently assigned memory amount
	/// and sets the node's memory fraction to zero.
	/// Called on data structures by phase::finalize_data_structure_memory.
	///////////////////////////////////////////////////////////////////////////
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
	/** If this is a pipe_segment, a pointer to this, otherwise a null pointer. */
	pipe_segment * m_selfPipeSegment;

	/** If this is a data_structure, a pointer to this, otherwise a null pointer. */
	data_structure * m_selfDataStructure;
};

} // namespace pipelining

} // namespace tpie

#endif // __TPIE_PIPELINING_SEGMENT_BASE_H__
