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

#ifndef TPIE_BLOCKS_BLOCK_COLLECTION_H
#define TPIE_BLOCKS_BLOCK_COLLECTION_H

#include <tpie/array.h>
#include <tpie/file_accessor/file_accessor.h>
#include <tpie/tpie_log.h>

namespace tpie {

namespace blocks {

///////////////////////////////////////////////////////////////////////////////
/// \brief  Pointer to a block in a block collection on disk.
///
/// Essentially a strong typedef of a stream_size_type.
/// Block allocation is administered by the block collection framework, so it
/// does not make sense to perform integer arithmetic on the block handles.
///
/// The block_handle 0 is never assigned to user code, as it is reserved for
/// the root allocation bitmap.
/// As such, block_handle(0) may be used as a sentinel in user code, similar to
/// the well-known NULL pointer from C.
///////////////////////////////////////////////////////////////////////////////
class block_handle {
public:
	explicit block_handle(stream_size_type id)
		: m_id(id)
	{
	}

	block_handle()
		: m_id(std::numeric_limits<stream_size_type>::max())
	{
	}

	stream_size_type id() const {
		return m_id;
	}

	operator stream_size_type() const {
		return id();
	}

	bool operator==(stream_size_type i) const {
		return m_id == i;
	}

private:
	stream_size_type m_id;
};

///////////////////////////////////////////////////////////////////////////////
/// \brief  Buffer corresponding to a single block on disk.
///
/// Passed to read_block, write_block, get_free_block and others in the
/// block_collection class.
///
/// Access the underlying byte buffer through get() which returns a pointer to
/// the raw buffer.
///
/// Access the byte size of the buffer through size().
/// To free the memory allocated for this buffer, call resize(0).
///
/// Get or set the block handle associated to this buffer with get_handle and
/// set_handle.
///////////////////////////////////////////////////////////////////////////////
class block_buffer {
public:
	block_buffer() {
	}

	block_buffer(memory_size_type size)
		: m_buffer(size, 0)
	{
	}

	void resize(memory_size_type size) {
		m_buffer.resize(size, 0);
	}

	memory_size_type size() const {
		return m_buffer.size();
	}

	char * get() {
		return m_buffer.get();
	}

	const char * get() const {
		return m_buffer.get();
	}

	void set_handle(block_handle handle) {
		m_handle = handle;
	}

	block_handle get_handle() const {
		return m_handle;
	}

private:
	array<char> m_buffer;
	block_handle m_handle;
};

class free_space_block {
public:
	block_buffer & get_buffer() { return m_buffer; }

	void resize(memory_size_type blockSize) {
		m_buffer.resize(blockSize);
		m_bufferStart = reinterpret_cast<size_t *>(m_buffer.get());
		m_bufferNext = m_bufferStart;
		m_bufferEnd = reinterpret_cast<size_t *>(m_buffer.get()
												 + m_buffer.size());
		std::fill(m_bufferStart, m_bufferEnd, 0);
	}

	void initial() {
		m_bufferStart[0] = 0x1;
		m_buffer.set_handle(block_handle(0));
	}

	block_handle get_free_block() {
		while (m_bufferNext != m_bufferEnd && ~*m_bufferNext == 0)
			++m_bufferNext;
		if (m_bufferNext == m_bufferEnd) throw exception("Out of blocks");
		stream_size_type id = (m_bufferNext - m_bufferStart) * sizeof(size_t) * 8;
		size_t i = 1;
		while (i != 0 && (*m_bufferNext & i) == i) {
			i = i << 1;
			++id;
		}
		if (i == 0) throw exception("A bit trick went wrong");
		*m_bufferNext |= i;
		return block_handle(id);
	}

	void free_block(block_handle handle) {
		memory_size_type id = static_cast<memory_size_type>(handle);
		memory_size_type hi = id / (sizeof(size_t) * 8);
		memory_size_type lo = id % (sizeof(size_t) * 8);
		size_t * b = m_bufferStart + hi;
		*b &= ~(static_cast<size_t>(1) << lo);
		m_bufferNext = std::min(m_bufferNext, b);
	}

private:
	block_buffer m_buffer;
	size_t * m_bufferStart;
	size_t * m_bufferNext;
	size_t * m_bufferEnd;
};

class block_collection {
public:
	static memory_size_type default_block_size() {
		return 16*1024;
	}

	block_collection()
		: m_open(false)
		, m_write(false)
		, m_blockSize(default_block_size())
	{
	}

	~block_collection() {
		close();
	}

	bool is_open() const {
		return m_open;
	}

	void close() {
		if (m_open) {
			write_allocation_bitmap();
			m_accessor.close();
			m_open = false;
			m_freeSpace.resize(0);
		}
	}

	void open(std::string fileName, bool writable) {
		close();

		m_accessor.open(fileName,
						true,
						writable,
						1,
						block_size(),
						max_user_data_size(),
						access_random);

		m_write = writable;

		m_freeSpace.resize(block_size());

		if (m_accessor.size() == 0)
			initial_allocation_bitmap();
		else
			read_allocation_bitmap();

		m_open = true;
		// TODO exception safety
	}

private:
	void initial_allocation_bitmap() {
		m_freeSpace.initial();
		write_allocation_bitmap();
	}

	void read_allocation_bitmap() {
		read_block(block_handle(0), m_freeSpace.get_buffer());
	}

	void write_allocation_bitmap() {
		write_block(m_freeSpace.get_buffer());
	}

public:
	///////////////////////////////////////////////////////////////////////////
	/// \brief  Get a free block.
	///
	/// This will never return block 0, as that handle is reserved for the
	/// implementation.
	///////////////////////////////////////////////////////////////////////////
	block_handle get_free_block() {
		if (!m_write) throw exception("get_free_block: !m_write");
		return m_freeSpace.get_free_block();
	}

	void get_free_block(block_buffer & buf) {
		buf.set_handle(get_free_block());
		buf.resize(block_size());
	}

	void free_block(block_handle handle) {
		m_freeSpace.free_block(handle);
	}

	void free_block(block_buffer & buf) {
		free_block(buf.get_handle());
	}

	void read_block(block_handle id, block_buffer & buf) {
		buf.set_handle(id);
		buf.resize(block_size());
		m_accessor.read_block(buf.get(), id, block_size());
	}

	void write_block(block_buffer & buf) {
		m_accessor.write_block(buf.get(), buf.get_handle(), block_size());
	}

private:
	memory_size_type max_user_data_size() const {
		return 0;
	}

	memory_size_type block_size() const {
		return m_blockSize;
	}

	tpie::file_accessor::file_accessor m_accessor;

	bool m_open;
	bool m_write;

	memory_size_type m_blockSize;

	free_space_block m_freeSpace;

};

} // namespace blocks

} // namespace tpie

#endif // TPIE_BLOCKS_BLOCK_COLLECTION_H
