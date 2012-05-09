// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2009, 2011, The TPIE development team
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
#ifndef __TPIE_FILE_STREAM_H__
#define __TPIE_FILE_STREAM_H__
#include <tpie/tempname.h>
#include <tpie/file.h>
#include <tpie/memory.h>
#include <tpie/array.h>
///////////////////////////////////////////////////////////////////////////////
/// \file file_stream.h
/// \brief Simple class acting both as a tpie::file and a
/// tpie::file::stream.
///////////////////////////////////////////////////////////////////////////////

namespace tpie {

///////////////////////////////////////////////////////////////////////////////
/// \brief Simple class acting both as \ref file and a file::stream.
///
/// A file_stream basically supports every operation a \ref file or a
/// file::stream supports. This is used to access a file I/O-efficiently, and
/// is the direct replacement of the old ami::stream.
///
/// \tparam T The type of items stored in the stream.
///////////////////////////////////////////////////////////////////////////////
template <typename T>
class file_stream {
public:
	/** The type of the items stored in the stream */
	typedef T item_type;

	/////////////////////////////////////////////////////////////////////////
	/// \brief Construct a new file_stream.
	/// 
	/// \copydetails tpie::file::file(double blockFactor, file_accessor::file_accessor * fileAccessor)
	/////////////////////////////////////////////////////////////////////////
	inline file_stream(double blockFactor=1.0, 
					   file_accessor::file_accessor * fileAccessor=NULL)
		throw()
		// : m_file(blockFactor, fileAccessor), m_stream(m_file, 0)
		{

		m_size = 0;
		m_itemSize = sizeof(item_type);
		m_open = false;
		if (fileAccessor == 0)
			fileAccessor = new default_file_accessor();
		m_fileAccessor = fileAccessor;

		m_blockSize = block_size(blockFactor);
		m_blockItems = m_blockSize/m_itemSize;

		m_blockStartIndex = 0;
		m_nextBlock = std::numeric_limits<stream_size_type>::max();
		m_nextIndex = std::numeric_limits<memory_size_type>::max();
		m_index = std::numeric_limits<memory_size_type>::max();

		m_tempFile = 0;
	};

	///////////////////////////////////////////////////////////////////////////
	/// Destroy a file_stream.
	///////////////////////////////////////////////////////////////////////////
	inline ~file_stream() {
		close();
	}


	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::open
	/// \sa file_base::open
	/////////////////////////////////////////////////////////////////////////
	inline void open(const std::string & path,
					 file_base::access_type accessType=file_base::read_write,
					 memory_size_type userDataSize=0) throw (stream_exception) {
		close();
		open_inner(path, accessType, userDataSize);
	}

	/////////////////////////////////////////////////////////////////////////
	///
	/////////////////////////////////////////////////////////////////////////
	inline void open(memory_size_type userDataSize=0) throw (stream_exception) {
		close();
		m_ownedTempFile.reset(new temp_file());
		m_tempFile=m_ownedTempFile.get();
		open_inner(m_tempFile->path(), file_base::read_write, userDataSize);
	}

	inline void open(temp_file & file, 
					 file_base::access_type accessType=file_base::read_write,
					 memory_size_type userDataSize=0) throw (stream_exception) {
		close();
		m_tempFile=&file;
		open_inner(m_tempFile->path(), accessType, userDataSize);
	}


	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::read_user_data
	/// \sa file_base::read_user_data
	/////////////////////////////////////////////////////////////////////////
	template <typename TT>
	void read_user_data(TT & data) throw (stream_exception) {
		assert(m_open);
		if (sizeof(TT) != m_fileAccessor->user_data_size()) throw io_exception("Wrong user data size");
		m_fileAccessor->read_user_data(reinterpret_cast<void*>(&data));
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::write_user_data
	/// \sa file_base::write_user_data
	/////////////////////////////////////////////////////////////////////////
	template <typename TT>
	void write_user_data(const TT & data) {
		assert(m_open);
		if (sizeof(TT) != m_fileAccessor->user_data_size()) throw io_exception("Wrong user data size");
		m_fileAccessor->write_user_data(reinterpret_cast<const void*>(&data));
	}

	/////////////////////////////////////////////////////////////////////////
	/// \brief Close the file and release resources.
	///
	/// This will close the file and resources used by buffers and such.
	/////////////////////////////////////////////////////////////////////////
	inline void close() throw(stream_exception) {
		if (m_open) {
			flush_block();
			m_fileAccessor->close();
		}
		m_open = false;
		m_ownedTempFile.reset();
		m_tempFile=NULL;
		m_block.data.resize(0);
	}
	
	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file<T>::stream::write(const item_type & item)
	/// \sa file<T>::stream::write(const item_type & item)
	/////////////////////////////////////////////////////////////////////////
	inline void write(const item_type & item) throw(stream_exception) {
		assert(m_open);
#ifndef NDEBUG
		if (!is_writable())
			throw io_exception("Cannot write to read only stream");
#endif
		if (m_index >= m_blockItems) update_block();
		m_block.data[m_index++] = item;
		write_update();
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file<T>::stream::write(const IT & start, const IT & end)
	/// \sa file<T>::stream::write(const IT & start, const IT & end)
	/////////////////////////////////////////////////////////////////////////
	template <typename IT>
	inline void write(const IT & start, const IT & end) throw(stream_exception) {
		assert(m_open);
		IT i = start;
		while (i != end) {
			if (m_index >= m_blockItems) update_block();

			IT blockmax = i + (m_blockItems-m_index);

			T * dest = m_block.data.get() + m_index;

			IT till = std::min(end, blockmax);

			std::copy(i, till, dest);

			m_index += till - i;
			write_update();
			i = till;
		}
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file<T>::stream::read()
	/// \sa file<T>::stream::read()
	/////////////////////////////////////////////////////////////////////////
	inline const item_type & read() throw(stream_exception) {
		assert(m_open);
		if (m_index >= m_block.size) {
			update_block();
			if (offset() >= size()) {
				throw end_of_stream_exception();
			}
		}
		return m_block.data[m_index++];
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file<T>::stream::read(const IT & start, const IT & end)
	/// \sa file<T>::stream::read(const IT & start, const IT & end)
	/////////////////////////////////////////////////////////////////////////
	template <typename IT>
	inline void read(const IT & start, const IT & end) throw(stream_exception) {
		assert(m_open);
		IT i = start;
		while (i != end) {
			if (m_index >= m_blockItems) {
				// check to make sure we have enough items in the stream
				stream_size_type offs = offset();
				if (offs >= size()
					|| offs + (end-i) >= size()) {

					throw end_of_stream_exception();
				}

				// fetch next block from disk
				update_block();
			}

			T * src = m_block.data.get() + m_index;

			// either read the rest of the block or until `end'
			memory_size_type count = std::min(m_blockItems-m_index, static_cast<memory_size_type>(end-i));

			std::copy(src, src + count, i);

			// advance output iterator
			i += count;

			// advance input position
			m_index += count;
		}
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file<T>::stream::read_back()
	/// \sa file<T>::stream::read_back()
	/////////////////////////////////////////////////////////////////////////
	inline const item_type & read_back() throw(stream_exception) {
		assert(m_open);
		seek(-1, file_base::current);
		const item_type & i = read();
		seek(-1, file_base::current);
		return i;
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::stream::offset()
	/// \sa file_base::stream::offset()
	/////////////////////////////////////////////////////////////////////////
	inline stream_size_type offset() const throw() {
		assert(m_open);
		if (m_nextBlock == std::numeric_limits<stream_size_type>::max())
			return m_index + m_blockStartIndex;
		return m_nextIndex + m_nextBlock * m_blockItems;
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::path()
	/// \sa file_base::path()
	/////////////////////////////////////////////////////////////////////////
	inline const std::string & path() const throw() {
		assert(m_open);
		return m_fileAccessor->path();
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::size()
	/// \sa file_base::size()
	/////////////////////////////////////////////////////////////////////////
	inline stream_size_type size() const throw() {
		// XXX update_vars changes internal state in a way that is not visible
		// through the class interface.
		// therefore, a const_cast is warranted.
		const_cast<file_stream<T>*>(this)->update_vars();
		return m_size;
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::stream::can_read()
	/// \sa file_base::stream::can_read()
	/////////////////////////////////////////////////////////////////////////
	inline bool can_read() const throw() {
		assert(m_open);
		if (m_index < m_block.size) return true;
		return offset() < m_size;
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::stream::can_read_back()
	/// \sa file_base::stream::can_read_back()
	/////////////////////////////////////////////////////////////////////////
	inline bool can_read_back() const throw() {
		assert(m_open);
		const_cast<file_stream<T>*>(this)->update_vars();
		if (m_index > 0) return true;
		return m_block.number != 0;
	}

	/////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::stream::seek()
	/// \sa file_base::stream::seek()
	/////////////////////////////////////////////////////////////////////////
	inline void seek(stream_offset_type offset, 
					 file_base::offset_type whence=file_base::beginning) 
		throw (stream_exception) {

		assert(m_open);
		if (whence == file_base::end)
			offset += size();
		else if (whence == file_base::current) {
			// are we seeking into the current block?
			if (offset >= 0 || static_cast<stream_size_type>(-offset) <= m_index) {
				stream_size_type new_index = static_cast<stream_offset_type>(offset+m_index);

				if (new_index < m_blockItems) {
					update_vars();
					m_index = new_index;
					return;
				}
			}

			offset += this->offset();
		}
		if (0 > offset || (stream_size_type)offset > size())
			throw io_exception("Tried to seek out of file");
		update_vars();
		stream_size_type b = static_cast<stream_size_type>(offset) / m_blockItems;
		m_index = static_cast<memory_size_type>(offset - b*m_blockItems);
		if (b == m_block.number) {
			m_nextBlock = std::numeric_limits<stream_size_type>::max();
			m_nextIndex = std::numeric_limits<memory_size_type>::max();
			assert(this->offset() == (stream_size_type)offset);
			return;
		}
		m_nextBlock = b;
		m_nextIndex = m_index;
		m_index = std::numeric_limits<memory_size_type>::max();
		assert(this->offset() == (stream_size_type)offset);
	}

	///////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base::truncate()
	/// \sa file_base::truncate()
	///
	/// Note that when using a file_stream the stream will automatically be
	/// rewound if it is beyond the new end of the file. 
	///////////////////////////////////////////////////////////////////////////
	inline void truncate(stream_size_type size) {
		stream_size_type o=offset();
		//TODO flush current block here
		m_size = size;
		m_fileAccessor->truncate(size);
		seek(std::min(o, size));
	}

	///////////////////////////////////////////////////////////////////////////
	/// Check if we can read from the file.
	///
	/// \returns True if we can read from the file.
	///////////////////////////////////////////////////////////////////////////
	bool is_readable() const throw() {
		return m_canRead;
	}

	///////////////////////////////////////////////////////////////////////////
	/// Check if we can write to the file
	///
	/// \returns True if we can write to the file.
	///////////////////////////////////////////////////////////////////////////
	bool is_writable() const throw() {
		return m_canWrite;
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Calculate the amount of memory used by a single file_stream.
	///
	/// \param blockFactor The block factor you pass to open.
	/// \param includeDefaultFileAccessor Unless you are supplying your own
	/// file accessor to open, leave this to be true.
	/// \returns The amount of memory maximally used by the count file_streams.
	///////////////////////////////////////////////////////////////////////////
	inline static memory_size_type memory_usage(
		float blockFactor=1.0,
		bool includeDefaultFileAccessor=true) throw() {
		// TODO
		memory_size_type x = sizeof(file_stream);
		x += block_memory_usage(blockFactor); // allocated in constructor
		if (includeDefaultFileAccessor)
			x += default_file_accessor::memory_usage();
		return x;
	}

	void swap(file_stream<T> & other) {
		using std::swap;
		swap(m_index,           other.m_index);
		swap(m_nextBlock,       other.m_nextBlock);
		swap(m_nextIndex,       other.m_nextIndex);
		swap(m_blockStartIndex, other.m_blockStartIndex);
		swap(m_blockItems,      other.m_blockItems);
		swap(m_blockSize,       other.m_blockSize);
		swap(m_size,            other.m_size);
		swap(m_canRead,         other.m_canRead);
		swap(m_canWrite,        other.m_canWrite);
		swap(m_itemSize,        other.m_itemSize);
		swap(m_open,            other.m_open);
		swap(m_fileAccessor,    other.m_fileAccessor);
		swap(m_block.size,      other.m_block.size);
		swap(m_block.number,    other.m_block.number);
		swap(m_block.dirty,     other.m_block.dirty);
		swap(m_block.data,      other.m_block.data);
		swap(m_ownedTempFile,   other.m_ownedTempFile);
		swap(m_tempFile,        other.m_tempFile);
	}

private:
	struct block_t {
		memory_size_type size;
		stream_size_type number;
		bool dirty;
		array<T> data;
	};

	memory_size_type m_index;
	stream_size_type m_nextBlock;
	memory_size_type m_nextIndex;
	stream_size_type m_blockStartIndex;
	memory_size_type m_blockItems;
	memory_size_type m_blockSize;
	stream_size_type m_size;
	bool m_canRead;
	bool m_canWrite;
	memory_size_type m_itemSize;
	bool m_open;
	file_accessor::file_accessor * m_fileAccessor;
	block_t m_block;
	std::auto_ptr<temp_file> m_ownedTempFile;
	temp_file * m_tempFile;

	inline void open_inner(const std::string & path,
						   file_base::access_type accessType,
						   memory_size_type user_data_size) throw (stream_exception) {
		m_canRead = accessType == file_base::read || accessType == file_base::read_write;
		m_canWrite = accessType == file_base::write || accessType == file_base::read_write;
		m_fileAccessor->open(path, m_canRead, m_canWrite, m_itemSize, m_blockSize, user_data_size);
		m_size = m_fileAccessor->size();
		m_open = true;
		
		m_blockStartIndex = 0;
		m_nextBlock = std::numeric_limits<stream_size_type>::max();
		m_nextIndex = std::numeric_limits<memory_size_type>::max();
		m_index = std::numeric_limits<memory_size_type>::max();

		m_block.size = 0;
		m_block.number = std::numeric_limits<stream_size_type>::max();
		m_block.dirty = false;
		m_block.data.resize(m_blockItems);
		
		initialize();
		seek(0);
	}


	///////////////////////////////////////////////////////////////////////////
	/// \brief Use file_accessor to fetch indicated block number into m_block.
	///////////////////////////////////////////////////////////////////////////
	inline void get_block(stream_size_type block) {
		// If the file contains n full blocks (numbered 0 through n-1), we may
		// request any block in {0, 1, ... n}

		// If the file contains n-1 full blocks and a single non-full block, we may
		// request any block in {0, 1, ... n-1}

		// We capture this restraint with the check:
		if (block * static_cast<stream_size_type>(m_blockItems) > size()) {
			throw end_of_stream_exception();
		}

		m_block.dirty = false;
		m_block.number = block;

		// calculate buffer size
		m_block.size = m_blockItems;
		if (static_cast<stream_size_type>(m_block.size) + m_block.number * static_cast<stream_size_type>(m_blockItems) > size())
			m_block.size = size() - m_block.number * m_blockItems;

		// populate buffer data
		if (m_block.size > 0 &&
			m_fileAccessor->read_block(m_block.data.get(), m_block.number, m_block.size) != m_block.size) {
			throw io_exception("Incorrect number of items read");
		}
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Fetch block from disk as indicated by m_nextBlock, writing old
	/// block to disk if needed.
	///////////////////////////////////////////////////////////////////////////
	inline void update_block() {
		if (m_nextBlock == std::numeric_limits<stream_size_type>::max()) {
			m_nextBlock = m_block.number+1;
			m_nextIndex = 0;
		}
		flush_block();
		get_block(m_nextBlock);
		m_blockStartIndex = m_nextBlock*static_cast<stream_size_type>(m_blockItems);
		m_index = m_nextIndex;
		m_nextBlock = std::numeric_limits<stream_size_type>::max();
		m_nextIndex = std::numeric_limits<memory_size_type>::max();
	}

	///////////////////////////////////////////////////////////////////////////
	/// \brief Write block to disk.
	///////////////////////////////////////////////////////////////////////////
	inline void flush_block() {
		if (m_block.dirty) {
			assert(m_canWrite);
			update_vars();
			m_fileAccessor->write_block(m_block.data.get(), m_block.number, m_block.size);
		}
	}

	inline void update_vars() {
		if (m_block.dirty && m_index != std::numeric_limits<memory_size_type>::max()) {
			assert(m_index <= m_blockItems);
			m_block.size = std::max(m_block.size, m_index);
			m_size = std::max(m_size, static_cast<stream_size_type>(m_index)+m_blockStartIndex);
			if (m_tempFile) 
				m_tempFile->update_recorded_size(m_fileAccessor->byte_size());
		}
	}

	inline void initialize() {
		flush_block();
		m_nextBlock = std::numeric_limits<stream_size_type>::max();
		m_nextIndex = std::numeric_limits<memory_size_type>::max();
		m_index = std::numeric_limits<memory_size_type>::max();
	}

	inline void write_update() {
		m_block.dirty = true;
	}

	////////////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base<T>::block_size(double)
	////////////////////////////////////////////////////////////////////////////////
	static inline memory_size_type block_size(double blockFactor) throw () {
		return static_cast<memory_size_type>(2 * 1024*1024 * blockFactor);
	}

	////////////////////////////////////////////////////////////////////////////////
	/// \copydoc file_base<T>::calculate_block_factor(memory_size_type)
	////////////////////////////////////////////////////////////////////////////////
	static inline double calculate_block_factor(memory_size_type blockSize) throw () {
		return (double)blockSize / (double)block_size(1.0);
	}

	static inline memory_size_type block_memory_usage(double blockFactor) {
		return block_size(blockFactor);
	}

	inline memory_size_type block_memory_usage() {
		return m_blockItems * m_itemSize;
	}
};

} // namespace tpie

namespace std {

///////////////////////////////////////////////////////////////////////////////
/// \brief Enable std::swapping two tpie::file_streams.
///////////////////////////////////////////////////////////////////////////////
template <typename T>
void swap(tpie::file_stream<T> & a, tpie::file_stream<T> & b) {
	a.swap(b);
}

} // namespace std

#endif //__TPIE_FILE_STREAM_H__