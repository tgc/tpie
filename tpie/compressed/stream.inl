namespace tpie {

template <typename T>
void compressed_stream<T>::write_expensive(const T & item) {
	if (m_seekState != seek_state::none) perform_seek();

	if (!use_compression()) {
		if (m_nextItem == m_bufferEnd) {
			compressor_thread_lock lock(compressor());
			if (m_bufferDirty) {
				flush_block(lock);
			}
			if (offset() == size()) {
				get_buffer(lock, m_streamBlocks);
				m_nextItem = m_bufferBegin;
			} else {
				read_next_block(lock, block_number());
			}
		}
		if (offset() == m_size) ++m_size;
		*m_nextItem++ = item;
		this->m_bufferDirty = true;
		++m_offset;
		cache_read_writes();
		return;
	}

	if (m_offset != size())
		throw stream_exception("Non-appending write attempted");

	if (m_nextItem == m_bufferEnd) {
		compressor_thread_lock l(compressor());
		if (m_bufferDirty)
			flush_block(l);
		get_buffer(l, m_streamBlocks);
		m_nextItem = m_bufferBegin;
	}

	*m_nextItem++ = item;
	this->m_bufferDirty = true;
	++m_size;
	++m_offset;

	cache_read_writes();
}

} // namespace tpie
