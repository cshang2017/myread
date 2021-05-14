package org.apache.flink.table.runtime.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple iterator over the input read though an I/O channel.
 * Use {@link BinaryRowDataSerializer#deserializeFromPages}.
 */
public class BinaryRowChannelInputViewIterator implements MutableObjectIterator<BinaryRowData> {
	private final ChannelReaderInputView inView;

	private final BinaryRowDataSerializer serializer;

	private final List<MemorySegment> freeMemTarget;

	public BinaryRowChannelInputViewIterator(
			ChannelReaderInputView inView,
			BinaryRowDataSerializer serializer) {
		this(inView, new ArrayList<>(), serializer);
	}

	public BinaryRowChannelInputViewIterator(
			ChannelReaderInputView inView,
			List<MemorySegment> freeMemTarget,
			BinaryRowDataSerializer serializer) {
		this.inView = inView;
		this.freeMemTarget = freeMemTarget;
		this.serializer = serializer;
	}

	@Override
	public BinaryRowData next(BinaryRowData reuse) throws IOException {
		try {
			return this.serializer.deserializeFromPages(reuse, this.inView);
		} catch (EOFException eofex) {
			final List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return null;
		}
	}

	@Override
	public BinaryRowData next() throws IOException {
		throw new UnsupportedOperationException("This method is disabled due to performance issue!");
	}
}
