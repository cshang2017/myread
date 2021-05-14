package org.apache.flink.table.runtime.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.io.CompressedBlockChannelReader;
import org.apache.flink.table.runtime.io.CompressedBlockChannelWriter;
import org.apache.flink.table.runtime.io.CompressedHeaderlessChannelReaderInputView;
import org.apache.flink.table.runtime.io.CompressedHeaderlessChannelWriterOutputView;
import org.apache.flink.table.runtime.io.HeaderlessChannelWriterOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;

/**
 * File channel util for runtime.
 */
public class FileChannelUtil {

	public static AbstractChannelReaderInputView createInputView(
			IOManager ioManager,
			ChannelWithMeta channel,
			List<FileIOChannel> channels,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize,
			int segmentSize) throws IOException {
		if (compressionEnable) {
			CompressedHeaderlessChannelReaderInputView in =
					new CompressedHeaderlessChannelReaderInputView(
							channel.getChannel(),
							ioManager,
							compressionCodecFactory,
							compressionBlockSize,
							channel.getBlockCount()
					);
			channels.add(in.getReader());
			return in;
		} else {
			BlockChannelReader<MemorySegment> reader =
					ioManager.createBlockChannelReader(channel.getChannel());
			channels.add(reader);
			return new HeaderlessChannelReaderInputView(
					reader,
					Arrays.asList(
							allocateUnpooledSegment(segmentSize),
							allocateUnpooledSegment(segmentSize)
					),
					channel.getBlockCount(),
					channel.getNumBytesInLastBlock(), false
			);
		}
	}

	public static AbstractChannelWriterOutputView createOutputView(
			IOManager ioManager,
			FileIOChannel.ID channel,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize,
			int segmentSize) throws IOException {
		if (compressionEnable) {
			BufferFileWriter bufferWriter = ioManager.createBufferFileWriter(channel);
			return new CompressedHeaderlessChannelWriterOutputView(
					bufferWriter,
					compressionCodecFactory,
					compressionBlockSize);
		} else {
			BlockChannelWriter<MemorySegment> blockWriter =
					ioManager.createBlockChannelWriter(channel);
			return new HeaderlessChannelWriterOutputView(
					blockWriter,
					Arrays.asList(
							allocateUnpooledSegment(segmentSize),
							allocateUnpooledSegment(segmentSize)
					),
					segmentSize
			);
		}
	}

	public static BlockChannelWriter<MemorySegment> createBlockChannelWriter(
			IOManager ioManager,
			FileIOChannel.ID channel,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize,
			int segmentSize) throws IOException {
		if (compressionEnable) {
			return new CompressedBlockChannelWriter(
					ioManager,
					channel,
					bufferReturnQueue,
					compressionCodecFactory,
					compressionBlockSize,
					segmentSize
			);
		} else {
			return ioManager.createBlockChannelWriter(channel, bufferReturnQueue);
		}
	}

	public static BlockChannelReader<MemorySegment> createBlockChannelReader(
			IOManager ioManager,
			FileIOChannel.ID channel,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize,
			int segmentSize) throws IOException {
		if (compressionEnable) {
			return new CompressedBlockChannelReader(
					ioManager,
					channel,
					bufferReturnQueue,
					compressionCodecFactory,
					compressionBlockSize,
					segmentSize
			);
		} else {
			return ioManager.createBlockChannelReader(channel, bufferReturnQueue);
		}
	}
}