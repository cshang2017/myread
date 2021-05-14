package org.apache.flink.table.runtime.io;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;

/**
 * Channel with block count and numBytesInLastBlock of file.
 */
@Getter
public class ChannelWithMeta {

	private final FileIOChannel.ID channel;
	private final int blockCount;
	private final int numBytesInLastBlock;

	public ChannelWithMeta(FileIOChannel.ID channel, int blockCount, int numBytesInLastBlock) {
		this.channel = channel;
		this.blockCount = blockCount;
		this.numBytesInLastBlock = numBytesInLastBlock;
	}
}
