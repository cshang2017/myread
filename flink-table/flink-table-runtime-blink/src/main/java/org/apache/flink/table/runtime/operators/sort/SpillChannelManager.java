package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Channel manager to manage the life cycle of spill channels.
 */
public class SpillChannelManager implements Closeable {

	private final HashSet<FileIOChannel.ID> channels;
	private final HashSet<FileIOChannel> openChannels;

	private volatile boolean closed;

	public SpillChannelManager() {
		this.channels = new HashSet<>(64);
		this.openChannels = new HashSet<>(64);
	}

	/**
	 * Add a new File channel.
	 */
	public synchronized void addChannel(FileIOChannel.ID id) {
		checkArgument(!closed);
		channels.add(id);
	}

	/**
	 * Open File channels.
	 */
	public synchronized void addOpenChannels(List<FileIOChannel> toOpen) {
		checkArgument(!closed);
		for (FileIOChannel channel : toOpen) {
			openChannels.add(channel);
			channels.remove(channel.getChannelID());
		}
	}

	public synchronized void removeChannel(FileIOChannel.ID id) {
		checkArgument(!closed);
		channels.remove(id);
	}

	@Override
	public synchronized void close() {

		if (this.closed) {
			return;
		}

		this.closed = true;

		for (Iterator<FileIOChannel> channels = this.openChannels.iterator(); channels.hasNext(); ) {
			final FileIOChannel channel = channels.next();
			channels.remove();
			try {
				channel.closeAndDelete();
			} catch (Throwable ignored) {
			}
		}

		for (Iterator<FileIOChannel.ID> channels = this.channels.iterator(); channels.hasNext(); ) {
			final FileIOChannel.ID channel = channels.next();
			channels.remove();
			try {
				final File f = new File(channel.getPath());
				if (f.exists()) {
					f.delete();
				}
			} catch (Throwable ignored) {
			}
		}
	}

}
