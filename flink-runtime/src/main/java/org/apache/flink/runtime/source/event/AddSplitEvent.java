package org.apache.flink.runtime.source.event;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A source event that adds splits to a source reader.
 *
 * @param <SplitT> the type of splits.
 */
public class AddSplitEvent<SplitT> implements OperatorEvent {

	private static final long serialVersionUID = 1L;
	private final int serializerVersion;
	private final ArrayList<byte[]> splits;

	public AddSplitEvent(List<SplitT> splits, SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
		this.splits = new ArrayList<>(splits.size());
		this.serializerVersion = splitSerializer.getVersion();
		for (SplitT split : splits) {
			this.splits.add(splitSerializer.serialize(split));
		}
	}

	public List<SplitT> splits(SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
		List<SplitT> result = new ArrayList<>(splits.size());
		for (byte[] serializedSplit : splits) {
			result.add(splitSerializer.deserialize(serializerVersion, serializedSplit));
		}
		return result;
	}

}
