package org.apache.flink.table.runtime.partitioner;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.util.MathUtils;

import java.util.Arrays;

/**
 * Hash partitioner for {@link BinaryRowData}.
 */
public class BinaryHashPartitioner extends StreamPartitioner<RowData> {

	private GeneratedHashFunction genHashFunc;

	private transient HashFunction hashFunc;
	private String[] hashFieldNames;

	public BinaryHashPartitioner(GeneratedHashFunction genHashFunc, String[] hashFieldNames) {
		this.genHashFunc = genHashFunc;
		this.hashFieldNames = hashFieldNames;
	}

	@Override
	public StreamPartitioner<RowData> copy() {
		return this;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
		return MathUtils.murmurHash(
				getHashFunc().hashCode(record.getInstance().getValue())) % numberOfChannels;
	}

	private HashFunction getHashFunc() {
		if (hashFunc == null) {
				hashFunc = genHashFunc.newInstance(Thread.currentThread().getContextClassLoader());
				genHashFunc = null;
			
		}
		return hashFunc;
	}

	@Override
	public String toString() {
		return "HASH" + Arrays.toString(hashFieldNames);
	}
}
