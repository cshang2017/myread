package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This accumulator stores a collection of objects in serialized form, so that the stored objects
 * are not affected by modifications to the original objects.
 *
 * Objects may be deserialized on demand with a specific classloader.
 *
 * @param <T> The type of the accumulated objects
 */
@PublicEvolving
public class SerializedListAccumulator<T> implements Accumulator<T, ArrayList<byte[]>> {

	private ArrayList<byte[]> localValue = new ArrayList<>();


	@Override
	public void add(T value) {
		throw new UnsupportedOperationException();
	}

	public void add(T value, TypeSerializer<T> serializer) throws IOException {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(outStream);
			serializer.serialize(value, out);
			localValue.add(outStream.toByteArray());
		
	}

	@Override
	public ArrayList<byte[]> getLocalValue() {
		return localValue;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<byte[]>> other) {
		localValue.addAll(other.getLocalValue());
	}

	@Override
	public SerializedListAccumulator<T> clone() {
		SerializedListAccumulator<T> newInstance = new SerializedListAccumulator<T>();
		newInstance.localValue = new ArrayList<byte[]>(localValue);
		return newInstance;
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> deserializeList(ArrayList<byte[]> data, TypeSerializer<T> serializer)
			throws IOException, ClassNotFoundException
	{
		List<T> result = new ArrayList<T>(data.size());
		for (byte[] bytes : data) {
			ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
			DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(inStream);
			T val = serializer.deserialize(in);
			result.add(val);
		}
		return result;
	}

	@Override
	public String toString() {
		return "SerializedListAccumulator: " + localValue.size() + " elements";
	}
}
