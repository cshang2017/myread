package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.util.ArrayList;

/**
 * This accumulator stores a collection of objects.
 *
 * @param <T> The type of the accumulated objects
 */
@Public
public class ListAccumulator<T> implements Accumulator<T, ArrayList<T>> {

	private ArrayList<T> localValue = new ArrayList<T>();
	
	@Override
	public void add(T value) {
		localValue.add(value);
	}

	@Override
	public ArrayList<T> getLocalValue() {
		return localValue;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<T>> other) {
		localValue.addAll(other.getLocalValue());
	}

	@Override
	public Accumulator<T, ArrayList<T>> clone() {
		ListAccumulator<T> newInstance = new ListAccumulator<T>();
		newInstance.localValue = new ArrayList<T>(localValue);
		return newInstance;
	}

	@Override
	public String toString() {
		return "List Accumulator " + localValue;
	}
}
