package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.util.Map;
import java.util.TreeMap;

/**
 * Histogram accumulator, which builds a histogram in a distributed manner.
 * Implemented as a Integer-&gt;Integer TreeMap, so that the entries are sorted
 * according to the values.
 * 
 * This class does not extend to continuous values later, because it makes no
 * attempt to put the data in bins.
 */
@Public
public class Histogram implements Accumulator<Integer, TreeMap<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	private TreeMap<Integer, Integer> treeMap = new TreeMap<Integer, Integer>();

	@Override
	public void add(Integer value) {
		Integer current = treeMap.get(value);
		Integer newValue = (current != null ? current : 0) + 1;
		this.treeMap.put(value, newValue);
	}

	@Override
	public TreeMap<Integer, Integer> getLocalValue() {
		return this.treeMap;
	}

	@Override
	public void merge(Accumulator<Integer, TreeMap<Integer, Integer>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Integer> entryFromOther : other.getLocalValue().entrySet()) {
			Integer ownValue = this.treeMap.get(entryFromOther.getKey());
			if (ownValue == null) {
				this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
			} else {
				this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
			}
		}
	}

	@Override
	public void resetLocal() {
		this.treeMap.clear();
	}

	@Override
	public String toString() {
		return this.treeMap.toString();
	}

	@Override
	public Accumulator<Integer, TreeMap<Integer, Integer>> clone() {
		Histogram result = new Histogram();
		result.treeMap = new TreeMap<Integer, Integer>(treeMap);
		return result;
	}
}
