package org.apache.flink.runtime.executiongraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

class AllVerticesIterator implements Iterator<ExecutionVertex> {

	private final Iterator<ExecutionJobVertex> jobVertices;
	
	private ExecutionVertex[] currVertices;
	
	private int currPos;
	
	
	public AllVerticesIterator(Iterator<ExecutionJobVertex> jobVertices) {
		this.jobVertices = jobVertices;
	}
	
	
	@Override
	public boolean hasNext() {
		while (true) {
			if (currVertices != null) {
				if (currPos < currVertices.length) {
					return true;
				} else {
					currVertices = null;
				}
			}
			else if (jobVertices.hasNext()) {
				currVertices = jobVertices.next().getTaskVertices();
				currPos = 0;
			}
			else {
				return false;
			}
		}
	}
	
	@Override
	public ExecutionVertex next() {
		if (hasNext()) {
			return currVertices[currPos++];
		} else {
			throw new NoSuchElementException();
		}
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
