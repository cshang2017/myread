package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An intermediate data set is the data set produced by an operator - either a
 * source or any intermediate operation.
 * 
 * Intermediate data sets may be read by other operators, materialized, or
 * discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {
	
	private final IntermediateDataSetID id; 		// the identifier
	
	private final JobVertex producer;			// the operation that produced this data set
	
	private final List<JobEdge> consumers = new ArrayList<JobEdge>();

	// The type of partition to use at runtime
	private final ResultPartitionType resultType;
	
	// --------------------------------------------------------------------------------------------

	public IntermediateDataSet(IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
		this.id = checkNotNull(id);
		this.producer = checkNotNull(producer);
		this.resultType = checkNotNull(resultType);
	}

	// --------------------------------------------------------------------------------------------
	
	public IntermediateDataSetID getId() {
		return id;
	}

	public JobVertex getProducer() {
		return producer;
	}
	
	public List<JobEdge> getConsumers() {
		return this.consumers;
	}

	public ResultPartitionType getResultType() {
		return resultType;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void addConsumer(JobEdge edge) {
		this.consumers.add(edge);
	}

}
