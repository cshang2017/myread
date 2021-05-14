package org.apache.flink.runtime.taskexecutor;

import java.io.IOException;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * This interface gives access to transient, named, global aggregates.  This can be used to share
 * state amongst parallel tasks in a job.  It is not designed for high throughput updates
 * and the aggregates do NOT survive a job failure.  Each call to the updateGlobalAggregate()
 * method results in serialized RPC communication with the JobMaster so use with care.
 */
public interface GlobalAggregateManager {
	/**
	 * Update the global aggregate and return the new value.
	 *
	 * @param aggregateName The name of the aggregate to update
	 * @param aggregand The value to add to the aggregate
	 * @param aggregateFunction The function to apply to the current aggregate and aggregand to
	 * obtain the new aggregate value
	 * @return The updated aggregate
	 */
	<IN, ACC, OUT> OUT updateGlobalAggregate(String aggregateName, Object aggregand, AggregateFunction<IN, ACC, OUT> aggregateFunction) throws IOException;
}
