package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * An internal special {@link TableSink} to collect the select query result to local client.
 */
@Internal
public interface SelectTableSink extends TableSink<Row> {

	default TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		// disable this to make sure there is only one SelectTableSink instance
		// so that the instance can be shared within optimizing and executing in client
		throw new UnsupportedOperationException();
	}

	/**
	 * Set the job client associated with the select job to retrieve the result.
	 */
	void setJobClient(JobClient jobClient);

	/**
	 * Returns the select result as row iterator.
	 */
	CloseableIterator<Row> getResultIterator();
}
