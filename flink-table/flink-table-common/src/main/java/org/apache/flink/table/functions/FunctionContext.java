package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

import java.io.File;
import java.util.Set;

/**
 * A {@link FunctionContext} allows to obtain global runtime information about the context in which the
 * user-defined function is executed.
 *
 * <p>The information includes the metric group, distributed cache files, and global job parameters.
 */
@PublicEvolving
public class FunctionContext {

	private RuntimeContext context;

	/**
	 * Wraps the underlying {@link RuntimeContext}.
	 *
	 * @param context the runtime context in which Flink's {@link Function} is executed.
	 */
	public FunctionContext(RuntimeContext context) {
		this.context = context;
	}

	/**
	 * Returns the metric group for this parallel subtask.
	 *
	 * @return metric group for this parallel subtask.
	 */
	public MetricGroup getMetricGroup() {
		return context.getMetricGroup();
	}

	/**
	 * Gets the local temporary file copy of a distributed cache files.
	 *
	 * @param name distributed cache file name
	 * @return local temporary file copy of a distributed cache file.
	 */
	public File getCachedFile(String name) {
		return context.getDistributedCache().getFile(name);
	}

	/**
	 * Gets the global job parameter value associated with the given key as a string.
	 *
	 * @param key          key pointing to the associated value
	 * @param defaultValue default value which is returned in case global job parameter is null
	 *                     or there is no value associated with the given key
	 * @return (default) value associated with the given key
	 */
	public String getJobParameter(String key, String defaultValue) {
		final GlobalJobParameters conf = context.getExecutionConfig().getGlobalJobParameters();
		if (conf != null && conf.toMap().containsKey(key)) {
			return conf.toMap().get(key);
		} else {
			return defaultValue;
		}
	}

	/**
	 * Get the external resource information.
	 */
	public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
		return context.getExternalResourceInfos(resourceName);
	}
}
