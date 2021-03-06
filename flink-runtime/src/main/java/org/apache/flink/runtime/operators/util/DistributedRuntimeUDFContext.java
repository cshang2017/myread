

package org.apache.flink.runtime.operators.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.broadcast.BroadcastVariableMaterialization;
import org.apache.flink.runtime.broadcast.InitializationTypeConflictException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.util.Preconditions;

/**
 * A standalone implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
public class DistributedRuntimeUDFContext extends AbstractRuntimeUDFContext {

	private final HashMap<String, BroadcastVariableMaterialization<?, ?>> broadcastVars = new HashMap<String, BroadcastVariableMaterialization<?, ?>>();

	private final ExternalResourceInfoProvider externalResourceInfoProvider;

	public DistributedRuntimeUDFContext(TaskInfo taskInfo, ClassLoader userCodeClassLoader, ExecutionConfig executionConfig,
										Map<String, Future<Path>> cpTasks, Map<String, Accumulator<?, ?>> accumulators,
										MetricGroup metrics, ExternalResourceInfoProvider externalResourceInfoProvider) {
		super(taskInfo, userCodeClassLoader, executionConfig, accumulators, cpTasks, metrics);
		this.externalResourceInfoProvider = Preconditions.checkNotNull(externalResourceInfoProvider);
	}

	@Override
	public boolean hasBroadcastVariable(String name) {
		return this.broadcastVars.containsKey(name);
	}

	@Override
	public <T> List<T> getBroadcastVariable(String name) {
		Preconditions.checkNotNull(name, "The broadcast variable name must not be null.");
		
		// check if we have an initialized version
		@SuppressWarnings("unchecked")
		BroadcastVariableMaterialization<T, ?> variable = (BroadcastVariableMaterialization<T, ?>) this.broadcastVars.get(name);
		if (variable != null) {
			try {
				return variable.getVariable();
			}
			catch (InitializationTypeConflictException e) {
				throw new RuntimeException("The broadcast variable '" + name + "' has been initialized by a prior call to a " + e.getType());
			}
		}
		else {
			throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
		}
	}
	
	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		Preconditions.checkNotNull(name, "The broadcast variable name must not be null.");
		Preconditions.checkNotNull(initializer, "The broadcast variable initializer must not be null.");

		// check if we have an initialized version
		@SuppressWarnings("unchecked")
		BroadcastVariableMaterialization<T, C> variable = (BroadcastVariableMaterialization<T, C>) this.broadcastVars.get(name);
		if (variable != null) {
			return variable.getVariable(initializer);
		}
		else {
			throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
		}
	}

	@Override
	public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
		return externalResourceInfoProvider.getExternalResourceInfos(resourceName);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setBroadcastVariable(String name, BroadcastVariableMaterialization<?, ?> value) {
		this.broadcastVars.put(name, value);
	}
	
	public void clearBroadcastVariable(String name) {
		this.broadcastVars.remove(name);
	}
	
	public void clearAllBroadcastVariables() {
		this.broadcastVars.clear();
	}
}
