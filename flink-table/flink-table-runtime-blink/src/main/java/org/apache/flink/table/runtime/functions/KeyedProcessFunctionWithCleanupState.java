package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.io.IOException;

/**
 * A function that processes elements of a stream, and could cleanup state.
 * @param <K> Type of the key.
 * @param <IN>  Type of the input elements.
 * @param <OUT> Type of the output elements.
 */
public abstract class KeyedProcessFunctionWithCleanupState<K, IN, OUT>
	extends KeyedProcessFunction<K, IN, OUT> implements CleanupState {

	private final long minRetentionTime;
	private final long maxRetentionTime;
	protected final boolean stateCleaningEnabled;

	// holds the latest registered cleanup timer
	private ValueState<Long> cleanupTimeState;

	public KeyedProcessFunctionWithCleanupState(long minRetentionTime, long maxRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.maxRetentionTime = maxRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
	}

	protected void initCleanupTimeState(String stateName) {
		if (stateCleaningEnabled) {
			ValueStateDescriptor<Long> inputCntDescriptor = new ValueStateDescriptor<>(stateName, Types.LONG);
			cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
		}
	}

	protected void registerProcessingCleanupTimer(Context ctx, long currentTime) throws Exception {
		if (stateCleaningEnabled) {
			registerProcessingCleanupTimer(
				cleanupTimeState,
				currentTime,
				minRetentionTime,
				maxRetentionTime,
				ctx.timerService()
			);
		}
	}

	protected boolean isProcessingTimeTimer(OnTimerContext ctx) {
		return ctx.timeDomain() == TimeDomain.PROCESSING_TIME;
	}

	protected void cleanupState(State... states) {
		for (State state : states) {
			state.clear();
		}
		this.cleanupTimeState.clear();
	}

	protected Boolean needToCleanupState(Long timestamp) throws IOException {
		if (stateCleaningEnabled) {
			Long cleanupTime = cleanupTimeState.value();
			// check that the triggered timer is the last registered processing time timer.
			return timestamp.equals(cleanupTime);
		} else {
			return false;
		}
	}
}
