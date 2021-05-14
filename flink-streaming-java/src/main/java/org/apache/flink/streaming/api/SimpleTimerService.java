package org.apache.flink.streaming.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;

/**
 * Implementation of {@link TimerService} that uses a {@link InternalTimerService}.
 */
@Internal
public class SimpleTimerService implements TimerService {

	private final InternalTimerService<VoidNamespace> internalTimerService;

	public SimpleTimerService(InternalTimerService<VoidNamespace> internalTimerService) {
		this.internalTimerService = internalTimerService;
	}

	@Override
	public long currentProcessingTime() {
		return internalTimerService.currentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return internalTimerService.currentWatermark();
	}

	@Override
	public void registerProcessingTimeTimer(long time) {
		internalTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void registerEventTimeTimer(long time) {
		internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void deleteProcessingTimeTimer(long time) {
		internalTimerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void deleteEventTimeTimer(long time) {
		internalTimerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, time);
	}
}
