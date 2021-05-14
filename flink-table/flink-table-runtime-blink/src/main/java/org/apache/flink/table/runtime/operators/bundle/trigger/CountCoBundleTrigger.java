

package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.apache.flink.util.Preconditions;

/**
 * A {@link CoBundleTrigger} that fires once the count of elements in a bundle reaches the given
 * count.
 */
public class CountCoBundleTrigger<IN1, IN2> implements CoBundleTrigger<IN1, IN2> {

	private final long maxCount;
	private transient BundleTriggerCallback callback;
	private transient long count = 0;

	public CountCoBundleTrigger(long maxCount) {
		Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
		this.maxCount = maxCount;
	}

	@Override
	public void registerCallback(BundleTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement1(final IN1 element) throws Exception {
		count++;
		if (count >= maxCount) {
			callback.finishBundle();
			reset();
		}
	}

	@Override
	public void onElement2(final IN2 element) throws Exception {
		count++;
		if (count >= maxCount) {
			callback.finishBundle();
			reset();
		}
	}

	@Override
	public void reset() {
		count = 0;
	}

	@Override
	public String explain() {
		return "CountCoBundleTrigger with size " + maxCount;
	}
}

