package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * A {@link CoBundleTrigger} is similar with {@link BundleTrigger}, and the only differences is
 * {@link CoBundleTrigger} can handle two inputs.
 *
 * @param <IN1> The first input element type.
 * @param <IN2> The second input element type.
 */
@Internal
public interface CoBundleTrigger<IN1, IN2> extends Serializable {

	/**
	 * Register a callback which will be called once this trigger decides to finish this bundle.
	 */
	void registerCallback(BundleTriggerCallback callback);

	/**
	 * Called for every element that gets added to the bundle from the first input. If the trigger
	 * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
	 *
	 * @param element The element that arrived from the first input.
	 */
	void onElement1(final IN1 element) throws Exception;

	/**
	 * Called for every element that gets added to the bundle from the second input. If the trigger
	 * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
	 *
	 * @param element The element that arrived from the second input.
	 */
	void onElement2(final IN2 element) throws Exception;

	/**
	 * Reset the trigger to its initiate status.
	 */
	void reset();

	String explain();
}
