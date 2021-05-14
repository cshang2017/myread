package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.apache.flink.annotation.Internal;

/**
 * Interface for bundle trigger callbacks that can be registered to a {@link BundleTrigger}.
 */
@Internal
public interface BundleTriggerCallback {

	/**
	 * This method is invoked to finish current bundle and start a new one when the trigger was fired.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 * to fail and may trigger recovery.
	 */
	void finishBundle() throws Exception;
}
