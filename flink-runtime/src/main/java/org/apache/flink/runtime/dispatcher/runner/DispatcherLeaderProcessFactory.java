
package org.apache.flink.runtime.dispatcher.runner;

import java.util.UUID;

/**
 * Factory for the {@link DispatcherLeaderProcess}.
 */
public interface DispatcherLeaderProcessFactory {

	DispatcherLeaderProcess create(UUID leaderSessionID);
}
