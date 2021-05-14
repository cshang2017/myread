package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;

/**
 * An operator that needs access to the {@link MailboxExecutor} to yield to downstream operators needs to be created
 * through a factory implementing this interface.
 */
@Experimental
public interface YieldingOperatorFactory<OUT> extends StreamOperatorFactory<OUT> {
	void setMailboxExecutor(MailboxExecutor mailboxExecutor);
}
