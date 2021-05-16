package org.apache.flink.runtime.event;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

/**
 * This type of event can be used to exchange notification messages between
 * different {@link TaskExecutor} objects at runtime using the communication
 * channels.
 */
public abstract class AbstractEvent implements IOReadableWritable {}
