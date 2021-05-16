package org.apache.flink.runtime.event;

/**
 * Subclasses of this event are recognized as custom events that are not part of the core
 * flink runtime.
 */
public abstract class TaskEvent extends AbstractEvent {}
