package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.Internal;

/**
 * Internal timestamp kind for attaching time attribute metadata to timestamps with or without a
 * time zone.
 */
@Internal
public enum TimestampKind {

	REGULAR,

	ROWTIME,

	PROCTIME
}
