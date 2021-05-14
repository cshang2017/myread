package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/**
 * A container class hosting the information of a {@link SourceReader}.
 */
@Getter
@PublicEvolving
public final class ReaderInfo implements Serializable {

	private final int subtaskId;
	private final String location;

	public ReaderInfo(int subtaskId, String location) {
		this.subtaskId = subtaskId;
		this.location = location;
	}

}
