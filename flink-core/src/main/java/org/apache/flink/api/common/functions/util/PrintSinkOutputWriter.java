

package org.apache.flink.api.common.functions.util;

import org.apache.flink.annotation.Internal;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * Print sink output writer for DataStream and DataSet print API.
 */
@Internal
public class PrintSinkOutputWriter<IN> implements Serializable {


	private static final boolean STD_OUT = false;
	private static final boolean STD_ERR = true;

	private final boolean target;
	private transient PrintStream stream;
	private final String sinkIdentifier;
	private transient String completedPrefix;

	public PrintSinkOutputWriter() {
		this("", STD_OUT);
	}

	public PrintSinkOutputWriter(final boolean stdErr) {
		this("", stdErr);
	}

	public PrintSinkOutputWriter(final String sinkIdentifier, final boolean stdErr) {
		this.target = stdErr;
		this.sinkIdentifier = (sinkIdentifier == null ? "" : sinkIdentifier);
	}

	public void open(int subtaskIndex, int numParallelSubtasks) {
		// get the target stream
		stream = target == STD_OUT ? System.out : System.err;

		completedPrefix = sinkIdentifier;

		if (numParallelSubtasks > 1) {
			if (!completedPrefix.isEmpty()) {
				completedPrefix += ":";
			}
			completedPrefix += (subtaskIndex + 1);
		}

		if (!completedPrefix.isEmpty()) {
			completedPrefix += "> ";
		}
	}

	public void write(IN record) {
		stream.println(completedPrefix + record.toString());
	}

	@Override
	public String toString() {
		return "Print to " + (target == STD_OUT ? "System.out" : "System.err");
	}
}
