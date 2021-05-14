package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * This is used together with {@link FileMonitoringFunction} to read from files that the
 * monitoring functions discovers.
 *
 * @deprecated Internal class deprecated in favour of {@link ContinuousFileMonitoringFunction}.
 */
@Internal
@Deprecated
public class FileReadFunction implements FlatMapFunction<Tuple3<String, Long, Long>, String> {

	@Override
	public void flatMap(Tuple3<String, Long, Long> value, Collector<String> out) throws Exception {
		FSDataInputStream stream = FileSystem.get(new URI(value.f0)).open(new Path(value.f0));
		stream.seek(value.f1);

		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		String line;

		try {
			while ((line = reader.readLine()) != null && (value.f2 == -1L || stream.getPos() <= value.f2)) {
				out.collect(line);
			}
		} finally {
			reader.close();
		}
	}
}
