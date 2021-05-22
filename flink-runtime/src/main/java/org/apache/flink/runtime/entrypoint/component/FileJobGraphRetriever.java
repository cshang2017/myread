
package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JobGraphRetriever} implementation which retrieves the {@link JobGraph} from
 * a file on disk.
 */
public class FileJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {

	public static final ConfigOption<String> JOB_GRAPH_FILE_PATH = ConfigOptions
		.key("internal.jobgraph-path")
		.defaultValue("job.graph");

	@Nonnull
	private final String jobGraphFile;

	public FileJobGraphRetriever(@Nonnull String jobGraphFile, @Nullable File usrLibDir) throws IOException {
		super(usrLibDir);
		this.jobGraphFile = jobGraphFile;
	}

	@Override
	public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
		final File fp = new File(jobGraphFile);

		FileInputStream input = new FileInputStream(fp);
			ObjectInputStream obInput = new ObjectInputStream(input);
			final JobGraph jobGraph = (JobGraph) obInput.readObject();
			addUserClassPathsToJobGraph(jobGraph);
			return jobGraph;
	}

	private void addUserClassPathsToJobGraph(JobGraph jobGraph) {
		final List<URL> classPaths = new ArrayList<>();

		if (jobGraph.getClasspaths() != null) {
			classPaths.addAll(jobGraph.getClasspaths());
		}
		classPaths.addAll(getUserClassPaths());
		jobGraph.setClasspaths(classPaths);
	}

	public static FileJobGraphRetriever createFrom(Configuration configuration, @Nullable File usrLibDir) throws IOException {
		checkNotNull(configuration, "configuration");
		return new FileJobGraphRetriever(configuration.getString(JOB_GRAPH_FILE_PATH), usrLibDir);
	}
}
