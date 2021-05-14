package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The default implementation of the {@link PipelineExecutorServiceLoader}. This implementation uses
 * Java service discovery to find the available {@link PipelineExecutorFactory executor factories}.
 */
@Internal
public class DefaultExecutorServiceLoader implements PipelineExecutorServiceLoader {


	// The reason of this duplication is the package structure which does not allow for the ExecutorServiceLoader
	// to know about the ClusterClientServiceLoader. Remove duplication when package structure has improved.

	@Override
	public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
		checkNotNull(configuration);

		final ServiceLoader<PipelineExecutorFactory> loader =
				ServiceLoader.load(PipelineExecutorFactory.class);

		final List<PipelineExecutorFactory> compatibleFactories = new ArrayList<>();
		final Iterator<PipelineExecutorFactory> factories = loader.iterator();
		while (factories.hasNext()) {
			final PipelineExecutorFactory factory = factories.next();
			if (factory != null && factory.isCompatibleWith(configuration)) {
				compatibleFactories.add(factory);
			}
		}

		if (compatibleFactories.size() > 1) {
			final String configStr =
					configuration.toMap().entrySet().stream()
							.map(e -> e.getKey() + "=" + e.getValue())
							.collect(Collectors.joining("\n"));

			throw new IllegalStateException("Multiple compatible client factories found for:\n" + configStr + ".");
		}

		return compatibleFactories.get(0);
	}

	@Override
	public Stream<String> getExecutorNames() {
		final ServiceLoader<PipelineExecutorFactory> loader =
				ServiceLoader.load(PipelineExecutorFactory.class);

		return StreamSupport.stream(loader.spliterator(), false)
				.map(PipelineExecutorFactory::getName);
	}
}
