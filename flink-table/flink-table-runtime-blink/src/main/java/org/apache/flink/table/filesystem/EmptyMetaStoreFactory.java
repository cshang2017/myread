package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Empty implementation {@link TableMetaStoreFactory}.
 */
@Internal
public class EmptyMetaStoreFactory implements TableMetaStoreFactory {

	private final Path path;

	public EmptyMetaStoreFactory(Path path) {
		this.path = path;
	}

	@Override
	public TableMetaStore createTableMetaStore() {
		return new TableMetaStore() {

			@Override
			public Path getLocationPath() {
				return path;
			}

			@Override
			public Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) {
				return Optional.empty();
			}

			@Override
			public void createOrAlterPartition(
					LinkedHashMap<String, String> partitionSpec,
					Path partitionPath) {
			}

			@Override
			public void close() {

			}
		};
	}
}
