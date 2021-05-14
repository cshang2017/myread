package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_DEFAULT_NAME;
import static org.apache.flink.table.filesystem.FileSystemOptions.PATH;

/**
 * File system {@link TableFactory}.
 *
 * <p>1.The partition information should be in the file system path, whether it's a temporary
 * table or a catalog table.
 * 2.Support insert into (append) and insert overwrite.
 * 3.Support static and dynamic partition inserting.
 *
 * <p>Migrate to new source/sink interface after FLIP-95 is ready.
 */
public class FileSystemTableFactory implements
		TableSourceFactory<RowData>,
		TableSinkFactory<RowData> {

	public static final String IDENTIFIER = "filesystem";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR, IDENTIFIER);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		// contains format properties.
		return Collections.singletonList("*");
	}

	@Override
	public TableSource<RowData> createTableSource(TableSourceFactory.Context context) {
		Configuration conf = new Configuration();
		context.getTable().getOptions().forEach(conf::setString);

		return new FileSystemTableSource(
				TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema()),
				getPath(conf),
				context.getTable().getPartitionKeys(),
				conf.get(PARTITION_DEFAULT_NAME),
				context.getTable().getProperties());
	}

	@Override
	public TableSink<RowData> createTableSink(TableSinkFactory.Context context) {
		Configuration conf = new Configuration();
		context.getTable().getOptions().forEach(conf::setString);

		return new FileSystemTableSink(
				context.getObjectIdentifier(),
				context.isBounded(),
				TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema()),
				getPath(conf),
				context.getTable().getPartitionKeys(),
				conf.get(PARTITION_DEFAULT_NAME),
				context.getTable().getOptions());
	}

	private static Path getPath(Configuration conf) {
		return new Path(conf.getOptional(PATH).orElseThrow(() ->
				new ValidationException("Path should be not empty.")));
	}

	public static FileSystemFormatFactory createFormatFactory(Map<String, String> properties) {
		String format = properties.get(FORMAT);
		if (format == null) {
			throw new ValidationException(String.format(
					"Table options do not contain an option key '%s' for discovering a format.",
					FORMAT));
		}
		return FactoryUtil.discoverFactory(
				Thread.currentThread().getContextClassLoader(),
				FileSystemFormatFactory.class,
				format);
	}
}
