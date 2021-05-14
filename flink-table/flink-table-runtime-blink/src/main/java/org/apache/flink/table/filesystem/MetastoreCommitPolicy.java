
package org.apache.flink.table.filesystem;

import org.apache.flink.table.filesystem.TableMetaStoreFactory.TableMetaStore;

import java.util.LinkedHashMap;

/**
 * Partition commit policy to update metastore.
 *
 * <p>If this is for file system table, the metastore is a empty implemantation.
 * If this is for hive table, the metastore is for connecting to hive metastore.
 */
public class MetastoreCommitPolicy implements PartitionCommitPolicy {


	private TableMetaStore metaStore;

	public void setMetastore(TableMetaStore metaStore) {
		this.metaStore = metaStore;
	}

	@Override
	public void commit(Context context) throws Exception {
		LinkedHashMap<String, String> partitionSpec = context.partitionSpec();
		metaStore.createOrAlterPartition(partitionSpec, context.partitionPath());
	}
}
