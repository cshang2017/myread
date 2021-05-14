package org.apache.flink.runtime.minicluster;

/**
 * Enum which defines whether the mini cluster components use a shared RpcService
 * or whether every component gets its own dedicated RpcService started.
 */
public enum RpcServiceSharing {
	SHARED, // a single shared rpc service
	DEDICATED // every component gets his own dedicated rpc service
}
