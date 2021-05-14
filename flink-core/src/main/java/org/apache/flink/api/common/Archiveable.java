package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

@Internal
public interface Archiveable<T extends Serializable> {
	T archive();
}
