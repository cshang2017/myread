package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Three-valued flag for representing {@code TRUE}, {@code FALSE}, and {@code UNKNOWN}.
 */
@PublicEvolving
public enum HintFlag {

	TRUE,

	FALSE,

	UNKNOWN
}
