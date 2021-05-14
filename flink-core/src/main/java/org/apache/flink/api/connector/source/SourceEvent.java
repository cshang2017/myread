package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * An base class for the events passed between the SourceReaders and Enumerators.
 */
@PublicEvolving
public interface SourceEvent extends Serializable {

}
