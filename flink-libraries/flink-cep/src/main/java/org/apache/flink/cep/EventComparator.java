
package org.apache.flink.cep;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Custom comparator used to compares two events.
 *
 * @param <T> Type of the event
 */
public interface EventComparator<T> extends Comparator<T>, Serializable {
}
