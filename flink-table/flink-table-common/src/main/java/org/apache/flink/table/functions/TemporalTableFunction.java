package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

/**
 * Class representing temporal table function over some history table. A
 * {@link TemporalTableFunction} is also an instance of {@link TableFunction}.
 *
 * <p>Currently {@link TemporalTableFunction}s are only supported in streaming.
 */
@PublicEvolving
public abstract class TemporalTableFunction extends TableFunction<Row> {

}
