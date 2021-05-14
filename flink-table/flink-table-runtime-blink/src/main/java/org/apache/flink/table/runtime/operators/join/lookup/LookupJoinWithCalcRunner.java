package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/**
 * The join runner with an additional calculate function on the dimension table.
 */
public class LookupJoinWithCalcRunner extends LookupJoinRunner {

	private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;

	private transient FlatMapFunction<RowData, RowData> calc;
	private transient Collector<RowData> calcCollector;

	public LookupJoinWithCalcRunner(
			GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
			GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
			GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
			boolean isLeftOuterJoin,
			int tableFieldsCount) {
		super(generatedFetcher, generatedCollector, isLeftOuterJoin, tableFieldsCount);
		this.generatedCalc = generatedCalc;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.calc = generatedCalc.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(calc, getRuntimeContext());
		FunctionUtils.openFunction(calc, parameters);
		this.calcCollector = new CalcCollector(collector);
	}

	@Override
	public void close() throws Exception {
		super.close();
		FunctionUtils.closeFunction(calc);
	}

	@Override
	public Collector<RowData> getFetcherCollector() {
		return calcCollector;
	}

	private class CalcCollector implements Collector<RowData> {

		private final Collector<RowData> delegate;

		private CalcCollector(Collector<RowData> delegate) {
			this.delegate = delegate;
		}

		@Override
		public void collect(RowData record) {
				calc.flatMap(record, delegate);
		}

		@Override
		public void close() {
			delegate.close();
		}
	}
}
