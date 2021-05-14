package org.apache.flink.table.functions;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for a user-defined asynchronously table function (UDTF). This is similar to
 * {@link TableFunction} but this function is asynchronously.
 *
 * <p>A user-defined table functions works on
 * zero, one, or multiple scalar values as input and returns multiple rows as output.
 *
 * <p>The behavior of a {@link AsyncTableFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly, not static and named "eval".
 * Evaluation methods can also be overloaded by implementing multiple methods named "eval".
 *
 * <p>The first parameter of evaluation method must be {@link CompletableFuture}, and the others are
 * user defined input parameters like the "eval" method of {@link TableFunction}. The generic type of
 * {@link CompletableFuture} must be {@link java.util.Collection} to collect multiple possible result
 * values.
 *
 * <p>For each "eval", an async io operation can be triggered, and once it has been done,
 * the result can be collected by calling {@link CompletableFuture#complete}. For each async
 * operation, its context is stored in the operator immediately after invoking "eval",
 * avoiding blocking for each stream input as long as the internal buffer is not full.
 *
 * <p>{@link CompletableFuture} can be passed into callbacks or futures to collect the result data.
 * An error can also be propagate to the async IO operator by
 * {@link CompletableFuture#completeExceptionally(Throwable)}.
 *
 * <p>User-defined functions must have a default constructor and must be instantiable during
 * runtime.
 *
 * <p>By default the result type of an evaluation method is determined by Flink's type extraction
 * facilities. Currently, only support {@link org.apache.flink.types.Row} and {@link RowData} as
 * the result type. Will support more complex, custom types in the future.
 *
 * <p>Example:
 *
 * {@code
 *
 *   public class HBaseAsyncTableFunction extends AsyncTableFunction<String> {
 *
 *     // implement an "eval" method with as many parameters as you want
 *     public void eval(CompletableFuture<Collection<String>> result, String rowkey) {
 *       Get get = new Get(Bytes.toBytes(rowkey));
 *       ListenableFuture<Result> future = hbase.asyncGet(get);
 *       Futures.addCallback(future, new FutureCallback<Result>() {
 *         public void onSuccess(Result result) {
 *           List<String> ret = process(result);
 *           result.complete(ret);
 *         }
 *         public void onFailure(Throwable thrown) {
 *           result.completeExceptionally(thrown);
 *         }
 *       });
 *     }
 *
 *     // you can overload the eval method here ...
 *   }
 * }
 *
 * <p>NOTE: the {@link AsyncTableFunction} can not be used as UDTF currently. It only used in
 * temporal table join as an async lookup function.
 *
 * @param <T> The type of the output row
 */
@Experimental
public abstract class AsyncTableFunction<T> extends UserDefinedFunction {

	/**
	 * Returns the result type of the evaluation method with a given signature.
	 *
	 * <p>This method needs to be overridden in case Flink's type extraction facilities are not
	 * sufficient to extract the {@link TypeInformation} based on the return type of the evaluation
	 * method. Flink's type extraction facilities can handle basic types or
	 * simple POJOs but might be wrong for more complex, custom, or composite types.
	 *
	 * @return {@link TypeInformation} of result type or <code>null</code> if Flink should determine the type
	 */
	public TypeInformation<T> getResultType() {
		return null;
	}

	@Override
	public final FunctionKind getKind() {
		return FunctionKind.ASYNC_TABLE;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		throw new TableException("Async table functions are not updated to the new type system yet.");
	}
}
