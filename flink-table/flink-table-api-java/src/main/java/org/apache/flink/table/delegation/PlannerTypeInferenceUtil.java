

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

import java.util.List;

/**
 * Temporary utility for validation and output type inference until all {@code PlannerExpression} are
 * upgraded to work with {@link TypeInferenceUtil}.
 */
@Internal
public interface PlannerTypeInferenceUtil {

	/**
	 * Same behavior as {@link TypeInferenceUtil#runTypeInference(TypeInference, CallContext)}.
	 */
	TypeInferenceUtil.Result runTypeInference(
		UnresolvedCallExpression unresolvedCall,
		List<ResolvedExpression> resolvedArgs);
}
