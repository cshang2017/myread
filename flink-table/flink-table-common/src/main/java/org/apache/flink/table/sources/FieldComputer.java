

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedFieldReference;

/**
 * The {@link FieldComputer} interface returns an expression to compute the field of the table
 * schema of a {@link TableSource} from one or more fields of the {@link TableSource}'s return type.
 *
 * @param <T> The result type of the provided expression.
 */
@PublicEvolving
public interface FieldComputer<T> {

	/**
	 * Returns the names of all fields that the expression of the field computer accesses.
	 *
	 * @return An array with the names of all accessed fields.
	 */
	String[] getArgumentFields();

	/**
	 * Returns the result type of the expression.
	 *
	 * @return The result type of the expression.
	 */
	TypeInformation<T> getReturnType();

	/**
	 * Validates that the fields that the expression references have the correct types.
	 *
	 * @param argumentFieldTypes The types of the physical input fields.
	 */
	void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes);

	/**
	 * Returns the {@link Expression} that computes the value of the field.
	 *
	 * @param fieldAccesses Field access expressions for the argument fields.
	 * @return The expression to extract the timestamp from the {@link TableSource} return type.
	 */
	Expression getExpression(ResolvedFieldReference[] fieldAccesses);
}
