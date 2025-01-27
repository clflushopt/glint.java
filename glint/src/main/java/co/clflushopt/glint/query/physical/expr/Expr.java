package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Physical expression that can be evaluated.
 *
 * Expr
 */
public interface Expr {

    /**
     * Evaluate the expression.
     *
     * @return the result of the expression.
     */
    ColumnVector eval(RecordBatch input);

    /**
     * Returns a string representation of the expression.
     *
     * @return the string representation of the expression.
     */
    String toString();
}
