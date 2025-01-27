package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Column expression that represents a column in a record batch.
 *
 * ColumnExpr
 */
public class ColumnExpr implements Expr {
    private int index;

    public ColumnExpr(int index) {
        this.index = index;
    }

    /**
     * Returns the column vector at the given index.
     *
     */
    @Override
    public ColumnVector eval(RecordBatch input) {
        return input.getField(index);
    }

    @Override
    public String toString() {
        return String.format("#%d", index);
    }

}
