package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;

/**
 * Column expression that represents a column in a record batch.
 *
 * ColumnExpr
 */
public class PhysicalColumnExpr implements PhysicalExpr {
    private int index;

    public PhysicalColumnExpr(int index) {
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
