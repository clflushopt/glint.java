package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.LiteralValueVector;
import co.clflushopt.glint.types.RecordBatch;

public class LiteralDoubleExpr implements Expr {
    private double value;

    public LiteralDoubleExpr(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.DoubleType, value, input.getRowSize());
    }

}
