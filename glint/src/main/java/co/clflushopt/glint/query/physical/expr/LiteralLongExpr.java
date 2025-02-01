package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.LiteralValueVector;
import co.clflushopt.glint.types.RecordBatch;

public class LiteralLongExpr implements PhysicalExpr {
    private long value;

    public LiteralLongExpr(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.Int64Type, value, input.getRowSize());
    }

}
