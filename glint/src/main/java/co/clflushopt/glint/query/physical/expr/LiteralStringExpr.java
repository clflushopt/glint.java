package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.LiteralValueVector;
import co.clflushopt.glint.types.RecordBatch;

public class LiteralStringExpr implements PhysicalExpr {
    private String value;

    public LiteralStringExpr(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("'%s'", value);
    }

    @Override
    public ColumnVector eval(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.StringType, value.getBytes(), input.getRowCount());
    }

}
