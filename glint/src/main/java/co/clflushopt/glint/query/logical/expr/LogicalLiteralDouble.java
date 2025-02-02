package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a Double literal.
 */
public class LogicalLiteralDouble implements LogicalExpr {
    private Double value;

    public LogicalLiteralDouble(Double value) {
        this.value = value;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(value.toString(), ArrowTypes.DoubleType);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    public Double getValue() {
        return value;
    }
}
