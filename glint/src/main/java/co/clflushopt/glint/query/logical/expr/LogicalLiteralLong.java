package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a Long literal.
 */
public class LogicalLiteralLong implements LogicalExpr {
    private Long value;

    public LogicalLiteralLong(Long value) {
        this.value = value;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(value.toString(), ArrowTypes.Int64Type);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    public Long getValue() {
        return value;
    }
}
