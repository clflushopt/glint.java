package co.clflushopt.glint.query.plan.logical;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a Long literal.
 */
public class LiteralLong implements LogicalExpr {
    private Long value;

    public LiteralLong(Long value) {
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
}
