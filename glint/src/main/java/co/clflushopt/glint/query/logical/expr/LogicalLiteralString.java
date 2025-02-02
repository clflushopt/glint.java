package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a string literal.
 */
public class LogicalLiteralString implements LogicalExpr {
    private String literal;

    public LogicalLiteralString(String literal) {
        this.literal = literal;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(literal, ArrowTypes.StringType);
    }

    @Override
    public String toString() {
        return "'" + literal + "'";
    }

    public String getValue() {
        return literal;
    }
}
