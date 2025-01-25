package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a named column reference which can either be
 * a column from the data source or a column in an input logical plan.
 *
 * ColumnExpr
 */
public class ColumnExpr implements LogicalExpr {
    private String name;

    public ColumnExpr(String name) {
        this.name = name;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return plan.getSchema().getFields().stream().filter(field -> field.name() == name)
                .findFirst().get();
    }

    @Override
    public String toString() {
        return "#" + name;
    }
}
