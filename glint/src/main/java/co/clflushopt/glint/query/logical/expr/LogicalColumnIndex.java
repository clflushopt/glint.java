package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Logical expression representing a referenced column by an index. ColumnIndex
 */
public class LogicalColumnIndex implements LogicalExpr {
    private Integer index;

    public LogicalColumnIndex(Integer index) {
        this.index = index;
    }

    public Integer getIndex() {
        return index;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return plan.getSchema().getFields().get(this.index);
    }

    @Override
    public String toString() {
        return "#" + index.toString();
    }
}
