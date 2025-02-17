package co.clflushopt.glint.query.logical.plan;

import java.util.List;

import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.types.Schema;

/**
 * The selection logical plan applies a filter expression to the tuples of the
 * parent logical plan, this is the `WHERE` clause in SQL.
 *
 * Selection
 */
public class Filter implements LogicalPlan {
    private LogicalPlan input;
    private LogicalExpr expr;

    public Filter(LogicalPlan input, LogicalExpr expr) {
        this.input = input;
        this.expr = expr;
    }

    public LogicalExpr getExpr() {
        return expr;
    }

    public LogicalPlan getInput() {
        return input;
    }

    @Override
    public Schema getSchema() {
        // Schema is inherited from the parent plan.
        return input.getSchema();
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return List.of(input);
    }

    @Override
    public String toString() {
        return String.format("Filter: %s", expr.toString());
    }

}
