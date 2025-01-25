package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Aggregate functions.
 *
 * AggregateExpr
 *
 * TODO: make `AggregateExpr` abstract and move out the simpler ones to a
 * different class and add COUNT and COUNT(DISTINCT).
 */
public class AggregateExpr implements LogicalExpr {
    private String name;
    private LogicalExpr expr;

    public AggregateExpr(String name, LogicalExpr expr) {
        this.name = name;
        this.expr = expr;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(name, expr.toField(plan).dataType());
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name, expr);
    }

    public static AggregateExpr Sum(LogicalExpr input) {
        return new AggregateExpr("SUM", input);
    }

    public static AggregateExpr Min(LogicalExpr input) {
        return new AggregateExpr("MIN", input);
    }

    public static AggregateExpr Max(LogicalExpr input) {
        return new AggregateExpr("MAX", input);
    }

    public static AggregateExpr Avg(LogicalExpr input) {
        return new AggregateExpr("AVG", input);
    }
}
