package co.clflushopt.glint.query.logical.plan;

import java.util.List;

import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.types.Schema;

/**
 * The projection logical plan applies a projection to the the input;
 * projections can be column literals or full expressions.
 *
 * For example `SELECT (CAST(user_id AS INT64))`.
 *
 * Projection
 */
public class Projection implements LogicalPlan {
    private LogicalPlan input;
    private List<LogicalExpr> expr;

    public Projection(LogicalPlan input, List<LogicalExpr> expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema getSchema() {
        return new Schema(expr.stream().map(e -> e.toField(input)).toList());
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(input);
    }

    @Override
    public String toString() {
        return String.format("Projection: %s",
                String.join(", ", expr.stream().map(e -> e.toString()).toList()));
    }
}
