package co.clflushopt.glint.query.logical.plan;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import co.clflushopt.glint.query.logical.expr.AggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.types.Schema;

/**
 * The aggregate plan represents query aggregation where an aggregation
 * expression is applied to a projection and grouped by another.
 *
 * Aggregate
 */
public class Aggregate implements LogicalPlan {
    private LogicalPlan input;
    private List<LogicalExpr> groupExpr;
    private List<AggregateExpr> aggregateExpr;

    public Aggregate(LogicalPlan input, List<LogicalExpr> groupBy, List<AggregateExpr> aggregates) {
        this.input = input;
        this.groupExpr = groupBy;
        this.aggregateExpr = aggregates;
    }

    @Override
    public Schema getSchema() {
        return new Schema(Stream
                .concat(groupExpr.stream().map(e -> e.toField(input)),
                        aggregateExpr.stream().map(e -> e.toField(input)))
                .collect(Collectors.toList()));
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return List.of(input);
    }

    @Override
    public String toString() {
        return String.format("Aggregate: groupExpr=%s, aggregateExpr=%s", groupExpr, aggregateExpr);
    }
}
