package co.clflushopt.glint.query.logical.plan;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
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
    private List<LogicalAggregateExpr> aggregateExpr;

    public Aggregate(LogicalPlan input, List<LogicalExpr> groupBy,
            List<LogicalAggregateExpr> aggregates) {
        this.input = input;
        this.groupExpr = groupBy;
        this.aggregateExpr = aggregates;
    }

    public List<LogicalExpr> getGroupExpr() {
        return groupExpr;
    }

    public List<LogicalAggregateExpr> getAggregateExpr() {
        return aggregateExpr;
    }

    public LogicalPlan getInput() {
        return input;
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
