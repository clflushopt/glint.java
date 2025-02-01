package co.clflushopt.glint.query.optimizer;

import java.util.HashSet;
import java.util.stream.Collectors;

import co.clflushopt.glint.query.logical.plan.Aggregate;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Projection;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.query.logical.plan.Selection;

public class PredicatePushdownRule implements OptimizerRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return pushdown(plan, new HashSet<>());
    }

    private static LogicalPlan pushdown(LogicalPlan plan, HashSet<String> columns) {
        if (plan instanceof Projection) {
            var projection = (Projection) plan;
            columns.addAll(ColumnExtractor.extractColumns(plan, ((Projection) plan).getExpr()));
            var input = pushdown(projection.getInput(), columns);
            return new Projection(input, ((Projection) plan).getExpr());
        }
        if (plan instanceof Selection) {
            var selection = (Selection) plan;
            var newColumns = new HashSet<>(columns);
            newColumns.addAll(ColumnExtractor.extractColumns(plan, selection.getExpr()));
            var input = pushdown(selection.getInput(), newColumns);
            return new Selection(input, selection.getExpr());
        }
        if (plan instanceof Aggregate) {
            var aggregate = (Aggregate) plan;
            var newColumns = new HashSet<>(columns);
            newColumns.addAll(ColumnExtractor.extractColumns(plan, aggregate.getGroupExpr()));
            newColumns.addAll(ColumnExtractor.extractColumns(plan,
                    aggregate.getAggregateExpr().stream().map(e -> e.getExpr()).toList()));
            var input = pushdown(aggregate.getInput(), newColumns);
            return new Aggregate(input, aggregate.getGroupExpr(), aggregate.getAggregateExpr());
        }
        if (plan instanceof Scan) {
            var scanPlan = (Scan) plan;
            var fieldNames = ((Scan) plan).getDataSource().getSchema().getFields().stream()
                    .map(f -> f.name()).collect(Collectors.toSet());
            var pushdownColumns = fieldNames.stream().filter(columns::contains)
                    .collect(Collectors.toList());
            return new Scan(scanPlan.getPath(), scanPlan.getDataSource(), pushdownColumns);
        }
        return plan;
    }
}
