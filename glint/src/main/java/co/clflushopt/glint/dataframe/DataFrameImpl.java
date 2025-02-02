package co.clflushopt.glint.dataframe;

import java.util.List;

import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.Aggregate;
import co.clflushopt.glint.query.logical.plan.Filter;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Projection;
import co.clflushopt.glint.types.Schema;

public class DataFrameImpl implements DataFrame {
    private LogicalPlan plan;

    public DataFrameImpl(LogicalPlan plan) {
        this.plan = plan;
    }

    @Override
    public DataFrame project(List<LogicalExpr> expr) {
        return new DataFrameImpl(new Projection(plan, expr));
    }

    @Override
    public DataFrame filter(LogicalExpr expr) {
        return new DataFrameImpl(new Filter(plan, expr));
    }

    @Override
    public DataFrame aggregate(List<LogicalExpr> groupBy,
            List<LogicalAggregateExpr> aggregateExpr) {
        return new DataFrameImpl(new Aggregate(plan, groupBy, aggregateExpr));
    }

    @Override
    public Schema getSchema() {
        return plan.getSchema();
    }

    @Override
    public LogicalPlan getLogicalPlan() {
        return plan;
    }
}
