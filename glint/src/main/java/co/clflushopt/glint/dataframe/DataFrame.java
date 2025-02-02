package co.clflushopt.glint.dataframe;

import java.util.List;

import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Schema;

/**
 * The dataframe API allows you to lazily build queries by chaining operators.
 *
 * DataFrame
 */
public interface DataFrame {

    /**
     * Apply a projection.
     *
     * @return projected columns as a dataframe.
     */
    public DataFrame project(List<LogicalExpr> expr);

    /**
     * Apply a filter.
     *
     * @return filtered tuples as a dataframe.
     */
    public DataFrame filter(LogicalExpr expr);

    /**
     * Apply an aggregation.
     *
     * @return aggregated tuples as a dataframe.
     */
    public DataFrame aggregate(List<LogicalExpr> groupBy, List<LogicalAggregateExpr> aggregateExpr);

    /**
     * Returns the schema of the tuples in the dataframe.
     *
     * @return
     */
    public Schema getSchema();

    /**
     * Build the logical plan.
     *
     * @return Logical plan representing the steps necessary to build the final
     *         dataframe.
     */
    public LogicalPlan getLogicalPlan();
}
