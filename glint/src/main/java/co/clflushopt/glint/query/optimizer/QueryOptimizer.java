package co.clflushopt.glint.query.optimizer;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;

/**
 * The query optimizer is responsible for optimizing the query plan at the
 * logical level.
 *
 * QueryOptimizer
 */
public class QueryOptimizer {

    /**
     * Optimizes the logical plan by applying all rules in the optimizer.
     *
     * @param plan
     * @return
     */
    public static LogicalPlan optimize(LogicalPlan plan) {
        return plan;
    }

}
