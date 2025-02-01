package co.clflushopt.glint.query.optimizer;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;

/**
 * An optimizer rule is an interface that allows chaining and applying rules to
 * a query plan.
 *
 * OptimizerRule
 */
public interface OptimizerRule {

    /**
     * Apply the rule to the query plan.
     *
     * @param plan the query plan to apply the rule to.
     * @return the optimized query plan.
     */
    public LogicalPlan apply(LogicalPlan plan);
}
