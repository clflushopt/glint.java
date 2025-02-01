package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.query.functional.Accumulator;

/**
 * Interface for aggregate expressions.
 *
 * AggregateExpr
 */
public interface PhysicalAggregateExpr {

    /**
     * Get the accumulator for the aggregate expression.
     *
     * @return
     */
    public Accumulator getAccumulator();

    /**
     * Get the input expression for the aggregate expression.
     *
     * @return
     */
    public PhysicalExpr getInputExpr();
}
