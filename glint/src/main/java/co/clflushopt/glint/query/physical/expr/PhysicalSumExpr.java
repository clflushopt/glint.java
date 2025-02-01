package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.functional.SumAccumulator;

/**
 * Implementation of the `SUM` expression.
 *
 * SumExpr
 */
public class PhysicalSumExpr implements PhysicalAggregateExpr {
    private PhysicalExpr expression;

    /**
     * Create a new `SUM` expression.
     *
     * @param expression the expression to aggregate.
     */
    public PhysicalSumExpr(PhysicalExpr expression) {
        this.expression = expression;
    }

    /**
     * Get the expression being aggregated.
     *
     * @return the expression being aggregated.
     */
    public PhysicalExpr getExpression() {
        return expression;
    }

    /**
     * Get the string representation of the `SUM` expression.
     *
     */
    @Override
    public String toString() {
        return String.format("SUM(%s)", expression.toString());
    }

    @Override
    public Accumulator getAccumulator() {
        return new SumAccumulator();
    }

    @Override
    public PhysicalExpr getInputExpr() {
        return expression;
    }

}
