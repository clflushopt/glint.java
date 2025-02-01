package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.functional.MaxAccumulator;

/**
 * Phyiscal expression for the `MAX` expression.
 *
 * MaxExpr
 */
public class PhysicalMaxExpr implements PhysicalAggregateExpr {
    /**
     * Expression being aggregated.
     */
    private PhysicalExpr expression;

    /**
     * Create a new `MAX` expression.
     *
     * @param expression the expression to aggregate.
     */
    public PhysicalMaxExpr(PhysicalExpr expression) {
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

    @Override
    public String toString() {
        return String.format("MAX(%s)", expression.toString());
    }

    @Override
    public Accumulator getAccumulator() {
        return new MaxAccumulator();
    }

    @Override
    public PhysicalExpr getInputExpr() {
        return expression;
    }
}
