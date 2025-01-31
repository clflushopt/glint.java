package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.functional.MaxAccumulator;

/**
 * Phyiscal expression for the `MAX` expression.
 *
 * MaxExpr
 */
public class MaxExpr implements AggregateExpr {
    /**
     * Expression being aggregated.
     */
    private Expr expression;

    /**
     * Create a new `MAX` expression.
     *
     * @param expression the expression to aggregate.
     */
    public MaxExpr(Expr expression) {
        this.expression = expression;
    }

    /**
     * Get the expression being aggregated.
     *
     * @return the expression being aggregated.
     */
    public Expr getExpression() {
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
    public Expr getInputExpr() {
        return expression;
    }
}
