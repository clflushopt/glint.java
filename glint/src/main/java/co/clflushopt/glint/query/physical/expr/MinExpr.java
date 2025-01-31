package co.clflushopt.glint.query.physical.expr;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.functional.MinAccumulator;

/**
 * Phyiscal expression for the `MIN` expression.
 *
 * MinExpr
 */
public class MinExpr implements AggregateExpr {
    /**
     * Expression being aggregated.
     */
    private Expr expression;

    /**
     * Create a new `MAX` expression.
     *
     * @param expression the expression to aggregate.
     */
    public MinExpr(Expr expression) {
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
        return String.format("MIN(%s)", expression.toString());
    }

    @Override
    public Accumulator getAccumulator() {
        return new MinAccumulator();
    }

    @Override
    public Expr getInputExpr() {
        return expression;
    }
}
