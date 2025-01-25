package co.clflushopt.glint.query.plan.logical;

import co.clflushopt.glint.types.Field;

/**
 * Mathematical expressions that return a numerical value.
 *
 * MathExpr
 */
public class MathExpr extends BinaryExpr implements LogicalExpr {

    public MathExpr(String name, String operator, LogicalExpr lhs, LogicalExpr rhs) {
        super(name, operator, lhs, rhs);
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(this.getName(), this.getLhs().toField(plan).dataType());
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for addition.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static MathExpr Add(LogicalExpr lhs, LogicalExpr rhs) {
        return new MathExpr("add", "+", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for substraction.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static MathExpr Sub(LogicalExpr lhs, LogicalExpr rhs) {
        return new MathExpr("sub", "-", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for multiplication.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static MathExpr Mul(LogicalExpr lhs, LogicalExpr rhs) {
        return new MathExpr("mul", "*", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for division.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static MathExpr Div(LogicalExpr lhs, LogicalExpr rhs) {
        return new MathExpr("div", "/", lhs, rhs);
    }
}
