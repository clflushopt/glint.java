package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * Mathematical expressions that return a numerical value.
 *
 * MathExpr
 */
public class LogicalMathExpr extends LogicalBinaryExpr implements LogicalExpr {

    public LogicalMathExpr(String name, String operator, LogicalExpr lhs, LogicalExpr rhs) {
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
    public static LogicalMathExpr Add(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalMathExpr("add", "+", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for substraction.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalMathExpr Sub(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalMathExpr("sub", "-", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for multiplication.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalMathExpr Mul(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalMathExpr("mul", "*", lhs, rhs);
    }

    /**
     * Returns an instance of `BinaryExpr` specialized for division.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalMathExpr Div(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalMathExpr("div", "/", lhs, rhs);
    }
}
