package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Binary expressions that return a boolean value.
 *
 * BooleanBinaryExpr
 */
public class LogicalBooleanExpr extends LogicalBinaryExpr implements LogicalExpr {

    public LogicalBooleanExpr(String name, String operator, LogicalExpr lhs, LogicalExpr rhs) {
        super(name, operator, lhs, rhs);
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(this.getName(), ArrowTypes.BooleanType);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for equality.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Eq(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("eq", "=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for inequality.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Neq(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("neq", "!=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for greater-than.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Gt(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("gt", ">", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for greater-than or
     * equal.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Gte(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("gte", ">=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for lesser-than.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Lt(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("lt", "<", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for lesser-than or
     * equal.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Lte(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("lte", "<=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for boolean AND.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr And(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("and", "AND", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for boolean OR.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static LogicalBooleanExpr Or(LogicalExpr lhs, LogicalExpr rhs) {
        return new LogicalBooleanExpr("or", "OR", lhs, rhs);
    }
}
