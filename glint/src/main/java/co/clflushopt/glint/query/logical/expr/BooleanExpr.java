package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Binary expressions that return a boolean value.
 *
 * BooleanBinaryExpr
 */
public class BooleanExpr extends BinaryExpr implements LogicalExpr {

    public BooleanExpr(String name, String operator, LogicalExpr lhs, LogicalExpr rhs) {
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
    public static BooleanExpr Eq(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("eq", "=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for inequality.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Neq(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("neq", "!=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for greater-than.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Gt(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("gt", ">", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for greater-than or
     * equal.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Gte(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("gte", ">=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for lesser-than.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Lt(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("lt", "<", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for lesser-than or
     * equal.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Lte(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("lte", "<=", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for boolean AND.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr And(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("and", "AND", lhs, rhs);
    }

    /**
     * Returns an instance of `BooleanBinaryExpr` specialized for boolean OR.
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static BooleanExpr Or(LogicalExpr lhs, LogicalExpr rhs) {
        return new BooleanExpr("or", "OR", lhs, rhs);
    }
}
