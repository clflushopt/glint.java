package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.Field;

/**
 * LogicalExpr is used to represent expressions such as column projection or
 * binary expressions and such.
 *
 */
public interface LogicalExpr {
    /**
     * Returns the field metadata that the logical expression will return.
     *
     * For example `LiteralString` will return a `Field` with `ArrowTypes.String`.
     *
     * @param plan
     * @return
     */
    public Field toField(LogicalPlan plan);

    /**
     * Creates an equality comparison expression (=).
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // status = 'active'
     * col("status").eq(lit("active"))
     * }</pre>
     *
     * @param other The expression to compare with
     * @return A boolean expression representing the equality comparison
     */
    default LogicalExpr eq(LogicalExpr other) {
        return LogicalBooleanExpr.Eq(this, other);
    }

    /**
     * Creates an inequality comparison expression (!=).
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // status != 'inactive'
     * col("status").neq(lit("inactive"))
     * }</pre>
     *
     * @param other The expression to compare with
     * @return A boolean expression representing the inequality comparison
     */
    default LogicalExpr neq(LogicalExpr other) {
        return LogicalBooleanExpr.Neq(this, other);
    }

    /**
     * Creates a greater-than comparison expression (&gt;).
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // salary > 50000
     * col("salary").gt(lit(50000))
     * }</pre>
     *
     * @param other The expression to compare with
     * @return A boolean expression representing the greater-than comparison
     */
    default LogicalExpr gt(LogicalExpr other) {
        return LogicalBooleanExpr.Gt(this, other);
    }

    /**
     * Creates a multiplication arithmetic expression (*). The resulting type is
     * determined by the types of the operands.
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // Apply 20% discount: price * 0.8
     * col("price").mult(lit(0.8))
     * }</pre>
     *
     * @param other The expression to multiply with
     * @return A binary expression representing the multiplication
     */
    default LogicalExpr mult(LogicalExpr other) {
        return LogicalMathExpr.Mul(this, other);
    }

    /**
     * Creates a division arithmetic expression (/). The result is always a
     * floating-point type to handle potential decimal results.
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // Calculate average: total / count
     * col("total").div(col("count"))
     * }</pre>
     *
     * @param other The expression to divide by
     * @return A binary expression representing the division
     */
    default LogicalExpr div(LogicalExpr other) {
        return LogicalMathExpr.Div(this, other);
    }

    /**
     * Creates an alias for this expression. Used to give expressions meaningful
     * names in the query result.
     *
     * <p>
     * Example usage:
     *
     * <pre>{@code
     * // salary * 0.1 AS bonus
     * col("salary").mult(lit(0.1)).alias("bonus")
     * }</pre>
     *
     * @param alias The name to assign to this expression's result
     * @return An aliased expression
     */
    default LogicalAliasExpr alias(String alias) {
        return new LogicalAliasExpr(this, alias);
    }
}
