package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL binary expressions.
 *
 */
public class SqlBinaryExpr implements SqlExpression {
    private final SqlExpression left;
    private final String operator;
    private final SqlExpression right;

    public SqlBinaryExpr(SqlExpression left, String operator, SqlExpression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public SqlExpression getLeft() {
        return left;
    }

    public String getOperator() {
        return operator;
    }

    public SqlExpression getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "(" + left + " " + operator + " " + right + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlBinaryExpr that = (SqlBinaryExpr) o;
        return Objects.equals(left, that.left) && Objects.equals(operator, that.operator)
                && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, operator, right);
    }
}
