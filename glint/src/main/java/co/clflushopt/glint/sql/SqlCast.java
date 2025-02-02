package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL cast expressions.
 *
 */
public class SqlCast implements SqlExpression {
    private final SqlExpression expr;
    private final SqlIdentifier dataType;

    public SqlCast(SqlExpression expr, SqlIdentifier dataType) {
        this.expr = expr;
        this.dataType = dataType;
    }

    public SqlExpression getExpr() {
        return expr;
    }

    public SqlIdentifier getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "CAST(" + expr + " AS " + dataType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlCast sqlCast = (SqlCast) o;
        return Objects.equals(expr, sqlCast.expr) && Objects.equals(dataType, sqlCast.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, dataType);
    }
}