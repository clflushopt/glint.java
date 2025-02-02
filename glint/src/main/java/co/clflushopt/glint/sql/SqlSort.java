package co.clflushopt.glint.sql;

import java.util.Objects;

public class SqlSort implements SqlExpression {
    private final SqlExpression expr;
    private final boolean asc;

    public SqlSort(SqlExpression expr, boolean asc) {
        this.expr = expr;
        this.asc = asc;
    }

    public SqlExpression getExpr() {
        return expr;
    }

    public boolean isAsc() {
        return asc;
    }

    @Override
    public String toString() {
        return expr + (asc ? " ASC" : " DESC");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlSort sqlSort = (SqlSort) o;
        return asc == sqlSort.asc && Objects.equals(expr, sqlSort.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, asc);
    }
}
