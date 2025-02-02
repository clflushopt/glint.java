package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL alias expressions.
 *
 */
public class SqlAliasExpr implements SqlExpression {
    private final SqlExpression expr;
    private final SqlIdentifier alias;

    public SqlAliasExpr(SqlExpression expr, SqlIdentifier alias) {
        this.expr = expr;
        this.alias = alias;
    }

    public SqlExpression getExpr() {
        return expr;
    }

    public SqlIdentifier getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return expr + " AS " + alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlAliasExpr sqlAlias = (SqlAliasExpr) o;
        return Objects.equals(expr, sqlAlias.expr) && Objects.equals(alias, sqlAlias.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, alias);
    }
}