package co.clflushopt.glint.sql;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * SQL Select statements.
 *
 */
public class SqlSelectStmt implements SqlStatement {
    private final List<SqlExpression> projection;
    private final SqlExpression selection;
    private final List<SqlExpression> groupBy;
    private final List<SqlExpression> orderBy;
    private final SqlExpression having;
    private final String tableName;

    public SqlSelectStmt(List<SqlExpression> projection, SqlExpression selection,
            List<SqlExpression> groupBy, List<SqlExpression> orderBy, SqlExpression having,
            String tableName) {
        this.projection = projection;
        this.selection = selection;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.having = having;
        this.tableName = tableName;
    }

    public List<SqlExpression> getProjection() {
        return projection;
    }

    public SqlExpression getSelection() {
        return selection;
    }

    public List<SqlExpression> getGroupBy() {
        return groupBy;
    }

    public List<SqlExpression> getOrderBy() {
        return orderBy;
    }

    public SqlExpression getHaving() {
        return having;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(String.join(", ",
                projection.stream().map(Object::toString).collect(Collectors.toList())));
        sb.append(" FROM ").append(tableName);

        if (selection != null) {
            sb.append(" WHERE ").append(selection);
        }

        if (!groupBy.isEmpty()) {
            sb.append(" GROUP BY ").append(String.join(", ",
                    groupBy.stream().map(Object::toString).collect(Collectors.toList())));
        }

        if (having != null) {
            sb.append(" HAVING ").append(having);
        }

        if (!orderBy.isEmpty()) {
            sb.append(" ORDER BY ").append(String.join(", ",
                    orderBy.stream().map(Object::toString).collect(Collectors.toList())));
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlSelectStmt sqlSelect = (SqlSelectStmt) o;
        return Objects.equals(projection, sqlSelect.projection)
                && Objects.equals(selection, sqlSelect.selection)
                && Objects.equals(groupBy, sqlSelect.groupBy)
                && Objects.equals(orderBy, sqlSelect.orderBy)
                && Objects.equals(having, sqlSelect.having)
                && Objects.equals(tableName, sqlSelect.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projection, selection, groupBy, orderBy, having, tableName);
    }
}