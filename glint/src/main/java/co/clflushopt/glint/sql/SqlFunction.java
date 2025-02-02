package co.clflushopt.glint.sql;

import java.util.List;
import java.util.Objects;

/**
 * SQL function calls.
 *
 */
public class SqlFunction implements SqlExpression {
    private final String id;
    private final List<SqlExpression> args;

    public SqlFunction(String id, List<SqlExpression> args) {
        this.id = id;
        this.args = args;
    }

    public String getId() {
        return id;
    }

    public List<SqlExpression> getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlFunction that = (SqlFunction) o;
        return Objects.equals(id, that.id) && Objects.equals(args, that.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, args);
    }
}