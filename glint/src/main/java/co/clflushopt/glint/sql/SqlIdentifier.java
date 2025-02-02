package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL identifiers.
 *
 */
public class SqlIdentifier implements SqlExpression {
    private final String id;

    public SqlIdentifier(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
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
        SqlIdentifier that = (SqlIdentifier) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}