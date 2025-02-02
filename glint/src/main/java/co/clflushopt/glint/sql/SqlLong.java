package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL long literals.
 *
 */
public class SqlLong implements SqlExpression {
    private final long value;

    public SqlLong(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlLong sqlLong = (SqlLong) o;
        return value == sqlLong.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}