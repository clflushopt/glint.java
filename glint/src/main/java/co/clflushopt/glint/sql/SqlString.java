package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL string literals.
 *
 */
public class SqlString implements SqlExpression {
    private final String value;

    public SqlString(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "'" + value + "'";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SqlString sqlString = (SqlString) o;
        return Objects.equals(value, sqlString.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}