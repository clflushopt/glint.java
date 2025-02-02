package co.clflushopt.glint.sql;

import java.util.Objects;

/**
 * SQL double literals.
 *
 */
public class SqlDouble implements SqlExpression {
    private final double value;

    public SqlDouble(double value) {
        this.value = value;
    }

    public double getValue() {
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
        SqlDouble sqlDouble = (SqlDouble) o;
        return Double.compare(sqlDouble.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}