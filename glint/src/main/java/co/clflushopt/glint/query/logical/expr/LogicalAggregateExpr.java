package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Aggregate functions.
 *
 * AggregateExpr
 */
public abstract class LogicalAggregateExpr implements LogicalExpr {
    protected final String name;
    protected final LogicalExpr expr;

    public LogicalAggregateExpr(String name, LogicalExpr expr) {
        this.name = name;
        this.expr = expr;
    }

    public LogicalExpr getExpr() {
        return expr;
    }

    @Override
    public Field toField(LogicalPlan plan) {
        return new Field(name, expr.toField(plan).dataType());
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name, expr);
    }

    /**
     * Sum aggregation operator.
     *
     */
    public static class Sum extends LogicalAggregateExpr {
        public Sum(LogicalExpr input) {
            super("SUM", input);
        }
    }

    public static class Min extends LogicalAggregateExpr {
        public Min(LogicalExpr input) {
            super("MIN", input);
        }
    }

    public static class Max extends LogicalAggregateExpr {
        public Max(LogicalExpr input) {
            super("MAX", input);
        }
    }

    public static class Avg extends LogicalAggregateExpr {
        public Avg(LogicalExpr input) {
            super("AVG", input);
        }

        @Override
        public Field toField(LogicalPlan plan) {
            // AVG always returns a floating-point type
            return new Field(name, ArrowTypes.DoubleType);
        }
    }

    public static class Count extends LogicalAggregateExpr {
        public Count(LogicalExpr input) {
            super("COUNT", input);
        }

        @Override
        public Field toField(LogicalPlan plan) {
            // COUNT always returns an integer type
            return new Field(name, ArrowTypes.Int64Type);
        }

        @Override
        public String toString() {
            return String.format("COUNT(%s)", expr.toString());
        }
    }

    public static class CountDistinct extends LogicalAggregateExpr {
        public CountDistinct(LogicalExpr input) {
            super("COUNT_DISTINCT", input);
        }

        @Override
        public Field toField(LogicalPlan plan) {
            return new Field(name, ArrowTypes.Int64Type);
        }

        @Override
        public String toString() {
            return String.format("COUNT(DISTINCT %s)", expr.toString());
        }
    }

    // Factory methods for creating aggregate expressions
    public static LogicalAggregateExpr sum(LogicalExpr input) {
        return new Sum(input);
    }

    public static LogicalAggregateExpr min(LogicalExpr input) {
        return new Min(input);
    }

    public static LogicalAggregateExpr max(LogicalExpr input) {
        return new Max(input);
    }

    public static LogicalAggregateExpr avg(LogicalExpr input) {
        return new Avg(input);
    }

    public static LogicalAggregateExpr count(LogicalExpr input) {
        return new Count(input);
    }
}
