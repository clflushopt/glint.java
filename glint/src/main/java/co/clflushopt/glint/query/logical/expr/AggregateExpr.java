package co.clflushopt.glint.query.logical.expr;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

/**
 * Aggregate functions.
 *
 * AggregateExpr
 *
 * TODO: make `AggregateExpr` abstract and move out the simpler ones to a
 * different class and add COUNT and COUNT(DISTINCT).
 */
public abstract class AggregateExpr implements LogicalExpr {
    protected final String name;
    protected final LogicalExpr expr;

    public AggregateExpr(String name, LogicalExpr expr) {
        this.name = name;
        this.expr = expr;
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
    public static class Sum extends AggregateExpr {
        public Sum(LogicalExpr input) {
            super("SUM", input);
        }
    }

    public static class Min extends AggregateExpr {
        public Min(LogicalExpr input) {
            super("MIN", input);
        }
    }

    public static class Max extends AggregateExpr {
        public Max(LogicalExpr input) {
            super("MAX", input);
        }
    }

    public static class Avg extends AggregateExpr {
        public Avg(LogicalExpr input) {
            super("AVG", input);
        }

        @Override
        public Field toField(LogicalPlan plan) {
            // AVG always returns a floating-point type
            return new Field(name, ArrowTypes.DoubleType);
        }
    }

    public static class Count extends AggregateExpr {
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

    public static class CountDistinct extends AggregateExpr {
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
    public static AggregateExpr sum(LogicalExpr input) {
        return new Sum(input);
    }

    public static AggregateExpr min(LogicalExpr input) {
        return new Min(input);
    }

    public static AggregateExpr max(LogicalExpr input) {
        return new Max(input);
    }

    public static AggregateExpr avg(LogicalExpr input) {
        return new Avg(input);
    }

    public static AggregateExpr count(LogicalExpr input) {
        return new Count(input);
    }
}
