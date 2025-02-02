package co.clflushopt.glint.query.logical.expr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;

public class AggregateExprTest {

    private static class MockLogicalExpr implements LogicalExpr {
        private final String name;
        private final Field field;

        public MockLogicalExpr(String name, Field field) {
            this.name = name;
            this.field = field;
        }

        @Override
        public Field toField(LogicalPlan plan) {
            return field;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Test
    public void testSum() {
        LogicalExpr input = new MockLogicalExpr("input", new Field("input", ArrowTypes.Int64Type));
        LogicalAggregateExpr sum = new LogicalAggregateExpr.Sum(input);

        assertEquals("SUM(input)", sum.toString());
        assertEquals(new Field("SUM", ArrowTypes.Int64Type), sum.toField(null));
    }

    @Test
    public void testMin() {
        LogicalExpr input = new MockLogicalExpr("input", new Field("input", ArrowTypes.Int64Type));
        LogicalAggregateExpr min = new LogicalAggregateExpr.Min(input);

        assertEquals("MIN(input)", min.toString());
        assertEquals(new Field("MIN", ArrowTypes.Int64Type), min.toField(null));
    }

    @Test
    public void testMax() {
        LogicalExpr input = new MockLogicalExpr("input", new Field("input", ArrowTypes.Int64Type));
        LogicalAggregateExpr max = new LogicalAggregateExpr.Max(input);

        assertEquals("MAX(input)", max.toString());
        assertEquals(new Field("MAX", ArrowTypes.Int64Type), max.toField(null));
    }

    @Test
    public void testAvg() {
        LogicalExpr input = new MockLogicalExpr("input", new Field("input", ArrowTypes.Int64Type));
        LogicalAggregateExpr avg = new LogicalAggregateExpr.Avg(input);

        assertEquals("AVG(input)", avg.toString());
        assertEquals(new Field("AVG", ArrowTypes.DoubleType), avg.toField(null));
    }

    @Test
    public void testCount() {
        LogicalExpr input = new MockLogicalExpr("input", new Field("input", ArrowTypes.Int64Type));
        LogicalAggregateExpr count = new LogicalAggregateExpr.Count(input);

        assertEquals("COUNT(input)", count.toString());
        assertEquals(new Field("COUNT", ArrowTypes.Int64Type), count.toField(null));
    }
}