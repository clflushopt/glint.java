package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.physical.expr.ColumnExpr;
import co.clflushopt.glint.query.physical.expr.MaxExpr;
import co.clflushopt.glint.query.physical.expr.MinExpr;
import co.clflushopt.glint.query.physical.expr.SumExpr;

public class AggregateExprTest {

    @Test
    public void testMinAccumulator() {
        MinExpr minExpr = new MinExpr(new ColumnExpr(0));
        Accumulator accumulator = minExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(4, accumulator.getResult());
    }

    @Test
    public void testMaxAccumulator() {
        MaxExpr maxExpr = new MaxExpr(new ColumnExpr(0));
        Accumulator accumulator = maxExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(14, accumulator.getResult());
    }

    @Test
    public void testSumAccumulator() {
        SumExpr sumExpr = new SumExpr(new ColumnExpr(0));
        Accumulator accumulator = sumExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(28.0, accumulator.getResult());
    }
}
