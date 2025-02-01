package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.physical.expr.PhysicalColumnExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalMaxExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalMinExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalSumExpr;

public class AggregateExprTest {

    @Test
    public void testMinAccumulator() {
        PhysicalMinExpr minExpr = new PhysicalMinExpr(new PhysicalColumnExpr(0));
        Accumulator accumulator = minExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(4, accumulator.getResult());
    }

    @Test
    public void testMaxAccumulator() {
        PhysicalMaxExpr maxExpr = new PhysicalMaxExpr(new PhysicalColumnExpr(0));
        Accumulator accumulator = maxExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(14, accumulator.getResult());
    }

    @Test
    public void testSumAccumulator() {
        PhysicalSumExpr sumExpr = new PhysicalSumExpr(new PhysicalColumnExpr(0));
        Accumulator accumulator = sumExpr.getAccumulator();

        List<Integer> values = Arrays.asList(10, 14, 4);
        for (Integer value : values) {
            accumulator.accumulate(value);
        }

        assertEquals(28.0, accumulator.getResult());
    }
}
