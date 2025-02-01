package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.physical.expr.PhysicalBooleanExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalColumnExpr;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;
import co.clflushopt.glint.util.Fuzzer;

@SuppressWarnings("unchecked")
public class BooleanExprTest {

    @Test
    public void testGteBytes() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.Int8Type),
                new Field("b", ArrowTypes.Int8Type)));

        List<Byte> a = Arrays.asList((byte) 10, (byte) 20, (byte) 30, Byte.MIN_VALUE,
                Byte.MAX_VALUE);
        List<Byte> b = Arrays.asList((byte) 10, (byte) 30, (byte) 20, Byte.MAX_VALUE,
                Byte.MIN_VALUE);

        // Cast the lists to List<Object>
        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);

        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i) >= b.get(i), result.getValue(i));
        }
    }

    @Test
    public void testGteShorts() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.Int16Type),
                new Field("b", ArrowTypes.Int16Type)));

        List<Short> a = Arrays.asList((short) 111, (short) 222, (short) 333, Short.MIN_VALUE,
                Short.MAX_VALUE);
        List<Short> b = Arrays.asList((short) 111, (short) 333, (short) 222, Short.MAX_VALUE,
                Short.MIN_VALUE);

        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i) >= b.get(i), result.getValue(i));
        }
    }

    @Test
    public void testGteInts() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.Int32Type),
                new Field("b", ArrowTypes.Int32Type)));

        List<Integer> a = Arrays.asList(111, 222, 333, Integer.MIN_VALUE, Integer.MAX_VALUE);
        List<Integer> b = Arrays.asList(111, 333, 222, Integer.MAX_VALUE, Integer.MIN_VALUE);

        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i) >= b.get(i), result.getValue(i));
        }
    }

    @Test
    public void testGteLongs() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.Int64Type),
                new Field("b", ArrowTypes.Int64Type)));

        List<Long> a = Arrays.asList(111L, 222L, 333L, Long.MIN_VALUE, Long.MAX_VALUE);
        List<Long> b = Arrays.asList(111L, 333L, 222L, Long.MAX_VALUE, Long.MIN_VALUE);

        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i) >= b.get(i), result.getValue(i));
        }
    }

    @Test
    public void testGteDoubles() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.DoubleType),
                new Field("b", ArrowTypes.DoubleType)));

        List<Double> a = Arrays.asList(0.0, 1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN);
        List<Double> b = Arrays.asList(Double.NaN, Double.MAX_VALUE, Double.MIN_VALUE, 1.0, 0.0); // Reversed
                                                                                                  // list
        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);

        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i) >= b.get(i), result.getValue(i));
        }
    }

    @Test
    public void testGteStrings() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.StringType),
                new Field("b", ArrowTypes.StringType)));

        List<String> a = Arrays.asList("aaa", "bbb", "ccc");
        List<String> b = Arrays.asList("aaa", "ccc", "bbb");

        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) a,
                (List<Object>) (List<?>) b);

        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        PhysicalBooleanExpr expr = new PhysicalBooleanExpr.GteExpression(new PhysicalColumnExpr(0),
                new PhysicalColumnExpr(1));
        ColumnVector result = expr.eval(batch);

        assertEquals(a.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(a.get(i).compareTo(b.get(i)) >= 0, result.getValue(i));
        }
    }
}