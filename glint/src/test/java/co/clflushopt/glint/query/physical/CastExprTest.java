package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.physical.expr.CastExpr;
import co.clflushopt.glint.query.physical.expr.ColumnExpr;
import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;
import co.clflushopt.glint.util.Fuzzer;

public class CastExprTest {

    @Test
    public void testCastByteToString() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.Int8Type)));

        List<Byte> values = Arrays.asList((byte) 10, (byte) 20, (byte) 30, Byte.MIN_VALUE,
                Byte.MAX_VALUE);

        @SuppressWarnings("unchecked")
        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) values);
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        Expr expr = new CastExpr(new ColumnExpr(0), ArrowTypes.StringType);
        ColumnVector result = expr.eval(batch);

        assertEquals(values.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(values.get(i).toString(), result.getValue(i));
        }
    }

    @Test
    public void testCastStringToFloat() {
        Schema schema = new Schema(Arrays.asList(new Field("a", ArrowTypes.StringType)));

        List<String> values = Arrays.asList(String.valueOf(Float.MIN_VALUE),
                String.valueOf(Float.MAX_VALUE));

        @SuppressWarnings("unchecked")
        List<List<Object>> data = Arrays.asList((List<Object>) (List<?>) values);
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, data);

        Expr expr = new CastExpr(new ColumnExpr(0), ArrowTypes.FloatType);
        ColumnVector result = expr.eval(batch);

        assertEquals(values.size(), result.getSize());
        for (int i = 0; i < result.getSize(); i++) {
            assertEquals(Float.parseFloat(values.get(i)), result.getValue(i));
        }
    }
}