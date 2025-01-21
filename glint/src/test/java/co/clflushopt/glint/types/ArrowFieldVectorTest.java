package co.clflushopt.glint.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

public class ArrowFieldVectorTest {
    @Test
    public void testBooleanVector() {
        BitVector vector = (BitVector) FieldVectorFactory.create(ArrowTypes.BooleanType, 2);
        vector.setSafe(0, 1); // true
        vector.setSafe(1, 0); // false
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.BooleanType);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.BooleanType, fieldVector.getType());
        assertEquals(true, fieldVector.getValue(0));
        assertEquals(false, fieldVector.getValue(1));
    }

    @Test
    public void testInt8Vector() {
        TinyIntVector vector = (TinyIntVector) FieldVectorFactory.create(ArrowTypes.Int8Type, 2);
        vector.setSafe(0, 42);
        vector.setSafe(1, -42);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.Int8Type);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.Int8Type, fieldVector.getType());
        assertEquals((byte) 42, fieldVector.getValue(0));
        assertEquals((byte) -42, fieldVector.getValue(1));
    }

    @Test
    public void testInt16Vector() {
        SmallIntVector vector = (SmallIntVector) FieldVectorFactory.create(ArrowTypes.Int16Type, 2);
        vector.setSafe(0, 12345);
        vector.setSafe(1, -12345);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.Int16Type);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.Int16Type, fieldVector.getType());
        assertEquals((short) 12345, fieldVector.getValue(0));
        assertEquals((short) -12345, fieldVector.getValue(1));
    }

    @Test
    public void testInt32Vector() {
        IntVector vector = (IntVector) FieldVectorFactory.create(ArrowTypes.Int32Type, 2);
        vector.setSafe(0, 1234567);
        vector.setSafe(1, -1234567);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.Int32Type);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.Int32Type, fieldVector.getType());
        assertEquals(1234567, fieldVector.getValue(0));
        assertEquals(-1234567, fieldVector.getValue(1));
    }

    @Test
    public void testInt64Vector() {
        BigIntVector vector = (BigIntVector) FieldVectorFactory.create(ArrowTypes.Int64Type, 2);
        vector.setSafe(0, 123456789L);
        vector.setSafe(1, -123456789L);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.Int64Type);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.Int64Type, fieldVector.getType());
        assertEquals(123456789L, fieldVector.getValue(0));
        assertEquals(-123456789L, fieldVector.getValue(1));
    }

    @Test
    public void testFloatVector() {
        Float4Vector vector = (Float4Vector) FieldVectorFactory.create(ArrowTypes.FloatType, 2);
        vector.setSafe(0, 3.14f);
        vector.setSafe(1, -3.14f);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.FloatType);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.FloatType, fieldVector.getType());
        assertEquals(3.14f, (Float) fieldVector.getValue(0), 0.0001);
        assertEquals(-3.14f, (Float) fieldVector.getValue(1), 0.0001);
    }

    @Test
    public void testDoubleVector() {
        Float8Vector vector = (Float8Vector) FieldVectorFactory.create(ArrowTypes.DoubleType, 2);
        vector.setSafe(0, 3.14159);
        vector.setSafe(1, -3.14159);
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.DoubleType);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.DoubleType, fieldVector.getType());
        assertEquals(3.14159, (Double) fieldVector.getValue(0), 0.0001);
        assertEquals(-3.14159, (Double) fieldVector.getValue(1), 0.0001);
    }

    @Test
    public void testStringVector() {
        VarCharVector vector = (VarCharVector) FieldVectorFactory.create(ArrowTypes.StringType, 2);
        vector.setSafe(0, "hello".getBytes());
        vector.setSafe(1, "world".getBytes());
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertTrue(fieldVector.getType() == ArrowTypes.StringType);
        assertEquals(2, fieldVector.getSize());
        assertEquals(ArrowTypes.StringType, fieldVector.getType());
        assertEquals("hello", fieldVector.getValue(0));
        assertEquals("world", fieldVector.getValue(1));
    }

    @Test
    public void testNullValues() {
        VarCharVector vector = (VarCharVector) FieldVectorFactory.create(ArrowTypes.StringType, 2);
        vector.setNull(0);
        vector.setSafe(1, "not null".getBytes());
        vector.setValueCount(2);

        ArrowFieldVector fieldVector = new ArrowFieldVector(vector);

        assertEquals(2, fieldVector.getSize());
        assertNull(fieldVector.getValue(0));
        assertEquals("not null", fieldVector.getValue(1));
    }
}
