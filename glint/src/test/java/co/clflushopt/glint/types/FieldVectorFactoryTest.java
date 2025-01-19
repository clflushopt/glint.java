package co.clflushopt.glint.types;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

public class FieldVectorFactoryTest {

    @Test
    public void testCreateWithZeroCapacity() {
        FieldVector vector = FieldVectorFactory.create(ArrowTypes.Int32Type, 0);
        assertNotNull(vector);
        assertTrue(vector instanceof IntVector);
    }

    @Test
    public void testCreateWithNonZeroCapacity() {
        FieldVector vector = FieldVectorFactory.create(ArrowTypes.Int32Type, 100);
        assertNotNull(vector);
        assertTrue(vector instanceof IntVector);
    }

    @Test
    public void testCreateAllTypes() {
        // Test creation of each supported type
        assertTrue(FieldVectorFactory.create(ArrowTypes.BooleanType, 1) instanceof BitVector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.Int8Type, 1) instanceof TinyIntVector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.Int16Type, 1) instanceof SmallIntVector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.Int32Type, 1) instanceof IntVector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.Int64Type, 1) instanceof BigIntVector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.FloatType, 1) instanceof Float4Vector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.DoubleType, 1) instanceof Float8Vector);
        assertTrue(FieldVectorFactory.create(ArrowTypes.StringType, 1) instanceof VarCharVector);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsupportedType() {
        // Allow deprecated here for tests.
        ArrowType unsupportedType = new ArrowType.Decimal(10, 2);
        FieldVectorFactory.create(unsupportedType, 1);

    }
}
