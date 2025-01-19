package co.clflushopt.glint.types;

import org.apache.arrow.memory.RootAllocator;
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

public final class FieldVectorFactory {
    private FieldVectorFactory() {
    } // Prevent instantiation

    public static FieldVector create(ArrowType arrowType, int initialCapacity) {
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        FieldVector fieldVector;

        if (arrowType.equals(ArrowTypes.BooleanType)) {
            fieldVector = new BitVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int8Type)) {
            fieldVector = new TinyIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int16Type)) {
            fieldVector = new SmallIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int32Type)) {
            fieldVector = new IntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.Int64Type)) {
            fieldVector = new BigIntVector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.FloatType)) {
            fieldVector = new Float4Vector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.DoubleType)) {
            fieldVector = new Float8Vector("v", rootAllocator);
        } else if (arrowType.equals(ArrowTypes.StringType)) {
            fieldVector = new VarCharVector("v", rootAllocator);
        } else {
            throw new IllegalStateException("Unsupported Arrow type: " + arrowType);
        }

        if (initialCapacity != 0) {
            fieldVector.setInitialCapacity(initialCapacity);
        }
        fieldVector.allocateNew();
        return fieldVector;
    }
}