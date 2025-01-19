package co.clflushopt.glint.types;

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

/**
 * Wrapper around Arrow FieldVector
 */
public class ArrowFieldVector implements ColumnVector {
    private final FieldVector field;

    public ArrowFieldVector(FieldVector field) {
        this.field = field;
    }

    @Override
    public ArrowType getType() {
        if (field instanceof BitVector) {
            return ArrowTypes.BooleanType;
        } else if (field instanceof TinyIntVector) {
            return ArrowTypes.Int8Type;
        } else if (field instanceof SmallIntVector) {
            return ArrowTypes.Int16Type;
        } else if (field instanceof IntVector) {
            return ArrowTypes.Int32Type;
        } else if (field instanceof BigIntVector) {
            return ArrowTypes.Int64Type;
        } else if (field instanceof Float4Vector) {
            return ArrowTypes.FloatType;
        } else if (field instanceof Float8Vector) {
            return ArrowTypes.DoubleType;
        } else if (field instanceof VarCharVector) {
            return ArrowTypes.StringType;
        } else {
            throw new IllegalStateException("Unsupported field vector type: " + field.getClass());
        }
    }

    @Override
    public Object getValue(int i) {
        if (field.isNull(i)) {
            return null;
        }

        if (field instanceof BitVector) {
            return ((BitVector) field).get(i) == 1;
        } else if (field instanceof TinyIntVector) {
            return ((TinyIntVector) field).get(i);
        } else if (field instanceof SmallIntVector) {
            return ((SmallIntVector) field).get(i);
        } else if (field instanceof IntVector) {
            return ((IntVector) field).get(i);
        } else if (field instanceof BigIntVector) {
            return ((BigIntVector) field).get(i);
        } else if (field instanceof Float4Vector) {
            return ((Float4Vector) field).get(i);
        } else if (field instanceof Float8Vector) {
            return ((Float8Vector) field).get(i);
        } else if (field instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) field).get(i);
            return bytes == null ? null : new String(bytes);
        } else {
            throw new IllegalStateException("Unsupported field vector type: " + field.getClass());
        }
    }

    @Override
    public int getSize() {
        return field.getValueCount();
    }

    public FieldVector getField() {
        return field;
    }
}