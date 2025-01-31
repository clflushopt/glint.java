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

/**
 * Builder for Arrow vectors.
 *
 * ArrowVectorBuilder
 */
public class ArrowVectorBuilder {
    private FieldVector vector;

    public ArrowVectorBuilder(FieldVector vector) {
        this.vector = vector;
    }

    public void setValueCount(int count) {
        vector.setValueCount(count);
    }

    public ColumnVector build() {
        return new ArrowFieldVector(vector);
    }

    public void setValue(int index, Object value) {
        if (value == null) {
            this.setNull(index);
            return;
        }

        if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof VarCharVector) {
            if (value instanceof byte[]) {
                ((VarCharVector) vector).setSafe(index, (byte[]) value);
            } else {
                ((VarCharVector) vector).setSafe(index, value.toString().getBytes());
            }
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
        } else if (vector instanceof BitVector) {
            if (value instanceof Boolean) {
                ((BitVector) vector).setSafe(index, (Boolean) value ? 1 : 0);
            } else if (value instanceof Number) {
                ((BitVector) vector).setSafe(index, ((Number) value).intValue() != 0 ? 1 : 0);
            }
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
        }
    }

    private void setNull(int index) {
        if (vector instanceof IntVector) {
            ((IntVector) vector).setNull(index);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setNull(index);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setNull(index);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setNull(index);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setNull(index);
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setNull(index);
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setNull(index);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setNull(index);
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
        }
    }
}
