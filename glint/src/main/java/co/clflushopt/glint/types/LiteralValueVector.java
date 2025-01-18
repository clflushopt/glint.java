package co.clflushopt.glint.types;

import org.apache.arrow.vector.types.pojo.ArrowType;

// LiteralValueVector represents a scalar value that implements ColumnVector.
public class LiteralValueVector implements ColumnVector {
    private ArrowType type;
    private int size;
    private Object value;

    public LiteralValueVector(ArrowType arrowType, Object value, int size) {
        this.type = arrowType;
        this.size = size;
        this.value = value;
    }

    @Override
    public ArrowType getType() {
        return this.type;
    }

    @Override
    public Object getValue(int i) {
        if (i < 0 || i >= this.size) {
            throw new IndexOutOfBoundsException();
        }
        return this.value;
    }

    @Override
    public int getSize() {
        return this.size;
    }

}
