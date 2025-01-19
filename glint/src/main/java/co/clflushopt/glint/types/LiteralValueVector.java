package co.clflushopt.glint.types;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Implementation of `ColumnVector` for scalar (non-compound) values.
 */
public class LiteralValueVector implements ColumnVector {
    private ArrowType type;
    private int size;
    private Object value;

    /**
     * Construct a new scalar that can be represented as `ColumnVector`.
     * 
     * @param arrowType
     * @param value
     * @param size
     */
    public LiteralValueVector(ArrowType arrowType, Object value, int size) {
        this.type = arrowType;
        this.size = size;
        this.value = value;
    }

    /**
     * Returns the scalar's data type.
     */
    @Override
    public ArrowType getType() {
        return this.type;
    }

    /**
     * Returns the scalar's value.
     */
    @Override
    public Object getValue(int i) {
        if (i < 0 || i >= this.size) {
            throw new IndexOutOfBoundsException();
        }
        return this.value;
    }

    /**
     * Returns the scalar's size.
     */
    @Override
    public int getSize() {
        return this.size;
    }

}
