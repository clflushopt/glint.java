package co.clflushopt.glint.types;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * `ColumnVector` unifies the columnar format used by arrow to allow us to
 * extend it to custom data types.
 */
public interface ColumnVector {
    // Returns the type of the vector's elements.
    ArrowType getType();

    // Returns the value at index i.
    Object getValue(int i);

    // Return the number of elements in the vector.
    int getSize();
}