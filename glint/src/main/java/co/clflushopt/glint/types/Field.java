package co.clflushopt.glint.types;

import java.util.ArrayList;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Field is a metadata type used to qualify columns in the columnar format with
 * a name and data type.
 */
public record Field(String name, ArrowType dataType) {

    /**
     * Transform an internal `Field` type to Arrow Field type.
     *
     * @return
     */
    public org.apache.arrow.vector.types.pojo.Field toArrow() {
        var fieldType = new org.apache.arrow.vector.types.pojo.FieldType(true, dataType, null);
        var children = new ArrayList<org.apache.arrow.vector.types.pojo.Field>();
        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, children);
    }

    @Override
    public String toString() {
        return String.format("(name: %s, type: %s)", name, dataType.toString());
    }
}
