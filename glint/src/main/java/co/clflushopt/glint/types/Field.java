package co.clflushopt.glint.types;

import java.util.ArrayList;

import org.apache.arrow.vector.types.pojo.ArrowType;

public record Field(String name, ArrowType dataType) {
    public org.apache.arrow.vector.types.pojo.Field toArrow() {
        var fieldType = new org.apache.arrow.vector.types.pojo.FieldType(true, dataType, null);
        var children = new ArrayList<org.apache.arrow.vector.types.pojo.Field>();
        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, children);
    }
}
