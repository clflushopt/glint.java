package co.clflushopt.glint.types;

import java.util.List;
import java.util.stream.Collectors;

public class Schema {
    private List<Field> fields;

    public Schema(List<Field> fields) {
        this.fields = fields;
    }

    // Build a schema from a projection.
    public Schema project(List<Integer> indices) {
        var fields = indices.stream().map(index -> this.fields.get(index)).collect(Collectors.toList());
        return new Schema(fields);
    }

    // Build a schema from a projection (names).
    public Schema select(List<String> fieldNames) {
        return new Schema(
                this.fields.stream().filter(field -> fieldNames.contains(field.name()))
                        .collect(Collectors.toList()));
    }

    public org.apache.arrow.vector.types.pojo.Schema toArrow() {
        var fields = this.fields.stream().map(field -> field.toArrow()).collect(Collectors.toList());
        var arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);

        return arrowSchema;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
