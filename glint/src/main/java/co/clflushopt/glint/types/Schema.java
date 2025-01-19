package co.clflushopt.glint.types;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema is the metadata used to qualify the data types of a batch of records
 * or a datasoure.
 */
public class Schema {
    private List<Field> fields;

    /**
     * Create a new instance of `Schema` from a list of fields.
     * 
     * @param fields
     */
    public Schema(List<Field> fields) {
        this.fields = fields;
    }

    /**
     * Returns a sub-schema with the fields whose index in the list of input
     * indices.
     * 
     * @param indices
     * @return `Schema`.
     */
    public Schema project(List<Integer> indices) {
        var fields = indices.stream().map(index -> this.fields.get(index)).collect(Collectors.toList());
        return new Schema(fields);
    }

    /**
     * Returns a sub-schema with the fields whose name in the list of input field
     * names.
     * 
     * @param fieldNames
     * @return `Schema`.
     */
    public Schema select(List<String> fieldNames) {
        var fields = this.fields.stream().filter(field -> fieldNames.contains(field.name()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }

    /**
     * Transform an internal Glint schema to an Arrow schema.
     * 
     * @return `Schema` in Arrow format.
     */
    public org.apache.arrow.vector.types.pojo.Schema toArrow() {
        var fields = this.fields.stream().map(field -> field.toArrow()).collect(Collectors.toList());
        var arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);

        return arrowSchema;
    }

    /**
     * Get the schema fields.
     * 
     * @return `List<Field>`.
     */
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Set the schema fields.
     * 
     * @param fields
     */
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
