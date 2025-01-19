package co.clflushopt.glint.types;

import java.util.List;

/**
 * Batch of records in columnar format.
 */
public class RecordBatch {
    private Schema schema;
    private List<ColumnVector> fields;

    /**
     * Creates a new batch of records for a list of `ColumnVector` representing the
     * fields (in columnar format) and matching the given schema.
     * 
     * @param schema
     * @param fields
     */
    public RecordBatch(Schema schema, List<ColumnVector> fields) {
        this.schema = schema;
        this.fields = fields;
    }

    /**
     * Returns a reference to the ColumnVector at a given index.
     * 
     * @param i
     * @return `ColumnVector`
     */
    public ColumnVector getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the number of rows in the batch.
     * 
     * @return `int`.
     */
    public int getRowSize() {
        return fields.getFirst().getSize();
    }

    /**
     * Returns the number of columns in the batch.
     * 
     * @return `int..
     */
    public int getColumnSize() {
        return fields.size();
    }

}
