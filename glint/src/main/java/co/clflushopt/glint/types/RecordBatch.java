package co.clflushopt.glint.types;

import java.util.List;

public class RecordBatch {
    private Schema schema;
    private List<ColumnVector> fields;

    public RecordBatch(Schema schema, List<ColumnVector> fields) {
        this.schema = schema;
        this.fields = fields;
    }

    public ColumnVector getField(int i) {
        return fields.get(i);
    }

    // Returns the row size of the batch.
    public int getRowSize() {
        return fields.getFirst().getSize();
    }

    // Returns the column size of the batch.
    public int getColumnSize() {
        return fields.size();
    }

}
