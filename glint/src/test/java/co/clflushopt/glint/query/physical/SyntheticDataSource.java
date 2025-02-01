package co.clflushopt.glint.query.physical;

import java.util.List;

import co.clflushopt.glint.datasource.DataSource;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

// Test implementation of DataSource interface
public class SyntheticDataSource implements DataSource {
    private final Schema schema;
    private final List<RecordBatch> batches;

    public SyntheticDataSource(Schema schema, List<RecordBatch> batches) {
        this.schema = schema;
        this.batches = batches;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        // If projection is empty, return all columns
        if (projection.isEmpty()) {
            return batches;
        }

        return batches;
    }

}