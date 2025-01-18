package co.clflushopt.glint.datasource;

import java.util.List;

import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public interface DataSource {
    // Returns the data source's schema.
    public Schema getSchema();

    public List<RecordBatch> scan(List<String> projection);
}
