package co.clflushopt.glint.datasource;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * In-memory data source; designed mostly for testing operator logic.
 */
public class MemoryDataSource implements DataSource {
    private Schema schema;
    private List<RecordBatch> records;

    public MemoryDataSource(Schema schema, List<RecordBatch> records) {
        this.schema = schema;
        this.records = records;
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public List<RecordBatch> scan(List<String> projection) {
        var projectionIndices = projection.stream()
                .map(name -> Iterables.indexOf(schema.getFields(), field -> field.name() == name))
                .collect(Collectors.toList());

        return records.stream()
                .map(batch -> new RecordBatch(schema, projectionIndices.stream()
                        .map(index -> batch.getField(index)).collect(Collectors.toList())))
                .toList();
    }
}
