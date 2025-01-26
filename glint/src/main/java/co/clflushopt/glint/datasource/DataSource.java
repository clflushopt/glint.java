package co.clflushopt.glint.datasource;

import java.util.List;

import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * The data source abstraction is how the query engine consumes and processes
 * records we currently implement three different data sources Csv, Parquet and
 * In-Memory batches but there are no limitations to the underlying source;
 * further formats may added. DataSource
 */
public interface DataSource {

    /**
     * The schema associated with the data source.
     *
     * @return `Schema` of the data source; either inferred or user provided.
     */
    public Schema getSchema();

    /**
     * Returns record batches from the data source with only the specified projected
     * columns this is effectively how we handle column projection predicate
     * push-down.
     *
     * @param projection
     * @return `RecordBatch` with only requested projections.
     */
    public Iterable<RecordBatch> scan(List<String> projection);
}
