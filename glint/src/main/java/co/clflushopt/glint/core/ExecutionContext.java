package co.clflushopt.glint.core;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.dataframe.DataFrameImpl;
import co.clflushopt.glint.datasource.CsvDataSource;
import co.clflushopt.glint.datasource.DataSource;
import co.clflushopt.glint.datasource.ParquetDataSource;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.query.optimizer.QueryOptimizer;
import co.clflushopt.glint.query.planner.QueryPlanner;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class ExecutionContext {
    private final HashMap<String, Object> context;
    private final Configuration config;
    private final Integer defaultBatchSize = 1024;

    // Private constructor - use builder instead
    private ExecutionContext(HashMap<String, Object> context, Configuration configuration) {
        this.context = context;
        this.config = configuration;
    }

    public Iterator<RecordBatch> execute(LogicalPlan plan) {
        // Implementation to execute logical plan
        var optimizedPlan = QueryOptimizer.optimize(plan);
        var physicalPlan = QueryPlanner.createPhysicalPlan(optimizedPlan);
        return physicalPlan.execute();
    }

    /**
     * Configuration class that encapsulates all execution settings. Uses builder
     * pattern for a clean configuration API.
     */
    public static class Configuration {
        private int batchSize = 1024; // Default batch size
        private boolean enableCache = true;

        public Configuration setBatchSize(int size) {
            this.batchSize = size;
            return this;
        }

        public Configuration setEnableCache(boolean enable) {
            this.enableCache = enable;
            return this;
        }
    }

    /**
     * Creates a new builder for ExecutionContext. This is the primary way to create
     * and configure a context.
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * Builder class for constructing an ExecutionContext with specific settings.
     */
    public static class Builder {
        private Configuration config = new Configuration();

        public Builder withConfiguration(Configuration config) {
            this.config = config;
            return this;
        }

        public ExecutionContext build() {
            return new ExecutionContext(new HashMap<>(), config);
        }
    }

    /**
     * Creates a DataFrame from a CSV file with the specified options.
     */
    public DataFrame readCsv(String path, Optional<Schema> schema, CsvReaderOptions options)
            throws FileNotFoundException {
        var source = new CsvDataSource(path, schema, options.hasHeader(), defaultBatchSize);
        return new DataFrameImpl(new Scan(options.getTableName(), source, Collections.emptyList()));
    }

    /**
     * Creates a DataFrame from a CSV file with default options.
     *
     *
     * @param name
     * @param df
     */
    public DataFrame readCsv(String path, Schema schema, CsvReaderOptions options)
            throws FileNotFoundException {
        var source = new CsvDataSource(path, Optional.of(schema), options.hasHeader(),
                defaultBatchSize);
        return new DataFrameImpl(new Scan(options.getTableName(), source, Collections.emptyList()));
    }

    /**
     * Creates a DataFrame from a Parquet file with the specified options.
     *
     *
     * @param name
     * @param df
     */
    public DataFrame readParquet(String path, Optional<Schema> schema) {
        DataSource source;
        if (schema.isPresent()) {
            source = new ParquetDataSource(path, schema.get());
        } else {
            source = new ParquetDataSource(path);
        }
        return new DataFrameImpl(new Scan("parquet_scan", source, Collections.emptyList()));
    }

    /**
     * Creates a temporary table from a DataFrame for use in subsequent queries.
     */
    public void createTempTable(String name, DataFrame df) {
        // Implementation to register temporary table
    }
}