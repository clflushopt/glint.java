package co.clflushopt.glint.query.logical.plan;

import java.util.List;

import co.clflushopt.glint.datasource.DataSource;
import co.clflushopt.glint.types.Schema;

/**
 * The Scan logical plan is the operator responsible for reading tuples with a
 * projection and is considered a leaf node in the query dag.
 *
 * Scan
 */
public class Scan implements LogicalPlan {
    private String path;
    private DataSource dataSource;
    private List<String> projections;
    private Schema schema;

    public Scan(String path, DataSource dataSource, List<String> projections) {
        this.path = path;
        this.dataSource = dataSource;
        this.projections = projections;
        this.schema = infer();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public List<String> getProjections() {
        return projections;
    }

    public String getPath() {
        return path;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return List.of();
    }

    private Schema infer() {
        var schema = this.dataSource.getSchema();
        assert schema != null;
        assert schema.getFields().size() > 0;

        if (projections.isEmpty()) {
            return schema;
        }

        return schema.select(projections);
    }

    @Override
    public String toString() {
        if (projections.isEmpty()) {
            return String.format("Scan: %s [projection=None]", path);
        }

        return String.format("Scan:%s [projection=(%s)]", path, String.join(", ", projections));
    }
}
