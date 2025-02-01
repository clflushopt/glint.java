package co.clflushopt.glint.query.physical.plan;

import java.util.Iterator;
import java.util.List;

import co.clflushopt.glint.datasource.DataSource;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * Implementation of the Scan physical operator with support for predicate
 * pushdown.
 *
 */
public class ScanOperator implements PhysicalPlan {
    private DataSource dataSource;
    private List<String> projection;

    /**
     * Creates a new Scan operator.
     *
     */
    public ScanOperator(DataSource dataSource, List<String> projection) {
        this.dataSource = dataSource;
        this.projection = projection;
    }

    @Override
    public Schema getSchema() {
        return dataSource.getSchema();
    }

    @Override
    public Iterator<RecordBatch> execute() {
        return dataSource.scan(projection).iterator();
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        return List.of();
    }
}
