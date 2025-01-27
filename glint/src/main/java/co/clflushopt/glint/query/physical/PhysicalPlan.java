package co.clflushopt.glint.query.physical;

import java.util.List;

import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * Representation of a physical plans in Glint, this follows closely the
 * vectorized Volcano execution model.
 *
 * PhysicalPlan
 */
public interface PhysicalPlan {

    /**
     * Returns the schema of the tuples returned by the plan.
     *
     */
    public Schema getSchema();

    /**
     * Execute the plan and return the result as a sequence of record batches, this
     * is equivalent to `next()` in the Volcano paper.
     *
     */
    public Iterable<RecordBatch> execute();

    /**
     * Returns the pipeline structure of the plan.
     *
     */
    public List<PhysicalPlan> getChildren();
}
