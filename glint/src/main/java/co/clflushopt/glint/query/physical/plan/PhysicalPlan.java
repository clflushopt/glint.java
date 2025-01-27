package co.clflushopt.glint.query.physical.plan;

import java.util.List;
import java.util.stream.IntStream;

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

    /**
     * Return a human friendly view of the physical plan with default indentation.
     *
     * @param plan
     * @param indent
     * @return
     */
    public static String format(PhysicalPlan plan) {
        return format(plan, 1);
    }

    /**
     * Return a human friendly view of the logical plan.
     *
     * @return `String`
     */
    public static String format(PhysicalPlan plan, int indent) {
        var sb = new StringBuilder();

        IntStream.range(0, indent - 1).forEach(i -> sb.append("\t"));
        sb.append(plan.toString()).append("\n");
        plan.getChildren().stream().forEach(child -> sb.append(format(child, indent + 1)));

        return sb.toString();
    }
}
