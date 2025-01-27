package co.clflushopt.glint.query.logical.plan;

import java.util.List;
import java.util.stream.IntStream;

import co.clflushopt.glint.types.Schema;

/**
 *
 * LogicalPlan is an interface representing a relational operator pipeline that
 * acts on tuples.
 */
public interface LogicalPlan {

    /**
     * Returns the schema of the produced tuples.
     *
     * @return `Schema`
     */
    public Schema getSchema();

    /**
     * Returns a list of plans that will be consumed by this plan.
     *
     * @return `List<LogicalPlan>`
     */
    public List<LogicalPlan> getChildren();

    /**
     * Return a human friendly view of the logical plan with default indentation.
     *
     * @param plan
     * @param indent
     * @return
     */
    public static String format(LogicalPlan plan) {
        return format(plan, 1);
    }

    /**
     * Return a human friendly view of the logical plan.
     *
     * @return `String`
     */
    public static String format(LogicalPlan plan, int indent) {
        var sb = new StringBuilder();

        IntStream.range(0, indent - 1).forEach(i -> sb.append("\t"));
        sb.append(plan.toString()).append("\n");
        plan.getChildren().stream().forEach(child -> sb.append(format(child, indent + 1)));

        return sb.toString();
    }
}
