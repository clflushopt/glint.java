package co.clflushopt.glint.query.plan.logical;

import java.util.List;

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
    public List<LogicalPlan> children();

    /**
     * Return a human friendly view of the logical plan.
     * 
     * @return `String`
     */
    public static String format(LogicalPlan plan, int indent) {
        var sb = new StringBuilder();

        for (int i = 0; i < indent; i++) {
            sb.append(plan.toString()).append("\n");
            plan.children().stream().forEach(child -> sb.append(format(child, indent + 1)));
        }

        return sb.toString();
    }
}
