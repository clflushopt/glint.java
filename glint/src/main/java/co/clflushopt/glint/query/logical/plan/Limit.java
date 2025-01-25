package co.clflushopt.glint.query.logical.plan;

import java.util.List;

import co.clflushopt.glint.types.Schema;

/**
 * Equivalent to the `LIMIT` operator in SQL and caps the number of tuples.
 *
 * Limit
 */
public class Limit implements LogicalPlan {
    private LogicalPlan input;
    private Integer limit;

    public Limit(LogicalPlan input, Integer limit) {
        this.input = input;
        this.limit = limit;
    }

    @Override
    public Schema getSchema() {
        return input.getSchema();
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(input);
    }

    @Override
    public String toString() {
        return "Limit: " + limit.toString();
    }

}
