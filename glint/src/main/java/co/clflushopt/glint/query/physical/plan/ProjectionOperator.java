package co.clflushopt.glint.query.physical.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * ProjectionOperator is a physical operator that projects a subset of columns
 * from the input.
 *
 */
public class ProjectionOperator implements PhysicalPlan {
    private PhysicalPlan input;
    private List<Expr> projections;
    private Schema schema;

    /**
     * Create a new ProjectionOperator.
     *
     * @param input      the input operator.
     * @param projection the columns to project.
     */
    public ProjectionOperator(PhysicalPlan input, Schema schema, List<Expr> projections) {
        this.input = input;
        this.projections = projections;
        this.schema = schema;
    }

    @Override
    public Iterable<RecordBatch> execute() {
        var iter = input.execute().iterator();
        List<RecordBatch> result = new ArrayList<>();

        while (iter.hasNext()) {
            var columns = this.projections.stream().map(expr -> expr.eval(iter.next()))
                    .collect(Collectors.toList());
            result.add(new RecordBatch(schema, columns));
        }

        return result;
    }

    @Override
    public Schema getSchema() {
        return input.getSchema();
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        return List.of(input);
    }

}
