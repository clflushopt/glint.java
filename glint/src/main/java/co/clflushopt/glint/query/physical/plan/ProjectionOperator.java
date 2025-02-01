package co.clflushopt.glint.query.physical.plan;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.types.ColumnVector;
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
    public Iterator<RecordBatch> execute() {
        Iterator<RecordBatch> inputIterator = input.execute();

        return new Iterator<RecordBatch>() {
            @Override
            public boolean hasNext() {
                return inputIterator.hasNext();
            }

            @Override
            public RecordBatch next() {
                RecordBatch batch = inputIterator.next();
                List<ColumnVector> columns = projections.stream()
                        .map(expression -> expression.eval(batch)).collect(Collectors.toList());
                return new RecordBatch(schema, columns);
            }
        };
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
