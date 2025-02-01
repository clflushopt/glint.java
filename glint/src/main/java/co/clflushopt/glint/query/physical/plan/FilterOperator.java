package co.clflushopt.glint.query.physical.plan;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;

import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.ArrowVectorBuilder;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class FilterOperator implements PhysicalPlan {
    private final PhysicalPlan input;
    private final Expr expr; // Assuming you have an Expression interface

    public FilterOperator(PhysicalPlan input, Expr expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Iterator<RecordBatch> execute() {
        // Convert Sequence to Iterator/Iterable
        Iterator<RecordBatch> inputIterator = input.execute();

        // Return a new Iterator that applies the filter
        return new Iterator<RecordBatch>() {
            @Override
            public boolean hasNext() {
                return inputIterator.hasNext();
            }

            @Override
            public RecordBatch next() {
                RecordBatch batch = inputIterator.next();
                BitVector result = (BitVector) ((ArrowFieldVector) expr.eval(batch)).getField();
                Schema schema = batch.getSchema();
                int columnCount = schema.getFields().size();

                // Filter each field
                List<FieldVector> filteredFields = IntStream.range(0, columnCount)
                        .mapToObj(i -> filter(batch.getField(i), result))
                        .collect(Collectors.toList());

                // Convert to ArrowFieldVectors
                List<ColumnVector> fields = filteredFields.stream().map(ArrowFieldVector::new)
                        .collect(Collectors.toList());

                return new RecordBatch(schema, fields);
            }
        };
    }

    private FieldVector filter(ColumnVector v, BitVector selection) {
        VarCharVector filteredVector = new VarCharVector("v", new RootAllocator(Long.MAX_VALUE));
        filteredVector.allocateNew();

        ArrowVectorBuilder builder = new ArrowVectorBuilder(filteredVector);

        int count = 0;
        for (int i = 0; i < selection.getValueCount(); i++) {
            if (selection.get(i) == 1) {
                builder.setValue(count, v.getValue(i));
                count++;
            }
        }
        filteredVector.setValueCount(count);
        return filteredVector;
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
