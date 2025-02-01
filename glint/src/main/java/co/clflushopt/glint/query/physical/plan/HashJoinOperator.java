package co.clflushopt.glint.query.physical.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import co.clflushopt.glint.query.functional.Accumulator;
import co.clflushopt.glint.query.physical.expr.AggregateExpr;
import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.ArrowVectorBuilder;
import co.clflushopt.glint.types.ColumnVector;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * HashJoinOperator implements the Hash Aggregate Join algorithm where the input
 * is consumed from two sources and the join is performed in two phases: build
 * and probe.
 *
 * The build phase consumes the left input and builds a hash table using the
 * join key. The probe phase consumes the right input and probes the hash table
 * to find matching rows.
 *
 */
public class HashJoinOperator implements PhysicalPlan {
    private PhysicalPlan input;
    private List<Expr> groupByExpr;
    private List<AggregateExpr> aggregateExpr;
    private Schema schema;

    /**
     * Create a new HashJoinOperator.
     *
     * @param left     the left input operator.
     * @param right    the right input operator.
     * @param leftKey  the join key for the left input.
     * @param rightKey the join key for the right input.
     */
    public HashJoinOperator(PhysicalPlan input, List<Expr> groupByExpr,
            List<AggregateExpr> aggregateExpr, Schema schema) {
        this.input = input;
        this.groupByExpr = groupByExpr;
        this.aggregateExpr = aggregateExpr;
        this.schema = schema;
    }

    @Override
    public Iterator<RecordBatch> execute() {
        // Map to store grouping keys and their accumulators
        Map<List<Object>, List<Accumulator>> map = new HashMap<>();

        // Process each batch from input
        Iterator<RecordBatch> inputIter = input.execute();
        while (inputIter.hasNext()) {
            RecordBatch batch = inputIter.next();

            // Evaluate grouping expressions
            List<ColumnVector> groupKeys = groupByExpr.stream().map(expr -> expr.eval(batch))
                    .collect(Collectors.toList());

            // Evaluate aggregate input expressions
            List<ColumnVector> aggrInputValues = aggregateExpr.stream()
                    .map(expr -> expr.getInputExpr().eval(batch)).collect(Collectors.toList());

            // Process each row in the batch
            for (int rowIndex = 0; rowIndex < batch.getRowSize(); rowIndex++) {
                // Create final variable for lambda.
                final int currentRow = rowIndex;
                // Create key for hash map
                List<Object> rowKey = groupKeys.stream().map(key -> {
                    Object value = key.getValue(currentRow);
                    if (value instanceof byte[]) {
                        return new String((byte[]) value);
                    }
                    return value;
                }).collect(Collectors.toList());

                // Get or create accumulators for this grouping key
                List<Accumulator> accumulators = map.computeIfAbsent(rowKey, k -> aggregateExpr
                        .stream().map(acc -> acc.getAccumulator()).collect(Collectors.toList()));

                // Perform accumulation
                for (int i = 0; i < accumulators.size(); i++) {
                    Object value = aggrInputValues.get(i).getValue(rowIndex);
                    accumulators.get(i).accumulate(value);
                }
            }
        }

        // Create result batch with final aggregate values
        VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrow(),
                new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();
        root.setRowCount(map.size());

        List<ArrowVectorBuilder> builders = root.getFieldVectors().stream()
                .map(ArrowVectorBuilder::new).collect(Collectors.toList());

        int rowIndex = 0;
        for (Map.Entry<List<Object>, List<Accumulator>> entry : map.entrySet()) {
            List<Object> groupingKey = entry.getKey();
            List<Accumulator> accumulators = entry.getValue();

            // Set grouping key values
            for (int i = 0; i < groupByExpr.size(); i++) {
                builders.get(i).setValue(rowIndex, groupingKey.get(i));
            }

            // Set aggregate values
            for (int i = 0; i < aggregateExpr.size(); i++) {
                builders.get(groupByExpr.size() + i).setValue(rowIndex,
                        accumulators.get(i).getResult());
            }
            rowIndex++;
        }

        RecordBatch outputBatch = new RecordBatch(schema, root.getFieldVectors().stream()
                .map(ArrowFieldVector::new).collect(Collectors.toList()));

        return Collections.singletonList(outputBatch).iterator();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> getChildren() {
        return List.of(input);
    }

}
