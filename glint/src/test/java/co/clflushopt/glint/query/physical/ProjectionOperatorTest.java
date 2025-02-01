package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.physical.expr.PhysicalColumnExpr;
import co.clflushopt.glint.query.physical.expr.PhysicalExpr;
import co.clflushopt.glint.query.physical.plan.ProjectionOperator;
import co.clflushopt.glint.query.physical.plan.ScanOperator;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class ProjectionOperatorTest {
    @Test
    public void testProjection() {
        // Create test data
        Schema schema = new Schema(TestUtils.createTestSchema());
        RecordBatch testBatch = TestUtils.createTestBatch();
        SyntheticDataSource dataSource = new SyntheticDataSource(schema,
                Collections.singletonList(testBatch));
        ScanOperator scan = new ScanOperator(dataSource, Collections.emptyList());

        // Project only name and age
        List<PhysicalExpr> projections = Arrays.asList(new PhysicalColumnExpr(1), // name
                new PhysicalColumnExpr(2) // age
        );

        ProjectionOperator projection = new ProjectionOperator(scan, schema, projections);
        Iterator<RecordBatch> result = projection.execute();

        // Verify results
        assertTrue(result.hasNext());
        RecordBatch batch = result.next();
        assertEquals(3, batch.getSchema().getFields().size());
        assertEquals("name", batch.getSchema().getFields().get(1).name());
        assertEquals("age", batch.getSchema().getFields().get(2).name());
        assertEquals("Alice", batch.getField(0).getValue(0));
        assertEquals(25, batch.getField(1).getValue(0));
    }
}