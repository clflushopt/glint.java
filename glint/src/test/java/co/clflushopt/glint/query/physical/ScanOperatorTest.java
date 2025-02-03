package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.query.physical.plan.ScanOperator;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

// Now the actual test
public class ScanOperatorTest {

    @Test
    public void testScanAllColumns() {
        // Create test data
        Schema schema = new Schema(TestUtils.createTestSchema());
        RecordBatch testBatch = TestUtils.createTestBatch();
        SyntheticDataSource dataSource = new SyntheticDataSource(schema,
                Collections.singletonList(testBatch));

        // Create scan operator
        ScanOperator scan = new ScanOperator(dataSource, Collections.emptyList());
        Iterator<RecordBatch> result = scan.execute();

        // Verify results
        assertTrue(result.hasNext());
        RecordBatch batch = result.next();
        assertEquals(3, batch.getSchema().getFields().size());
        assertEquals(3, batch.getRowCount());
        assertEquals("Alice", batch.getField(1).getValue(0));
        assertFalse(result.hasNext());
    }

    @Test
    public void testScanWithProjection() {
        // Create test data
        Schema schema = new Schema(TestUtils.createTestSchema());
        RecordBatch testBatch = TestUtils.createTestBatch();
        SyntheticDataSource dataSource = new SyntheticDataSource(schema,
                Collections.singletonList(testBatch));

        // Create scan operator with projection
        List<String> projection = Arrays.asList("name", "age");
        ScanOperator scan = new ScanOperator(dataSource, projection);
        Iterator<RecordBatch> result = scan.execute();

        // Verify results
        assertTrue(result.hasNext());
        RecordBatch batch = result.next();
        assertEquals(3, batch.getSchema().getFields().size());
        assertEquals("id", batch.getSchema().getFields().get(0).name());
        assertEquals("name", batch.getSchema().getFields().get(1).name());
        assertEquals(3, batch.getRowCount());
        assertEquals(Integer.valueOf(1), batch.getField(0).getValue(0));
        assertEquals(25, batch.getField(2).getValue(0));
        assertFalse(result.hasNext());
    }
}