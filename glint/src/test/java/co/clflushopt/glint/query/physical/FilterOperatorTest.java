package co.clflushopt.glint.query.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;

import co.clflushopt.glint.query.physical.expr.BooleanExpr;
import co.clflushopt.glint.query.physical.expr.ColumnExpr;
import co.clflushopt.glint.query.physical.expr.Expr;
import co.clflushopt.glint.query.physical.expr.LiteralIntExpr;
import co.clflushopt.glint.query.physical.plan.FilterOperator;
import co.clflushopt.glint.query.physical.plan.ScanOperator;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class FilterOperatorTest {
    @Test
    public void testFilter() {
        // Create test data
        Schema schema = new Schema(TestUtils.createTestSchema());
        RecordBatch testBatch = TestUtils.createTestBatch();
        SyntheticDataSource dataSource = new SyntheticDataSource(schema,
                Collections.singletonList(testBatch));
        ScanOperator scan = new ScanOperator(dataSource, Collections.emptyList());

        // Filter age > 30
        Expr filterExpr = new BooleanExpr.GtExpression(new ColumnExpr(2), new LiteralIntExpr(30));

        FilterOperator filter = new FilterOperator(scan, filterExpr);
        Iterator<RecordBatch> result = filter.execute();

        // Verify results
        assertTrue(result.hasNext());
        RecordBatch batch = result.next();
        assertEquals(1, batch.getRowSize());
        assertEquals("Charlie", batch.getField(1).getValue(0));
        assertEquals(35, Integer.parseInt((String) batch.getField(2).getValue(0)));
    }
}
