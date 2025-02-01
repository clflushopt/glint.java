package co.clflushopt.glint.query.planner;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.dataframe.DataFrameImpl;
import co.clflushopt.glint.datasource.DataSource;
import co.clflushopt.glint.datasource.MemoryDataSource;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Max;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.LiteralDouble;
import co.clflushopt.glint.query.logical.expr.LiteralString;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class QueryPlannerTest {

    @Test
    public void testPlanAggregateQuery() {
        // Create schema
        List<Field> fields = Arrays.asList(new Field("passenger_count", ArrowTypes.Int32Type),
                new Field("max_fare", ArrowTypes.DoubleType));
        Schema schema = new Schema(fields);

        // Create data source
        DataSource dataSource = new MemoryDataSource(schema, Collections.emptyList());

        // Create DataFrame
        DataFrame df = new DataFrameImpl(new Scan("", dataSource, Collections.emptyList()));

        // Create logical plan with aggregation
        LogicalPlan plan = df.aggregate(Collections.singletonList(col("passenger_count")),
                Collections.singletonList(max(col("max_fare")))).getLogicalPlan();

        // Test initial logical plan
        assertEquals("Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n"
                + "\tScan:  [projection=None]\n", LogicalPlan.format(plan));

    }

    // Helper methods for creating expressions

    private static Max max(LogicalExpr expr) {
        return new Max(expr);
    }

    /**
     * Helper method to create a literal expression.
     */
    private static LogicalExpr lit(String value) {
        return new LiteralString(value);
    }

    /**
     * Helper method to create a literal expression.
     */
    private static LogicalExpr lit(double value) {
        return new LiteralDouble(value);
    }

    /**
     * Helper method to create a column reference expression.
     */
    private static LogicalExpr col(String name) {
        return new ColumnExpr(name);
    }
}