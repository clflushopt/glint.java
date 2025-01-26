package co.clflushopt.glint.dataframe;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import co.clflushopt.glint.datasource.CsvDataSource;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Count;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Max;
import co.clflushopt.glint.query.logical.expr.AggregateExpr.Min;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.LiteralDouble;
import co.clflushopt.glint.query.logical.expr.LiteralString;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

/**
 * Test suite for DataFrame operations and query building. Demonstrates the
 * fluent API for constructing logical query plans.
 */
public class DataFrameTest {

    private DataFrame df;

    @Before
    public void setUp() throws FileNotFoundException {
        // Initialize with CSV data source before each test
        df = createCsvDataFrame();
    }

    /**
     * Tests basic DataFrame operations including filter and projection. Verifies
     * the correct construction of the logical plan.
     */
    @Test
    public void testBuildDataFrame() {
        DataFrame result = df.filter(col("state").eq(lit("CO")))
                .project(Arrays.asList(col("id"), col("first_name"), col("last_name")));

        String expected = "Projection: #id, #first_name, #last_name\n" + "\tFilter: #state = 'CO'\n"
                + "\t\tScan: employee [projection=None]\n";

        assertEquals(expected, LogicalPlan.format(result.getLogicalPlan()));
    }

    /**
     * Tests more complex operations including arithmetic expressions and aliases.
     * Demonstrates chaining multiple operations including nested filters.
     */
    @Test
    public void testMultiplierAndAlias() {
        DataFrame result = df.filter(col("state").eq(lit("CO")))
                .project(Arrays.asList(col("id"), col("first_name"), col("last_name"),
                        col("salary"), col("salary").mult(lit(0.1)).alias("bonus")))
                .filter(col("bonus").gt(lit(1000)));

        String expected = "Filter: #bonus > 1000.0\n"
                + "\tProjection: #id, #first_name, #last_name, #salary, #salary * 0.1 AS bonus\n"
                + "\t\tFilter: #state = 'CO'\n" + "\t\t\tScan: employee [projection=None]\n";

        assertEquals(expected, LogicalPlan.format(result.getLogicalPlan()));
    }

    /**
     * Tests aggregation operations with grouping and aggregate functions.
     * Demonstrates the use of built-in aggregate functions.
     */
    @Test
    public void testAggregateQuery() {
        DataFrame result = df.aggregate(Arrays.asList(col("state")), Arrays
                .asList(new Min(col("salary")), new Max(col("salary")), new Count(col("salary"))));

        String expected = "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n"
                + "\tScan: employee [projection=None]\n";

        assertEquals(expected, LogicalPlan.format(result.getLogicalPlan()));
    }

    /**
     * Helper method to create a DataFrame from CSV data source.
     */
    private DataFrame createCsvDataFrame() throws FileNotFoundException {
        String filename = "./testdata/employee.csv";
        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.Int64Type),
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.Int64Type));
        Schema expectedSchema = new Schema(expectedFields);
        Optional<Schema> schema = Optional.of(expectedSchema);
        CsvDataSource source = new CsvDataSource(filename, schema, true, 2); // batch size
        return new DataFrameImpl(new Scan("employee", source, Arrays.asList()));
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