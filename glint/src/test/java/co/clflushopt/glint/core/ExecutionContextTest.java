package co.clflushopt.glint.core;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import co.clflushopt.glint.query.logical.expr.LogicalColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralDouble;
import co.clflushopt.glint.query.logical.expr.LogicalLiteralString;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class ExecutionContextTest {
    @Test
    public void canBuildExecutionContext() throws FileNotFoundException {
        var context = ExecutionContext.create().build();
        List<Field> expectedFields = List.of(new Field("id", ArrowTypes.Int64Type),
                new Field("first_name", ArrowTypes.StringType), // Changed from Int64Type
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.Int64Type));
        Optional<Schema> schema = Optional.of(new Schema(expectedFields));
        var df = context.readCsv("./testdata/employee.csv", schema,
                CsvReaderOptions.builder().hasHeader(true).tableName("employee").build());

        var result = df.filter(col("state").eq(lit("CO")))
                .project(List.of(col("id"), col("first_name"), col("last_name"), col("salary"),
                        col("salary").mult(lit(0.1)).alias("bonus")));

        assertEquals(
                "Projection: #id, #first_name, #last_name, #salary, #salary * 0.1 AS bonus\n"
                        + "\tFilter: #state = 'CO'\n" + "\t\tScan: employee [projection=None]\n",
                LogicalPlan.format(result.getLogicalPlan()));
    }

    /**
     * Helper method to create a literal expression.
     */
    private static LogicalExpr lit(String value) {
        return new LogicalLiteralString(value);
    }

    /**
     * Helper method to create a literal expression.
     */
    private static LogicalExpr lit(double value) {
        return new LogicalLiteralDouble(value);
    }

    /**
     * Helper method to create a column reference expression.
     */
    private static LogicalExpr col(String name) {
        return new LogicalColumnExpr(name);
    }
}
