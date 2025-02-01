package co.clflushopt.glint.query.optimizer;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.dataframe.DataFrameImpl;
import co.clflushopt.glint.datasource.CsvDataSource;
import co.clflushopt.glint.query.logical.expr.AggregateExpr;
import co.clflushopt.glint.query.logical.expr.BooleanExpr;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.LiteralString;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.logical.plan.Scan;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class QueryOptimizerTest {

    @Test
    public void testProjectionPushDown() throws FileNotFoundException {
        DataFrame df = csv().project(Arrays.asList(col("id"), col("first_name"), col("last_name")));

        PredicatePushdownRule rule = new PredicatePushdownRule();
        LogicalPlan optimizedPlan = rule.apply(df.getLogicalPlan());

        String expected = "Projection: #id, #first_name, #last_name\n"
                + "\tScan:employee [projection=(last_name, id, first_name)]\n";

        assertEquals(expected, LogicalPlan.format(optimizedPlan));
    }

    @Test
    public void testProjectionPushDownWithSelection() throws FileNotFoundException {
        DataFrame df = csv().filter(eq(col("state"), lit("CO")))
                .project(Arrays.asList(col("id"), col("first_name"), col("last_name")));

        PredicatePushdownRule rule = new PredicatePushdownRule();
        LogicalPlan optimizedPlan = rule.apply(df.getLogicalPlan());

        String expected = "Projection: #id, #first_name, #last_name\n" + "\tFilter: #state = 'CO'\n"
                + "\t\tScan:employee [projection=(last_name, id, state, first_name)]\n";

        assertEquals(expected, LogicalPlan.format(optimizedPlan));
    }

    @Test
    public void testProjectionPushDownWithAggregateQuery() throws FileNotFoundException {
        DataFrame df = csv().aggregate(Collections.singletonList(col("state")),
                List.of(min(col("salary")), max(col("salary")), count(col("salary"))));

        PredicatePushdownRule rule = new PredicatePushdownRule();
        LogicalPlan optimizedPlan = rule.apply(df.getLogicalPlan());

        String expected = "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n"
                + "\tScan:employee [projection=(state, salary)]\n";

        assertEquals(expected, LogicalPlan.format(optimizedPlan));
    }

    private DataFrame csv() throws FileNotFoundException {
        String employeeCsv = "../testdata/employee.csv";
        Schema schema = new Schema(Arrays.asList(new Field("id", ArrowTypes.Int64Type),
                new Field("first_name", ArrowTypes.StringType),
                new Field("last_name", ArrowTypes.StringType),
                new Field("state", ArrowTypes.StringType),
                new Field("job_title", ArrowTypes.StringType),
                new Field("salary", ArrowTypes.Int64Type)));
        return new DataFrameImpl(new Scan("employee",
                new CsvDataSource(employeeCsv, Optional.of(schema), true, 1024),
                Collections.emptyList()));
    }

    // Helper methods for creating expressions
    private LogicalExpr col(String name) {
        return new ColumnExpr(name);
    }

    private LogicalExpr lit(String value) {
        return new LiteralString(value);
    }

    private LogicalExpr eq(LogicalExpr left, LogicalExpr right) {
        return BooleanExpr.Eq(left, right);
    }

    private AggregateExpr min(LogicalExpr expr) {
        return new AggregateExpr.Min(expr);
    }

    private AggregateExpr max(LogicalExpr expr) {
        return new AggregateExpr.Max(expr);
    }

    private AggregateExpr count(LogicalExpr expr) {
        return new AggregateExpr.Count(expr);
    }
}
